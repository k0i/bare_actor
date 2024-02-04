use actix_web::{post, web, App, HttpResponse, HttpServer, Responder};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use sqlx::{
    mysql::MySqlPoolOptions,
    types::chrono::{self, NaiveDateTime, Utc},
    Acquire, MySqlPool, Row,
};
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};

pub type WalletID = u64;
pub type WalletBalance = i64;
#[derive(Debug)]
struct WalletTableActor {
    receiver: mpsc::Receiver<TransactionMessage>,
    table: HashMap<WalletID, WalletBalanceActorHandle>,
    persistent_actor: WalletPersistentActorHandle,
}

#[derive(Debug)]
struct WalletBalanceActor {
    balance: WalletBalance,
    transaction_id: u64,
    wallet_id: WalletID,
    receiver: mpsc::Receiver<WalletBalanceMessage>,
    persistent_actor: WalletPersistentActorHandle,
}

#[derive(Debug)]
struct WalletPersistentActor {
    db_pool: MySqlPool,
    receiver: mpsc::Receiver<WalletPersistentMessage>,
}

enum TransactionMessage {
    Payment {
        id: WalletID,
        amount: WalletBalance,
        responder: oneshot::Sender<WalletBalance>,
    },
    Charge {
        id: WalletID,
        amount: WalletBalance,
        responder: oneshot::Sender<WalletBalance>,
    },
}

enum WalletBalanceMessage {
    GetBalance {
        responder: oneshot::Sender<WalletBalance>,
    },
    AddBalance {
        amount: WalletBalance,
        responder: oneshot::Sender<WalletBalance>,
    },
    SubtractBalance {
        amount: WalletBalance,
        responder: oneshot::Sender<WalletBalance>,
    },
}

enum WalletPersistentMessage {
    Persist {
        id: u64,
        wallet_id: WalletID,
        amount: WalletBalance,
        transaction_time: NaiveDateTime,
        last_wallet_amount: WalletBalance,
        responder: oneshot::Sender<WalletBalance>,
    },
    Retrieve {
        id: WalletID,
        responder: oneshot::Sender<(u64, WalletBalance)>,
    },
}

impl WalletTableActor {
    async fn new(receiver: mpsc::Receiver<TransactionMessage>) -> Self {
        WalletTableActor {
            receiver,
            table: HashMap::new(),
            persistent_actor: WalletPersistentActorHandle::new().await,
        }
    }
    async fn handle(&mut self, msg: TransactionMessage) {
        match msg {
            TransactionMessage::Payment {
                id,
                amount,
                responder,
            } => {
                if self.table.contains_key(&id) {
                    let actor = self.table.get(&id).unwrap();
                    let b = actor.subtract_balance(amount).await.unwrap();
                    let _ = responder.send(b);
                } else {
                    let actor =
                        WalletBalanceActorHandle::new(id, self.persistent_actor.clone()).await;
                    let b = actor.subtract_balance(amount).await.unwrap();
                    let _ = responder.send(b);
                    self.table.insert(id, actor);
                }
            }
            TransactionMessage::Charge {
                id,
                amount,
                responder,
            } => {
                if self.table.contains_key(&id) {
                    let actor = self.table.get(&id).unwrap().clone();
                    actor.add_balance(amount, responder).await.unwrap();
                } else {
                    let actor =
                        WalletBalanceActorHandle::new(id, self.persistent_actor.clone()).await;
                    actor.add_balance(amount, responder).await.unwrap();
                    self.table.insert(id, actor);
                }
            }
        }
    }
}

impl WalletBalanceActor {
    async fn new(
        receiver: mpsc::Receiver<WalletBalanceMessage>,
        wallet_id: WalletID,
        persistent_actor: WalletPersistentActorHandle,
    ) -> Self {
        let (transaction_id, last_wallet_amount) = persistent_actor
            .retrieve(wallet_id)
            .await
            .unwrap()
            .await
            .unwrap();
        WalletBalanceActor {
            balance: last_wallet_amount,
            transaction_id,
            receiver,
            wallet_id,
            persistent_actor,
        }
    }
    async fn handle(&mut self, msg: WalletBalanceMessage) {
        match msg {
            WalletBalanceMessage::GetBalance { responder } => {
                let _ = responder.send(self.balance);
            }
            WalletBalanceMessage::AddBalance { amount, responder } => {
                let id = self.transaction_id + 1;
                self.balance += amount;
                self.transaction_id += 1;
                let transaction_time = Utc::now().naive_utc();
                let wallet_id = self.wallet_id.clone();
                let balance = self.balance.clone();
                let persistent_actor = self.persistent_actor.clone();
                tokio::spawn(async move {
                    let recv = persistent_actor
                        .persist(id, wallet_id, amount, balance, transaction_time)
                        .await
                        .unwrap();
                    let _ = recv.await.unwrap();
                    let _ = responder.send(balance);
                });
            }
            WalletBalanceMessage::SubtractBalance { amount, responder } => {
                if self.balance > amount {
                    let transaction_time = Utc::now().naive_utc();
                    self.balance -= amount;
                    let _ = self
                        .persistent_actor
                        .persist(
                            self.transaction_id + 1,
                            self.wallet_id,
                            amount,
                            self.balance,
                            transaction_time,
                        )
                        .await;
                } else {
                    panic!("Insufficient balance");
                }
                let _ = responder.send(self.balance);
            }
        }
    }
}

impl WalletPersistentActor {
    fn new(db_pool: MySqlPool, receiver: mpsc::Receiver<WalletPersistentMessage>) -> Self {
        WalletPersistentActor { db_pool, receiver }
    }
    async fn handle(&mut self, msg: WalletPersistentMessage) {
        match msg {
            WalletPersistentMessage::Persist {
                id,
                wallet_id,
                amount,
                last_wallet_amount,
                transaction_time,
                responder,
            } => {
                let mut conn = self.db_pool.acquire().await.unwrap();
                tokio::spawn(async move {
                    sqlx::query("INSERT INTO transaction_events (id, wallet_id, amount, last_wallet_amount, transaction_time) VALUES (?, ?, ?, ?, ?)")
                        .bind(id)
                        .bind(wallet_id)
                        .bind(amount)
                        .bind(last_wallet_amount)
                        .bind(transaction_time)
                        .execute(&mut *conn)
                        .await.unwrap();
                    responder.send(last_wallet_amount).unwrap();
                });
            }
            WalletPersistentMessage::Retrieve { id, responder } => {
                let mut conn = self.db_pool.acquire().await.unwrap();
                tokio::spawn(async move {
                    let row = sqlx::query("SELECT id, last_wallet_amount FROM transaction_events WHERE wallet_id = ? ORDER BY id DESC LIMIT 1")
                        .bind(id)
                        .fetch_one(&mut *conn)
                        .await.unwrap();
                    let transaction_id: u64 = row.get(0);
                    let last_amount: i64 = row.get(1);
                    let _ = responder.send((transaction_id, last_amount)).unwrap();
                });
            }
        }
    }
}

async fn run_wallet_table_actor(mut actor: WalletTableActor) {
    let mut buffer = Vec::new();
    loop {
        let cnt = actor.receiver.recv_many(&mut buffer, 10).await;
        if cnt == 0 {
            break;
        }
        for msg in buffer.drain(..cnt) {
            actor.handle(msg).await;
        }
    }
}

async fn run_wallet_balance_actor(mut actor: WalletBalanceActor) {
    let mut buffer = Vec::new();
    loop {
        let cnt = actor.receiver.recv_many(&mut buffer, 100).await;
        if cnt == 0 {
            break;
        }
        for msg in buffer.drain(..cnt) {
            actor.handle(msg).await;
        }
    }
}

async fn run_wallet_persistent_actor(mut actor: WalletPersistentActor) {
    let mut buffer = Vec::new();
    loop {
        let cnt = actor.receiver.recv_many(&mut buffer, 100).await;
        if cnt == 0 {
            break;
        }
        for msg in buffer.drain(..cnt) {
            actor.handle(msg).await;
        }
    }
}

#[derive(Clone, Debug)]
pub struct WalletBalanceActorHandle {
    sender: mpsc::Sender<WalletBalanceMessage>,
}

#[derive(Clone, Debug)]
pub struct WalletTableActorHandle {
    sender: mpsc::Sender<TransactionMessage>,
}

#[derive(Clone, Debug)]
pub struct WalletPersistentActorHandle {
    sender: mpsc::Sender<WalletPersistentMessage>,
}

impl WalletTableActorHandle {
    pub async fn new() -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let actor = WalletTableActor::new(receiver).await;
        tokio::spawn(run_wallet_table_actor(actor));

        Self { sender }
    }

    pub async fn payment(&self, id: WalletID, amount: WalletBalance) -> Result<WalletBalance> {
        let (responder, recv) = oneshot::channel();
        let msg = TransactionMessage::Payment {
            id,
            amount,
            responder,
        };

        let _ = self.sender.send(msg).await;
        Ok(recv.await.expect("Actor task has been killed"))
    }
    pub async fn charge(&self, id: WalletID, amount: WalletBalance) -> Result<WalletBalance> {
        let sender = self.sender.clone();
        let (responder, recv) = oneshot::channel();
        tokio::spawn(async move {
            let msg = TransactionMessage::Charge {
                id,
                amount,
                responder,
            };

            let _ = sender.send(msg).await;
        });
        Ok(recv.await.expect("Actor task has been killed"))
    }
    pub async fn get_balance(&self, id: WalletID) -> Result<WalletBalance> {
        let (responder, recv) = oneshot::channel();
        let msg = TransactionMessage::Charge {
            id,
            amount: 0,
            responder,
        };

        let _ = self.sender.send(msg).await;
        Ok(recv.await.expect("Actor task has been killed"))
    }
}

impl WalletBalanceActorHandle {
    pub async fn new(wallet_id: WalletID, persisitant_actor: WalletPersistentActorHandle) -> Self {
        let (sender, receiver) = mpsc::channel(100);
        let actor = WalletBalanceActor::new(receiver, wallet_id, persisitant_actor).await;
        tokio::spawn(run_wallet_balance_actor(actor));

        Self { sender }
    }

    pub async fn get_balance(&self) -> Result<WalletBalance> {
        let (responder, recv) = oneshot::channel();
        let msg = WalletBalanceMessage::GetBalance { responder };

        let _ = self.sender.send(msg).await;
        Ok(recv.await.expect("Actor task has been killed"))
    }

    pub async fn add_balance(
        &self,
        amount: WalletBalance,
        responder: oneshot::Sender<WalletBalance>,
    ) -> Result<()> {
        let msg = WalletBalanceMessage::AddBalance { amount, responder };
        let _ = self.sender.send(msg).await;
        Ok(())
    }

    pub async fn subtract_balance(&self, amount: WalletBalance) -> Result<WalletBalance> {
        let (responder, recv) = oneshot::channel();
        let msg = WalletBalanceMessage::SubtractBalance { amount, responder };

        let _ = self.sender.send(msg).await;
        Ok(recv.await.expect("Actor task has been killed"))
    }
}

impl WalletPersistentActorHandle {
    pub async fn new() -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let db_pool = MySqlPoolOptions::new()
            .max_connections(30)
            .connect("mysql://root@localhost/actor_test")
            .await
            .expect("Failed to connect to MySQL");
        let actor = WalletPersistentActor::new(db_pool, receiver);
        tokio::spawn(run_wallet_persistent_actor(actor));

        Self { sender }
    }

    pub async fn persist(
        &self,
        id: u64,
        wallet_id: WalletID,
        amount: WalletBalance,
        last_wallet_amount: WalletBalance,
        transaction_time: NaiveDateTime,
    ) -> Result<oneshot::Receiver<WalletBalance>> {
        let (responder, recv) = oneshot::channel();
        let sender = self.sender.clone();
        tokio::spawn(async move {
            let _ = sender
                .send(WalletPersistentMessage::Persist {
                    id,
                    wallet_id,
                    amount,
                    last_wallet_amount,
                    transaction_time,
                    responder,
                })
                .await;
        });
        Ok(recv)
    }

    pub async fn retrieve(&self, id: WalletID) -> Result<oneshot::Receiver<(u64, WalletBalance)>> {
        let (responder, recv) = oneshot::channel();
        let _ = self
            .sender
            .send(WalletPersistentMessage::Retrieve { id, responder })
            .await;
        Ok(recv)
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct Transaction {
    wallet_id: WalletID,
    amount: WalletBalance,
}
#[post("/pay_with_actor")]
async fn pay_with_actor(p: web::Json<Transaction>, data: web::Data<AppState>) -> impl Responder {
    let table = &data.table;
    table.payment(p.wallet_id, p.amount).await.unwrap();
    HttpResponse::Ok().finish()
}

#[post("/charge_with_actor")]
async fn charge_with_actor(p: web::Json<Transaction>, data: web::Data<AppState>) -> impl Responder {
    let table = &data.table;
    table.charge(p.wallet_id, p.amount).await.unwrap();
    HttpResponse::Ok().finish()
}

#[post("/pay")]
async fn pay(p: web::Json<Transaction>) -> impl Responder {
    HttpResponse::Ok().finish()
}

#[post("/charge")]
async fn charge(p: web::Json<Transaction>, data: web::Data<AppState>) -> impl Responder {
    let db_pool = &data.db_pool;
    let mut conn = db_pool.acquire().await.unwrap();
    let mut tx = conn.begin().await.unwrap();
    let _wallet_lock = sqlx::query("SELECT * FROM wallets WHERE id = ? FOR UPDATE")
        .bind(p.wallet_id)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    let row = sqlx::query("SELECT last_wallet_amount FROM transaction_events WHERE wallet_id = ? ORDER BY id DESC LIMIT 1 FOR UPDATE")
        .bind(p.wallet_id)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    let balance: i64 = row.get(0);
    let _ = sqlx::query(
        "INSERT INTO transaction_events (wallet_id, amount, last_wallet_amount) VALUES (?, ?, ?)",
    )
    .bind(p.wallet_id)
    .bind(p.amount)
    .bind(balance + p.amount)
    .execute(&mut *tx)
    .await;
    tx.commit().await.unwrap();
    HttpResponse::Ok().finish()
}

struct AppState {
    table: WalletTableActorHandle,
    db_pool: MySqlPool,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
pub async fn main() -> std::io::Result<()> {
    let db_pool = MySqlPoolOptions::new()
        .max_connections(5)
        .connect("mysql://root@localhost/actor_test")
        .await
        .expect("Failed to connect to MySQL");

    let table = web::Data::new(AppState {
        table: WalletTableActorHandle::new().await,
        db_pool,
    });

    HttpServer::new(move || {
        App::new()
            .app_data(table.clone())
            .service(pay)
            .service(pay_with_actor)
            .service(charge)
            .service(charge_with_actor)
    })
    .bind(("127.0.0.1", 3000))?
    .run()
    .await
    //  for _ in 0..4 {
    //      let tbl = table.clone();
    //      let t1 = thread::spawn(move || {
    //          let rt = tokio::runtime::Builder::new_current_thread()
    //              .enable_all()
    //              .build()
    //              .unwrap();
    //          rt.block_on(async {
    //              for i in 1..=100000 {
    //                  let _ = tbl.charge(i, i as i64).await;
    //              }
    //          });
    //      });
    //      handles.push(t1);
    //  }

    //  for _ in 0..4 {
    //      let tbl = table.clone();
    //      let t2 = thread::spawn(move || {
    //          let rt = tokio::runtime::Builder::new_multi_thread()
    //              .enable_all()
    //              .build()
    //              .unwrap();
    //          rt.block_on(async {
    //              for i in 1..=100000 {
    //                  let _ = tbl.payment(i, i as i64).await;
    //              }
    //          });
    //      });
    //      handles.push(t2);
    //  }

    //  println!("Waiting for threads to finish");
    //  handles.into_iter().for_each(|h| h.join().unwrap());

    //  for i in 1..=100000 {
    //      let b = table.get_balance(i).await.unwrap();
    //      if b != 0 {
    //          panic!("{}: {}", i, b);
    //      }
    //  }
}
