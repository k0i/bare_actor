use anyhow::Result;
use sqlx::{mysql::MySqlPoolOptions, types::chrono::NaiveDateTime, MySqlPool, Row};
use tokio::sync::{mpsc, oneshot};

use super::{WalletBalance, WalletID};

#[derive(Debug)]
pub struct WalletPersistentActor {
    db_pool: MySqlPool,
    receiver: mpsc::Receiver<WalletPersistentMessage>,
}

#[derive(Clone, Debug)]
pub struct WalletPersistentActorHandle {
    sender: mpsc::Sender<WalletPersistentMessage>,
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

pub async fn run_wallet_persistent_actor(mut actor: WalletPersistentActor) {
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

pub enum WalletPersistentMessage {
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
