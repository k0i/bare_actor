use anyhow::Result;
use sqlx::types::chrono::Utc;
use tokio::sync::{mpsc, oneshot};

use super::{wallet_persistent_actor::WalletPersistentActorHandle, WalletBalance, WalletID};

#[derive(Debug)]
pub struct WalletBalanceActor {
    balance: WalletBalance,
    transaction_id: u64,
    wallet_id: WalletID,
    receiver: mpsc::Receiver<WalletBalanceMessage>,
    persistent_actor: WalletPersistentActorHandle,
}

#[derive(Clone, Debug)]
pub struct WalletBalanceActorHandle {
    sender: mpsc::Sender<WalletBalanceMessage>,
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
                self.balance += amount;
                self.transaction_id += 1;
                let id = self.transaction_id;
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
                if self.balance >= amount {
                    self.balance -= amount;
                    self.transaction_id += 1;
                    let id = self.transaction_id;
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
                } else {
                    // TODO: insert failed transaction
                    panic!("Insufficient balance");
                }
            }
        }
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

    pub async fn subtract_balance(
        &self,
        amount: WalletBalance,
        responder: oneshot::Sender<WalletBalance>,
    ) -> Result<()> {
        let msg = WalletBalanceMessage::SubtractBalance { amount, responder };

        let _ = self.sender.send(msg).await;
        Ok(())
    }
}

pub enum WalletBalanceMessage {
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

pub async fn run_wallet_balance_actor(mut actor: WalletBalanceActor) {
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
