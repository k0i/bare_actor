use std::collections::HashMap;

use tokio::sync::{mpsc, oneshot};

use super::{
    wallet_balance_actor::WalletBalanceActorHandle,
    wallet_persistent_actor::WalletPersistentActorHandle, WalletBalance, WalletID,
};
use anyhow::Result;

#[derive(Debug)]
pub struct WalletTableActor {
    receiver: mpsc::Receiver<TransactionMessage>,
    table: HashMap<WalletID, WalletBalanceActorHandle>,
    persistent_actor: WalletPersistentActorHandle,
}

#[derive(Clone, Debug)]
pub struct WalletTableActorHandle {
    sender: mpsc::Sender<TransactionMessage>,
}

pub enum TransactionMessage {
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
                    actor.subtract_balance(amount, responder).await.unwrap();
                } else {
                    let actor =
                        WalletBalanceActorHandle::new(id, self.persistent_actor.clone()).await;
                    actor.subtract_balance(amount, responder).await.unwrap();
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
