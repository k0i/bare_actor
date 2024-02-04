use actix_web::{post, web, App, HttpResponse, HttpServer, Responder};
use actors::{wallet_table_actor::WalletTableActorHandle, WalletBalance, WalletID};
use serde::{Deserialize, Serialize};
use sqlx::{mysql::MySqlPoolOptions, Acquire, MySqlPool, Row};
mod actors;

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
async fn pay(p: web::Json<Transaction>, data: web::Data<AppState>) -> impl Responder {
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
    if balance < p.amount {
        return HttpResponse::BadRequest().finish();
    }
    let _ = sqlx::query(
        "INSERT INTO transaction_events (wallet_id, amount, last_wallet_amount) VALUES (?, ?, ?)",
    )
    .bind(p.wallet_id)
    .bind(p.amount)
    .bind(balance - p.amount)
    .execute(&mut *tx)
    .await;
    tx.commit().await.unwrap();
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
}
