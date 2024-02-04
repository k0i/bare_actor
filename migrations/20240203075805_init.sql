-- Add migration script here
-- mysql

CREATE TABLE IF NOT EXISTS users (
  id bigint unsigned NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (id)
);

CREATE TABLE wallets (
  id bigint unsigned NOT NULL AUTO_INCREMENT,
  user_id bigint unsigned NOT NULL,
  amount bigint,
  PRIMARY KEY (id),
  CONSTRAINT fk_user_id FOREIGN KEY (user_id) REFERENCES users(id)
);

CREATE TABLE transaction_events (
  id bigint unsigned NOT NULL AUTO_INCREMENT,
  wallet_id bigint unsigned NOT NULL,
  amount bigint,
  last_wallet_amount bigint,
  transaction_time timestamp,
  PRIMARY KEY (id),
  CONSTRAINT fk_wallet_id FOREIGN KEY (wallet_id) REFERENCES wallets(id)
);
