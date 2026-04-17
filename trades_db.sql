CREATE DATABASE trades_db;
USE trades_db;

CREATE TABLE trades (
    trade_id INTEGER PRIMARY KEY,
    symbol TEXT,
    side TEXT,
    quantity INTEGER,
    price REAL
);

use trades_db;
select * from trades