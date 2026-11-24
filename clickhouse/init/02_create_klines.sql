CREATE TABLE IF NOT EXISTS trading.klines_1s
(
    symbol LowCardinality(String),
    open_time DateTime64(6, 'UTC'),
    open Float64, high Float64, low Float64, close Float64,
    volume Float64,
    number_of_trades UInt32
)
ENGINE = MergeTree
ORDER BY (symbol, open_time);