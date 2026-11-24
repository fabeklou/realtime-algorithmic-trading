CREATE TABLE IF NOT EXISTS trading.features_hist
(
    symbol LowCardinality(String),
    t DateTime('UTC'),
    avg_price Float64,
    price_volatility Float64,
    range_pct Float64,
    trade_count UInt64,
    ret_1m Float64,
    target_5m Nullable(Float64),
    target_60m Nullable(Float64)
)
ENGINE = MergeTree
ORDER BY (symbol, t);