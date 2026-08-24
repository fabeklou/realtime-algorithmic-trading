-- Table source : lit le topic Kafka market-prices
CREATE TABLE market_prices (
    symbol STRING,
    price DOUBLE,
    event_timestamp TIMESTAMP_LTZ(3),
    WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'market-prices',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'flink-features-consumer-v3',
    'scan.startup.mode' = 'latest-offset',
    'scan.watermark.idle-timeout' = '10000',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601'
);

-- Table sink : publie les features calculées vers un nouveau topic
CREATE TABLE market_features (
    symbol STRING,
    window_start TIMESTAMP_LTZ(3),
    window_end TIMESTAMP_LTZ(3),
    avg_price DOUBLE,
    price_volatility DOUBLE,
    tick_count BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'market-features',
    'properties.bootstrap.servers' = 'kafka:29092',
    'format' = 'json'
);

-- La transformation : fenêtre glissante de 1 minute, recalculée toutes les 10 secondes
INSERT INTO market_features
SELECT
    symbol,
    window_start,
    window_end,
    AVG(price) AS avg_price,
    STDDEV_POP(price) AS price_volatility,
    COUNT(*) AS tick_count
FROM TABLE(
    HOP(TABLE market_prices, DESCRIPTOR(event_timestamp), INTERVAL '10' SECOND, INTERVAL '1' MINUTE)
)
GROUP BY symbol, window_start, window_end;