-- Table 1 : lecture brute depuis Kafka (pas de stockage réel)
CREATE TABLE trading.market_features_queue
(
    symbol String,
    window_start DateTime64(3),
    window_end DateTime64(3),
    avg_price Float64,
    price_volatility Float64,
    tick_count UInt64
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'market-features',
    kafka_group_name = 'clickhouse-features-consumer-v2',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_skip_broken_messages = 5,
    date_time_input_format = 'best_effort';

-- Table 2 : stockage réel, persistant
CREATE TABLE trading.market_features
(
    symbol String,
    window_start DateTime64(3),
    window_end DateTime64(3),
    inserted_at DateTime64(3) DEFAULT now64(3),
    avg_price Float64,
    price_volatility Float64,
    tick_count UInt64
)
ENGINE = MergeTree
ORDER BY (symbol, window_start);

-- Le pont automatique : à chaque ligne consommée depuis Kafka,
-- la Materialized View la recopie immédiatement dans la table MergeTree.
CREATE MATERIALIZED VIEW trading.market_features_mv
TO trading.market_features
AS SELECT * FROM trading.market_features_queue;
