INSERT INTO trading.features_hist
SELECT
    symbol,
    toDateTime(open_time, 'UTC') AS t,
    avg_price,
    price_volatility,
    range_pct,
    trade_count,
    log(avg_price / prev_avg) AS ret_1m,
    -- La cible n'existe que si la ligne à +5 min / +60 min est exactement à cet instant.
    if(next5_time = open_time + INTERVAL 300 SECOND, log(close_5m / close), NULL) AS target_5m,
    if(next60_time = open_time + INTERVAL 3600 SECOND, log(close_60m / close), NULL) AS target_60m
FROM
(
    SELECT
        symbol, open_time, close, avg_price, price_volatility, range_pct, trade_count, rn,
        lagInFrame(avg_price, 60) OVER w2 AS prev_avg,
        lagInFrame(open_time, 60) OVER w2 AS prev_time,
        leadInFrame(close, 300) OVER w2 AS close_5m,
        leadInFrame(open_time, 300) OVER w2 AS next5_time,
        leadInFrame(close, 3600) OVER w2 AS close_60m,
        leadInFrame(open_time, 3600) OVER w2 AS next60_time
    FROM
    (
        SELECT
            symbol, open_time, close,
            avg(close) OVER w1 AS avg_price,
            stddevPop(close) OVER w1 AS price_volatility,
            (max(high) OVER w1 - min(low) OVER w1) / avg(close) OVER w1 AS range_pct,
            sum(number_of_trades) OVER w1 AS trade_count,
            row_number() OVER (PARTITION BY symbol ORDER BY open_time) AS rn
        FROM trading.klines_1s
        WHERE symbol = {sym:String}
        WINDOW w1 AS (PARTITION BY symbol ORDER BY open_time
                      ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)
    )
    WINDOW w2 AS (PARTITION BY symbol ORDER BY open_time
                  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
)
WHERE toUnixTimestamp(open_time) % 10 = 0
  AND rn >= 120
  AND prev_time = open_time - INTERVAL 60 SECOND;