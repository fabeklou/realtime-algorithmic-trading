INSERT INTO trading.klines_1s
SELECT
    splitByChar('-', _file)[1] AS symbol,
    fromUnixTimestamp64Micro(open_time) AS open_time,
    open, high, low, close, volume, number_of_trades
FROM file('klines/*-1s-*.csv', CSV,
    'open_time Int64, open Float64, high Float64, low Float64, close Float64,
     volume Float64, close_time Int64, quote_volume Float64,
     number_of_trades UInt32, taker_buy_base Float64,
     taker_buy_quote Float64, ignore String');
