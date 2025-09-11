{{ config(materialized='incremental', unique_key='bucket_symbol') }}

WITH b AS (
  SELECT
    DATE_TRUNC('minute', event_ts) AS bucket,
    symbol,
    MIN(event_ts) AS min_ts,
    MAX(event_ts) AS max_ts,
    MAX(price)    AS high,
    MIN(price)    AS low,
    SUM(size)     AS volume
  FROM {{ ref('stg_ticks') }}
  {% if is_incremental() %}
    WHERE event_ts > (SELECT COALESCE(MAX(bucket), '1970-01-01') FROM {{ this }})
  {% endif %}
  GROUP BY 1,2
),
open_price AS (
  SELECT b.bucket, b.symbol, s.price AS open
  FROM b
  JOIN {{ ref('stg_ticks') }} s
    ON s.symbol = b.symbol AND s.event_ts = b.min_ts
),
close_price AS (
  SELECT b.bucket, b.symbol, s.price AS close
  FROM b
  JOIN {{ ref('stg_ticks') }} s
    ON s.symbol = b.symbol AND s.event_ts = b.max_ts
)
SELECT
  b.bucket,
  b.symbol,
  o.open,
  b.high,
  b.low,
  c.close,
  b.volume,
  CONCAT(TO_VARCHAR(b.bucket),'|',b.symbol) AS bucket_symbol
FROM b
JOIN open_price  o USING (bucket, symbol)
JOIN close_price c USING (bucket, symbol)