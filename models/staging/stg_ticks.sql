{{ config(
    materialized='incremental',
    unique_key='event_ts_symbol_exchange'
) }}

WITH src AS (
  SELECT
    event_ts,
    LOWER(exchange)        AS exchange,
    UPPER(symbol)          AS symbol,
    IFF(side IN ('buy','sell'), side, NULL) AS side,
    TRY_TO_DOUBLE(price)    AS price,
    TRY_TO_DOUBLE(size)     AS size,
    TRY_TO_DOUBLE(best_bid) AS best_bid,
    TRY_TO_DOUBLE(best_ask) AS best_ask
  FROM {{ source('raw','crypto_ticks') }}
  WHERE 1=1
  {% if is_incremental() %}
    AND event_ts > (
      SELECT COALESCE(MAX(event_ts), TO_TIMESTAMP('1970-01-01'))
      FROM {{ this }}
    )
  {% endif %}
)

SELECT
  event_ts,
  exchange,
  symbol,
  side,
  price,
  size,
  best_bid,
  best_ask,
  CONCAT(TO_VARCHAR(event_ts), '|', symbol, '|', exchange) AS event_ts_symbol_exchange
FROM src
WHERE price > 0 AND size >= 0