-- Silver: one row per event_id, with typed fields extracted from the JSON payload.
-- Duplicate deliveries are collapsed here, which makes replays safe.
CREATE OR REPLACE TABLE analytics.silver_events AS
WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY event_id
            ORDER BY ingested_at, batch_id
        ) AS duplicate_rank
    FROM bronze_events
),
deduped AS (
    SELECT
        *,
        CAST(payload AS JSON) AS event_payload
    FROM ranked
    WHERE duplicate_rank = 1
)
SELECT
    event_id,
    event_type,
    event_ts,
    CAST(event_ts AS DATE) AS event_date,
    user_id,
    session_id,
    event_payload AS payload,
    ingested_at,
    batch_id,
    json_extract_string(event_payload, '$.order_id') AS order_id,
    json_extract_string(event_payload, '$.product_id') AS product_id,
    TRY_CAST(json_extract(event_payload, '$.quantity') AS INTEGER) AS quantity,
    TRY_CAST(json_extract(event_payload, '$.unit_price') AS DOUBLE) AS unit_price,
    TRY_CAST(json_extract(event_payload, '$.total') AS DOUBLE) AS order_total,
    TRY_CAST(json_extract(event_payload, '$.amount') AS DOUBLE) AS payment_amount,
    json_extract_string(event_payload, '$.currency') AS currency,
    json_extract_string(event_payload, '$.page') AS page,
    json_extract_string(event_payload, '$.device') AS device,
    json_extract_string(event_payload, '$.method') AS payment_method,
    json_extract_string(event_payload, '$.reason') AS cancel_reason
FROM deduped;
