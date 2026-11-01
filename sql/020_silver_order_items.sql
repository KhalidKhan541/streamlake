-- Silver: explode order_placed.items into one row per order line.
-- Index the JSON array with range() so the model works on every DuckDB 1.x
-- release without relying on nested-type casts.
CREATE OR REPLACE TABLE analytics.silver_order_items AS
WITH placed AS (
    SELECT
        order_id,
        user_id,
        session_id,
        event_ts,
        event_date,
        json_extract(payload, '$.items') AS items
    FROM analytics.silver_events
    WHERE event_type = 'order_placed'
),
exploded AS (
    SELECT
        order_id,
        user_id,
        session_id,
        event_ts,
        event_date,
        json_extract(
            items,
            '$[' || CAST(item_index.i AS VARCHAR) || ']'
        ) AS item
    FROM placed
    CROSS JOIN range(json_array_length(items)) AS item_index(i)
)
SELECT
    order_id,
    user_id,
    session_id,
    event_ts AS ordered_at,
    event_date AS order_date,
    json_extract_string(item, '$.product_id') AS product_id,
    TRY_CAST(json_extract(item, '$.quantity') AS INTEGER) AS quantity,
    TRY_CAST(json_extract(item, '$.unit_price') AS DOUBLE) AS unit_price,
    ROUND(
        TRY_CAST(json_extract(item, '$.quantity') AS INTEGER)
        * TRY_CAST(json_extract(item, '$.unit_price') AS DOUBLE),
        2
    ) AS line_total
FROM exploded;
