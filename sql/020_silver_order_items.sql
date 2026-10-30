-- Silver: explode order_placed.items into one row per order line.
-- from_json() turns the JSON array into a typed LIST of STRUCTs to unnest.
CREATE OR REPLACE TABLE analytics.silver_order_items AS
WITH placed AS (
    SELECT
        order_id,
        user_id,
        session_id,
        event_ts,
        event_date,
        from_json(
            json_extract(payload, '$.items'),
            '[{"product_id": "VARCHAR", "quantity": "INTEGER", "unit_price": "DOUBLE"}]'
        ) AS items
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
        UNNEST(items) AS item
    FROM placed
)
SELECT
    order_id,
    user_id,
    session_id,
    event_ts AS ordered_at,
    event_date AS order_date,
    item.product_id AS product_id,
    item.quantity AS quantity,
    item.unit_price AS unit_price,
    ROUND(item.quantity * item.unit_price, 2) AS line_total
FROM exploded;
