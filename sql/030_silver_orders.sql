-- Silver: one row per order, reconstructing lifecycle state from the event stream.
-- Later events win only for the timestamp they represent; status is paid > cancelled > pending.
CREATE OR REPLACE TABLE analytics.silver_orders AS
WITH placed AS (
    SELECT
        order_id,
        MIN(event_ts) AS ordered_at,
        MIN(event_date) AS order_date,
        MIN(user_id) AS user_id,
        MIN(session_id) AS session_id,
        MIN(order_total) AS order_total
    FROM analytics.silver_events
    WHERE event_type = 'order_placed'
    GROUP BY order_id
),
items AS (
    SELECT
        order_id,
        SUM(quantity) AS units,
        ROUND(SUM(line_total), 2) AS items_total
    FROM analytics.silver_order_items
    GROUP BY order_id
),
payments AS (
    SELECT
        order_id,
        MIN(event_ts) AS paid_at,
        ROUND(SUM(payment_amount), 2) AS paid_amount
    FROM analytics.silver_events
    WHERE event_type = 'payment_processed'
    GROUP BY order_id
),
cancellations AS (
    SELECT
        order_id,
        MIN(event_ts) AS cancelled_at
    FROM analytics.silver_events
    WHERE event_type = 'order_cancelled'
    GROUP BY order_id
)
SELECT
    placed.order_id,
    placed.user_id,
    placed.session_id,
    placed.ordered_at,
    placed.order_date,
    placed.order_total,
    items.items_total,
    items.units,
    payments.paid_at,
    payments.paid_amount,
    cancellations.cancelled_at,
    CASE
        WHEN payments.paid_at IS NOT NULL THEN 'paid'
        WHEN cancellations.cancelled_at IS NOT NULL THEN 'cancelled'
        ELSE 'pending'
    END AS status
FROM placed
LEFT JOIN items USING (order_id)
LEFT JOIN payments USING (order_id)
LEFT JOIN cancellations USING (order_id);
