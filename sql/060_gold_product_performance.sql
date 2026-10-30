-- Gold: paid units and revenue per product, plus how often it was added to a cart.
CREATE OR REPLACE TABLE analytics.gold_product_performance AS
WITH paid_items AS (
    SELECT
        order_items.product_id,
        SUM(order_items.quantity) AS units,
        ROUND(SUM(order_items.line_total), 2) AS gross_revenue
    FROM analytics.silver_order_items AS order_items
    INNER JOIN analytics.silver_orders AS orders USING (order_id)
    WHERE orders.status = 'paid'
    GROUP BY order_items.product_id
),
cart_adds AS (
    SELECT
        product_id,
        COUNT(*) AS add_to_cart_events
    FROM analytics.silver_events
    WHERE event_type = 'add_to_cart'
    GROUP BY product_id
)
SELECT
    product_id,
    COALESCE(add_to_cart_events, 0) AS add_to_cart_events,
    COALESCE(units, 0) AS units,
    COALESCE(gross_revenue, 0) AS gross_revenue
FROM paid_items
FULL OUTER JOIN cart_adds USING (product_id)
ORDER BY gross_revenue DESC, product_id;
