-- Gold: paid units and revenue per product, plus how often it was added to a cart.
CREATE OR REPLACE TABLE analytics.gold_product_performance AS
WITH paid_items AS (
    SELECT
        order_items.product_id,
        SUM(order_items.quantity) AS units,
        ROUND(SUM(order_items.line_total), 2) AS gross_revenue
    FROM analytics.silver_order_items AS order_items
    INNER JOIN analytics.silver_orders AS orders
        ON orders.order_id = order_items.order_id
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
),
products AS (
    SELECT product_id FROM paid_items
    UNION
    SELECT product_id FROM cart_adds
)
SELECT
    products.product_id,
    COALESCE(cart_adds.add_to_cart_events, 0) AS add_to_cart_events,
    COALESCE(paid_items.units, 0) AS units,
    COALESCE(paid_items.gross_revenue, 0) AS gross_revenue
FROM products
LEFT JOIN paid_items ON paid_items.product_id = products.product_id
LEFT JOIN cart_adds ON cart_adds.product_id = products.product_id;
