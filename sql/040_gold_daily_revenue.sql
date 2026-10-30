-- Gold: revenue, order counts and AOV per calendar day (UTC event dates).
CREATE OR REPLACE TABLE analytics.gold_daily_revenue AS
SELECT
    order_date,
    COUNT(*) AS orders,
    COUNT(*) FILTER (WHERE status = 'paid') AS paid_orders,
    COUNT(*) FILTER (WHERE status = 'cancelled') AS cancelled_orders,
    COUNT(*) FILTER (WHERE status = 'pending') AS pending_orders,
    ROUND(SUM(order_total), 2) AS gross_booked,
    ROUND(COALESCE(SUM(paid_amount), 0), 2) AS net_revenue,
    ROUND(
        COALESCE(SUM(paid_amount), 0)
        / NULLIF(COUNT(*) FILTER (WHERE status = 'paid'), 0),
        2
    ) AS avg_order_value,
    ROUND(
        COUNT(*) FILTER (WHERE status = 'paid') * 100.0 / COUNT(*),
        2
    ) AS payment_rate_pct
FROM analytics.silver_orders
GROUP BY order_date
ORDER BY order_date;
