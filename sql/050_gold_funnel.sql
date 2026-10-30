-- Gold: daily session funnel. A session counts for a step if it produced at
-- least one event of that type, so repeated page views do not inflate counts.
CREATE OR REPLACE TABLE analytics.gold_funnel AS
WITH per_session AS (
    SELECT
        session_id,
        MIN(event_date) AS session_date,
        MAX(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END) AS viewed,
        MAX(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS carted,
        MAX(CASE WHEN event_type = 'order_placed' THEN 1 ELSE 0 END) AS ordered,
        MAX(CASE WHEN event_type = 'payment_processed' THEN 1 ELSE 0 END) AS paid
    FROM analytics.silver_events
    GROUP BY session_id
),
daily AS (
    SELECT
        session_date,
        SUM(viewed) AS sessions,
        SUM(carted) AS sessions_with_cart,
        SUM(ordered) AS sessions_with_order,
        SUM(paid) AS sessions_with_payment
    FROM per_session
    GROUP BY session_date
),
steps AS (
    SELECT session_date, 'page_view' AS step, 1 AS step_order, sessions AS sessions
    FROM daily
    UNION ALL
    SELECT session_date, 'add_to_cart', 2, sessions_with_cart
    FROM daily
    UNION ALL
    SELECT session_date, 'order_placed', 3, sessions_with_order
    FROM daily
    UNION ALL
    SELECT session_date, 'payment_processed', 4, sessions_with_payment
    FROM daily
)
SELECT
    steps.session_date,
    steps.step,
    steps.step_order,
    steps.sessions,
    ROUND(steps.sessions * 100.0 / NULLIF(daily.sessions, 0), 2) AS conversion_pct
FROM steps
INNER JOIN daily USING (session_date)
ORDER BY steps.session_date, steps.step_order;
