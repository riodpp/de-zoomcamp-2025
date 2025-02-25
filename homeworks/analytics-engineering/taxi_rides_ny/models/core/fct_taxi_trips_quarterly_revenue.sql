WITH quarterly_revenue AS (
    SELECT
        DATE_TRUNC(pickup_datetime, QUARTER) AS quarter_start,
        EXTRACT(YEAR FROM pickup_datetime) AS year,
        EXTRACT(QUARTER FROM pickup_datetime) AS quarter,
        service_type,
        SUM(total_amount) AS total_revenue
    FROM {{ ref('fact_trips') }}
    GROUP BY 1, 2, 3, 4
),
yoy_growth AS (
    SELECT
        q1.quarter_start,
        q1.year,
        q1.quarter,
        q1.service_type,
        q1.total_revenue,
        q2.total_revenue AS previous_year_revenue,
        ROUND(
            (q1.total_revenue - q2.total_revenue) / NULLIF(q2.total_revenue, 0) * 100, 
            2
        ) AS yoy_growth_percentage
    FROM quarterly_revenue q1
    LEFT JOIN quarterly_revenue q2
        ON q1.service_type = q2.service_type
        AND q1.quarter = q2.quarter
        AND q1.year = q2.year + 1
)
SELECT *
FROM yoy_growth
ORDER BY quarter_start DESC
