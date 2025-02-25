WITH filtered_trips AS (
    SELECT
        service_type,
        DATE_TRUNC(pickup_datetime, MONTH) AS month_start,
        EXTRACT(YEAR FROM pickup_datetime) AS year,
        EXTRACT(MONTH FROM pickup_datetime) AS month,
        fare_amount
    FROM {{ ref('fact_trips') }}
    WHERE 
        fare_amount > 0 
        AND trip_distance > 0 
        AND payment_type_description IN ('Cash', 'Credit Card')
),
percentiles AS (
    SELECT
        service_type,
        month_start,
        year,
        month,
        APPROX_QUANTILES(fare_amount, 100)[OFFSET(97)] AS p97_fare,
        APPROX_QUANTILES(fare_amount, 100)[OFFSET(95)] AS p95_fare,
        APPROX_QUANTILES(fare_amount, 100)[OFFSET(90)] AS p90_fare
    FROM filtered_trips
    GROUP BY 1, 2, 3, 4
)
SELECT *
FROM percentiles
ORDER BY month_start DESC
