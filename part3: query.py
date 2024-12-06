# Helper function to write the queries to file
def write_query_to_file(query, outfile):
    df = pd.read_sql(query, engine)
    df.to_csv(outfile, index=False)

### Q1
QUERY_1 = """
WITH hourly_counts AS (
    SELECT 
        CAST(strftime('%H', pickup_datetime) AS INTEGER) as X,
        COUNT(*) as Y
    FROM taxi_trips
    WHERE 
        pickup_datetime >= '2020-01-01' 
        AND pickup_datetime < '2024-09-01'
    GROUP BY strftime('%H', pickup_datetime)
)
SELECT 
    X,
    Y,
    ROUND(CAST(Y AS FLOAT) * 100 / (SELECT SUM(Y) FROM hourly_counts), 2) as percentage
FROM hourly_counts
ORDER BY X;
"""
conn = sqlite3.connect('project.db')
hourly_stats = pd.read_sql_query(QUERY_1, conn)
conn.close()
hourly_stats.to_csv("hourly_taxi_popularity.csv", index=False)


QUERY_2 = """
WITH daily_counts AS (
    SELECT 
        weekday_num as X,
        COUNT(*) as Y
    FROM uber_trips
    WHERE 
        pickup_datetime >= '2020-01-01' 
        AND pickup_datetime < '2024-09-01'
    GROUP BY weekday_num
)
SELECT 
    X,
    Y,
    ROUND(CAST(Y AS FLOAT) * 100 / (SELECT SUM(Y) FROM daily_counts), 2) as percentage
FROM daily_counts
ORDER BY Y DESC;
"""

conn = sqlite3.connect('project.db')
daily_stats = pd.read_sql_query(QUERY_2, conn)
conn.close()
daily_stats.to_csv("daily_uber_popularity.csv", index=False)

### Q3
QUERY_3 = """
WITH combined_trips AS (
    SELECT trip_distance as distance
    FROM taxi_trips
    WHERE 
        pickup_datetime >= '2024-01-01' 
        AND pickup_datetime < '2024-02-01'
    UNION ALL
    SELECT trip_miles as distance
    FROM uber_trips
    WHERE 
        pickup_datetime >= '2024-01-01' 
        AND pickup_datetime < '2024-02-01'
),
sorted_distances AS (
    SELECT 
        distance,
        (ROW_NUMBER() OVER (ORDER BY distance) - 1.0) / 
        (COUNT(*) OVER () - 1.0) * 100 as percentile
    FROM combined_trips
)
SELECT ROUND(distance, 2) as percentile_95
FROM sorted_distances
WHERE percentile >= 95
ORDER BY distance ASC
LIMIT 1;
"""

conn = sqlite3.connect('project.db')
daily_stats = pd.read_sql_query(QUERY_3, conn)
conn.close()
daily_stats.to_csv("ride_distance_percentile.csv", index=False)

### Q4
QUERY_4 = """
WITH SnowDays AS (
    SELECT 
        date AS snow_date,
        avg_precipitation AS total_precipitation
    FROM daily_weather
    WHERE daily_weather_type = 'snow'
),
DailyRideCounts AS (
    SELECT 
        DATE(pickup_datetime) AS ride_date,
        COUNT(*) AS total_rides
    FROM (
        SELECT pickup_datetime FROM taxi_trips
        UNION ALL
        SELECT pickup_datetime FROM uber_trips
    )
    GROUP BY DATE(pickup_datetime)
)
SELECT 
    s.snow_date AS date,
    s.total_precipitation,
    COALESCE(d.total_rides, 0) AS total_rides
FROM SnowDays s
LEFT JOIN DailyRideCounts d
ON s.snow_date = d.ride_date
ORDER BY s.total_precipitation DESC
LIMIT 10;
"""

conn = sqlite3.connect('project.db')
df = pd.read_sql_query(QUERY_4, conn)
df.to_csv(("buiest_trip.csv", index=False)
conn.close()




### Q5
QUERY_5_FILENAME = "snowiest_days_rides.csv"
QUERY_5 = """
WITH SnowDays AS (
    SELECT 
        date AS snow_date,
        avg_precipitation AS total_precipitation
    FROM daily_weather
    WHERE daily_weather_type = 'snow'
),
DailyRideCounts AS (
    SELECT 
        DATE(pickup_datetime) AS ride_date,
        COUNT(*) AS total_rides
    FROM (
        SELECT pickup_datetime FROM taxi_trips
        UNION ALL
        SELECT pickup_datetime FROM uber_trips
    )
    GROUP BY DATE(pickup_datetime)
)
SELECT 
    s.snow_date AS date,
    s.total_precipitation,
    COALESCE(d.total_rides, 0) AS total_rides
FROM SnowDays s
LEFT JOIN DailyRideCounts d
ON s.snow_date = d.ride_date
ORDER BY s.total_precipitation DESC
LIMIT 10;
"""

conn = sqlite3.connect('project.db')
df = pd.read_sql_query(QUERY_5, conn)
df.to_csv(QUERY_5_FILENAME, index=False)
conn.close()

### Q6
QUERY_6 = """
WITH DailyWeatherData AS (
    SELECT 
        date AS day,
        avg_precipitation AS precipitation,
        avg_windspeed AS windspeed
    FROM daily_weather
    WHERE date BETWEEN '2023-09-25' AND '2023-10-03'
),
DailyRideCounts AS (
    SELECT 
        DATE(pickup_datetime) AS day,
        COUNT(*) AS total_rides
    FROM (
        SELECT pickup_datetime FROM taxi_trips
        UNION ALL
        SELECT pickup_datetime FROM uber_trips
    )
    WHERE DATE(pickup_datetime) BETWEEN '2023-09-25' AND '2023-10-03'
    GROUP BY DATE(pickup_datetime)
),
CombinedData AS (
    SELECT 
        d.day AS date,
        COALESCE(r.total_rides, 0) AS total_rides,
        COALESCE(d.precipitation, 0.0) AS precipitation,
        COALESCE(d.windspeed, 0.0) AS windspeed
    FROM DailyWeatherData d
    LEFT JOIN DailyRideCounts r
    ON d.day = r.day
)
SELECT 
    date,
    total_rides,
    precipitation,
    windspeed
FROM CombinedData
ORDER BY date ASC;
"""

conn = sqlite3.connect('project.db')
df = pd.read_sql_query(QUERY_6, conn)
df.to_csv("Tropical Storm Ophelia.csv", index=False)
conn.close()
