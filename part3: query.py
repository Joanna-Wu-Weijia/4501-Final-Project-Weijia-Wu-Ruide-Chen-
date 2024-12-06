# Helper function to write the queries to file
def write_query_to_file(query, outfile):
    df = pd.read_sql(query, engine)
    df.to_csv(outfile, index=False)

### Q1
QUERY_1_FILENAME = "hourly_taxi_popularity.csv"
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

# execute query either via sqlalchemy
#with engine.connect() as con:
    #results = con.execute(db.text(QUERY_1)).fetchall()
#results

# or via pandas
pd.read_sql(QUERY_1, con=engine)

write_query_to_file(QUERY_1, QUERY_1_FILENAME)

### Q2
QUERY_2_FILENAME = "daily_uber_popularity.csv"
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
pd.read_sql(QUERY_2, con=engine)
write_query_to_file(QUERY_2, QUERY_2_FILENAME)

### Q3
QUERY_3_FILENAME = "ride_distance_percentile.csv"
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

# Execute the query
percentile_95_result = pd.read_sql_query(QUERY_3, con=engine)
write_query_to_file(QUERY_3, QUERY_3_FILENAME)

### Q4
import sqlite3

# Establish connection to the SQLite database
conn = sqlite3.connect('project.db')

# SQL Query to find the top 10 snowiest days based on precipitation
query = """
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

# Execute the query and fetch the results into tuples
cursor = conn.cursor()
cursor.execute(query)
result_tuples = cursor.fetchall()

# Close the connection
conn.close()

# Display the tuples
for row in result_tuples:
    print(row)

### Q5
# Establish connection to the SQLite database
conn = sqlite3.connect('project.db')

# SQL Query to find the top 10 snowiest days based on precipitation
query = """
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

# Execute the query and fetch the results into tuples
cursor = conn.cursor()
cursor.execute(query)
result_tuples = cursor.fetchall()

# Close the connection
conn.close()

# Display the tuples
for row in result_tuples:
    print(row)

### Q6
import sqlite3

# Establish connection to the SQLite database
conn = sqlite3.connect('project.db')

# SQL Query for the 9-day period centered around Tropical Storm Ophelia
query = """
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

# Execute the query and fetch the results as tuples
cursor = conn.cursor()
cursor.execute(query)
result_tuples = cursor.fetchall()

# Close the database connection
conn.close()

# Output the list of tuples
for row in result_tuples:
    print(row)
