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
