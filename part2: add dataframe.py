taxi_data['pickup_coords'] = taxi_data['pickup_coords'].apply(lambda x: f"{x[0]},{x[1]}")
taxi_data['dropoff_coords'] = taxi_data['dropoff_coords'].apply(lambda x: f"{x[0]},{x[1]}")

uber_data['pickup_coords'] = uber_data['pickup_coords'].apply(lambda x: f"{x[0]},{x[1]}")
uber_data['dropoff_coords'] = uber_data['dropoff_coords'].apply(lambda x: f"{x[0]},{x[1]}")
# def str_to_coords(coord_str):
    #lat, lon = map(float, coord_str.split(','))
    #return (lat, lon)

engine = db.create_engine(DATABASE_URL)

HOURLY_WEATHER_SCHEMA = """
CREATE TABLE IF NOT EXISTS hourly_weather (
   date TIMESTAMP NOT NULL,
   hourly_weather_type VARCHAR(50),
   hourly_temperature FLOAT,
   hourly_precipitation FLOAT,
   hourly_windspeed FLOAT,
   hour INTEGER NOT NULL,
   weekday_num INTEGER NOT NULL,
   severe_weather FLOAT
);
"""

DAILY_WEATHER_SCHEMA = """
CREATE TABLE IF NOT EXISTS daily_weather (
   date DATE NOT NULL,
   daily_weather_type VARCHAR(50),
   avg_temperature FLOAT,
   avg_precipitation FLOAT, 
   avg_windspeed FLOAT
);
"""

TAXI_TRIPS_SCHEMA = """
CREATE TABLE IF NOT EXISTS taxi_trips (
   pickup_datetime TIMESTAMP NOT NULL,
   dropoff_datetime TIMESTAMP NOT NULL,
   rate_code_id FLOAT,
   trip_distance FLOAT NOT NULL,
   extra FLOAT,
   mta_tax FLOAT,
   tip_amount FLOAT,
   tolls_amount FLOAT,
   improvement_surcharge FLOAT,
   total_amount FLOAT,
   congestion_surcharge FLOAT,
   airport_fee FLOAT,
   pickup_coords VARCHAR(50),
   dropoff_coords VARCHAR(50),
   weekday_num INTEGER NOT NULL,
   airport VARCHAR(50)
);
"""

UBER_TRIPS_SCHEMA = """
CREATE TABLE IF NOT EXISTS uber_trips (
   hvfhs_license_num VARCHAR(50) NOT NULL,
   pickup_datetime TIMESTAMP NOT NULL,
   dropoff_datetime TIMESTAMP NOT NULL,
   trip_miles FLOAT,
   base_passenger_fare FLOAT,
   tolls FLOAT,
   sales_tax FLOAT,
   congestion_surcharge FLOAT,
   airport_fee FLOAT,
   driver_pay FLOAT,
   bcf FLOAT,
   pickup_coords VARCHAR(50),
   dropoff_coords VARCHAR(50),
   weekday_num INTEGER NOT NULL,
   total_amount FLOAT,
   airport VARCHAR(50)
);
"""



# create that required schema.sql file
with open(DATABASE_SCHEMA_FILE, "w") as f:
    f.write(HOURLY_WEATHER_SCHEMA)
    f.write(DAILY_WEATHER_SCHEMA)
    f.write(TAXI_TRIPS_SCHEMA)
    f.write(UBER_TRIPS_SCHEMA)

# create the tables with the schema files
with engine.connect() as connection:
    pass

def write_dataframes_to_table(table_to_df_dict):
    with engine.connect() as conn:
        conn.execute(db.text(HOURLY_WEATHER_SCHEMA))
        conn.execute(db.text(DAILY_WEATHER_SCHEMA))
        conn.execute(db.text(TAXI_TRIPS_SCHEMA))
        conn.execute(db.text(UBER_TRIPS_SCHEMA))  # 添加 UBER_TRIPS_SCHEMA
        conn.commit()
        
        for table_name, df in table_to_df_dict.items():
            if table_name == "hourly_weather":
                df = df.rename(columns={
                    'date': 'date',
                    'hourly weather type': 'hourly_weather_type',
                    'hourly temperature': 'hourly_temperature',
                    'hourly precipitation': 'hourly_precipitation',
                    'hourly windspeed': 'hourly_windspeed',
                    'hour': 'hour',
                    'weekday_num': 'weekday_num',
                    'severe weather': 'severe_weather'
                })
            elif table_name == "daily_weather":
                df = df.rename(columns={
                    'date': 'date',
                    'daily weather type': 'daily_weather_type',
                    'daily temperature': 'avg_temperature',
                    'daily precipitation': 'avg_precipitation',
                    'daily windspeed': 'avg_windspeed'
                })
            elif table_name == "taxi_trips":
                df = df.rename(columns={
                    'pickup_datetime': 'pickup_datetime',
                    'dropoff_datetime': 'dropoff_datetime',
                    'RatecodeID': 'rate_code_id',
                    'trip_distance': 'trip_distance',
                    'extra': 'extra',
                    'mta_tax': 'mta_tax',
                    'tip_amount': 'tip_amount',
                    'tolls_amount': 'tolls_amount',
                    'improvement_surcharge': 'improvement_surcharge',
                    'total_amount': 'total_amount',
                    'congestion_surcharge': 'congestion_surcharge',
                    'Airport_fee': 'airport_fee',
                    'pickup_coords': 'pickup_coords',
                    'dropoff_coords': 'dropoff_coords',
                    'weekday_num': 'weekday_num',
                    'airport': 'airport'
                })
            elif table_name == "uber_trips":
                df = df.rename(columns={
                    'hvfhs_license_num': 'hvfhs_license_num',
                    'pickup_datetime': 'pickup_datetime',
                    'dropoff_datetime': 'dropoff_datetime',
                    'trip_miles': 'trip_miles',
                    'base_passenger_fare': 'base_passenger_fare',
                    'tolls': 'tolls',
                    'sales_tax': 'sales_tax',
                    'congestion_surcharge': 'congestion_surcharge',
                    'airport_fee': 'airport_fee',
                    'driver_pay': 'driver_pay',
                    'bcf': 'bcf',
                    'pickup_coords': 'pickup_coords',
                    'dropoff_coords': 'dropoff_coords',
                    'weekday_num': 'weekday_num',
                    'total_amount': 'total_amount',
                    'airport': 'airport'
                })
                
            df.to_sql(
                name=table_name,
                con=engine,
                if_exists='append',
                index=False
            )
            print(f"Successfully wrote {len(df)} rows to table {table_name}")
map_table_name_to_dataframe = {
    "taxi_trips": taxi_data,
    "uber_trips": uber_data,
    "hourly_weather": hourly_weather_data,
    "daily_weather": daily_weather_data,
}

write_dataframes_to_table(map_table_name_to_dataframe)

