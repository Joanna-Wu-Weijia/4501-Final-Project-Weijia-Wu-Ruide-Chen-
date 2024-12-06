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
TODO
"""






def write_dataframes_to_table(table_to_df_dict):
   with engine.connect() as conn:
       conn.execute(db.text(HOURLY_WEATHER_SCHEMA))
       conn.execute(db.text(DAILY_WEATHER_SCHEMA))
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
                df = taxi_data.rename(columns={
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
          df.to_sql(
               name=table_name,
               con=engine,
               if_exists='append',
               index=False
           )
          print(f"Successfully wrote {len(df)} rows to table {table_name}")
