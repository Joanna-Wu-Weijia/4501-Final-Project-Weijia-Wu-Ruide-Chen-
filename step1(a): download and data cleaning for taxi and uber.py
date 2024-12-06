import os
import re
import requests
import bs4
import matplotlib.pyplot as plt
import pandas as pd
import geopandas as gpd
import math
import sqlalchemy as db
from bs4 import BeautifulSoup

TLC_URL = "https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page"

TAXI_ZONES_DIR = ""
TAXI_ZONES_SHAPEFILE = f"{TAXI_ZONES_DIR}/taxi_zones.shp"
WEATHER_CSV_DIR = ""

CRS = 4326  # coordinate reference system

# (lat, lon)
NEW_YORK_BOX_COORDS = ((40.560445, -74.242330), (40.908524, -73.717047))
LGA_BOX_COORDS = ((40.763589, -73.891745), (40.778865, -73.854838))
JFK_BOX_COORDS = ((40.639263, -73.795642), (40.651376, -73.766264))
EWR_BOX_COORDS = ((40.686794, -74.194028), (40.699680, -74.165205))

DATABASE_URL = "sqlite:///project.db"
DATABASE_SCHEMA_FILE = "schema.sql"
QUERY_DIRECTORY = "queries"

try:
    os.mkdir(QUERY_DIRECTORY)
except Exception as e:
    if e.errno == 17:
        # the directory already exists
        pass
    else:
        raise

def load_taxi_zones():
    try:
        # 读取shapefile并立即转换到WGS84坐标系统
        taxi_zones = gpd.read_file('taxi_zones.shp').to_crs(CRS)
        return taxi_zones[['LocationID', 'zone', 'borough', 'geometry']]
    except Exception as e:
        print(f"Error loading shapefile: {e}")
        raise ValueError("Could not load taxi zones shapefile")

def lookup_coords_for_taxi_zone_id(zone_loc_id, loaded_taxi_zones):
    try:
        # 检查zone_loc_id
        zone = loaded_taxi_zones[loaded_taxi_zones['LocationID'] == zone_loc_id]
        if zone.empty:
            raise ValueError(f"No taxi zone found for LocationID: {zone_loc_id}")
        # 这里的geometry已经是WGS84格式，直接获取中心点
        centroid = zone.geometry.centroid.iloc[0]
        return (centroid.y, centroid.x)  # 返回(latitude, longitude)
    except ValueError as e:
        print(f"Value error: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error looking up coordinates: {e}")
        return None


# Calculate sample size using Cochran's formula
def calculate_sample_size(population, confidence_level=0.95, margin_of_error=0.05, p=0.5):
    z = {0.90: 1.645, 0.95: 1.96, 0.99: 2.576}[confidence_level]
    e = margin_of_error
    numerator = (z**2) * p * (1 - p)
    denominator = e**2
    sample_size = numerator / denominator
    if population:
        sample_size = (sample_size * population) / (sample_size + population - 1)
    return math.ceil(sample_size)


def get_all_urls_from_tlc_page(TLC_URL):
    response = requests.get(TLC_URL)
    soup = BeautifulSoup(response.content, 'html.parser')
    links = soup.find_all('a', href=True)
    urls = [link['href'] for link in links]
    return urls

def find_taxi_parquet_urls(all_urls):
    yellow_taxi_pattern = re.compile(r'.*yellow_trip[-]?data_202[0-4]-(0[1-9]|1[0-2])\.parquet$', re.IGNORECASE)
    yellow_taxi_links = [url.strip() for url in all_urls if yellow_taxi_pattern.match(url.strip())]
    print(f"Found {len(yellow_taxi_links)} yellow taxi parquet files")  #should be 57
    if len(yellow_taxi_links) > 0:
        print("Sample URL:", yellow_taxi_links[0])
    return yellow_taxi_links

def find_uber_parquet_urls(all_urls):
    uber_pattern = re.compile(r'.*fhvhv_trip[-]?data_202[0-4]-(0[1-9]|1[0-2])\.parquet$', re.IGNORECASE)
    uber_links = [url.strip() for url in all_urls if uber_pattern.match(url.strip())]
    print(f"Found {len(uber_links)} fhvhv parquet files")  #should be 57
    if len(uber_links) > 0:
        print("Sample URL:", uber_links[0])
    return uber_links


def get_and_clean_taxi_month(url):
   try:
       filename = url.split('/')[-1]
       if os.path.exists(f"data/{filename}"):
           taxi_df = pd.read_parquet(f"data/{filename}")
       else:
           taxi_df = pd.read_parquet(url)
           os.makedirs("data", exist_ok=True)
           taxi_df.to_parquet(f"data/{filename}")
       
       # get sample with sample size calculate
       population_size = len(taxi_df)
       sample_size = calculate_sample_size(population_size)
       taxi_df = taxi_df.sample(n=sample_size, random_state=42)
       
       # column need to keep
       required_columns = [
           'tpep_pickup_datetime', 'tpep_dropoff_datetime',
           'PULocationID', 'DOLocationID', 'RatecodeID'
       ]
       optional_columns = [
           'trip_distance', 'extra', 'mta_tax', 'tip_amount', 
           'tolls_amount', 'improvement_surcharge', 'total_amount',
           'congestion_surcharge', 'Airport_fee'
       ]
       
       missing_columns = [col for col in required_columns if col not in taxi_df.columns]
       if missing_columns:
           raise ValueError(f"Missing required columns: {missing_columns}")
       
       available_columns = required_columns + [col for col in optional_columns if col in taxi_df.columns]
       taxi_df = taxi_df[available_columns]
       
       # load taxi zone and apply cord
       loaded_taxi_zones = load_taxi_zones()
       taxi_df['pickup_coords'] = taxi_df['PULocationID'].apply(
           lambda loc_id: lookup_coords_for_taxi_zone_id(loc_id, loaded_taxi_zones)
       )
       taxi_df['dropoff_coords'] = taxi_df['DOLocationID'].apply(
           lambda loc_id: lookup_coords_for_taxi_zone_id(loc_id, loaded_taxi_zones)
       )
       taxi_df = taxi_df.dropna(subset=['pickup_coords', 'dropoff_coords'])
       
       # processing datetime data
       taxi_df['tpep_pickup_datetime'] = pd.to_datetime(taxi_df['tpep_pickup_datetime'])
       taxi_df['tpep_dropoff_datetime'] = pd.to_datetime(taxi_df['tpep_dropoff_datetime'])
       taxi_df["weekday_num"] = taxi_df["tpep_dropoff_datetime"].dt.weekday + 1
       
       # total_amount
       taxi_df['total_amount'] = taxi_df.apply(
           lambda row: (
               row['extra'] + row['fare_amount'] + row['mta_tax'] + 
               row['airport_fee'] + row['Improvement_surcharge'] + 
               row['tolls_amount'] + row['congestion_surcharge']
           ) if pd.isna(row['total_amount']) and 
                row[['fare_amount']].notna().all()
           else row['total_amount'],
           axis=1
       )
       
       # airport
       taxi_df['airport'] = 'not airport'
       taxi_df.loc[taxi_df['RatecodeID'] == 2, 'airport'] = 'JFK'
       taxi_df.loc[taxi_df['RatecodeID'] == 3, 'airport'] = 'EWR'
       taxi_df.loc[
           (taxi_df['Airport_fee'] == 1.75) & (taxi_df['RatecodeID'] != 2),
           'airport'
       ] = 'LGA'

       taxi_df = taxi_df.drop(columns=['PULocationID', 'DOLocationID'])
       taxi_df = taxi_df.rename(columns={'tpep_pickup_datetime': 'pickup_datetime', 'tpep_dropoff_datetime': 'dropoff_datetime'})
       columns_to_fill = [
           'trip_distance', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 
           'improvement_surcharge', 'total_amount', 'congestion_surcharge', 'Airport_fee'
            ]
       taxi_df[columns_to_fill] = taxi_df[columns_to_fill].fillna(0)
       taxi_df['pickup_coords'] = taxi_df['pickup_coords'].apply(lambda x: f"{x[0]},{x[1]}")
       taxi_df['dropoff_coords'] = taxi_df['dropoff_coords'].apply(lambda x: f"{x[0]},{x[1]}")
       
       return taxi_df
       
   except Exception as e:
       print(f"Error processing {url}: {e}")
       return None


def get_and_clean_taxi_data(parquet_urls):
    all_taxi_dataframes = []
    
    for parquet_url in parquet_urls:
        taxi_df = get_and_clean_taxi_month(parquet_url)
        if taxi_df is not None:
            all_taxi_dataframes.append(taxi_df)
    
    if not all_taxi_dataframes:
        raise ValueError("No valid taxi data found")
        
    taxi_data = pd.concat(all_taxi_dataframes)
    return taxi_data

def get_taxi_data():
    all_urls = get_all_urls_from_tlc_page(TLC_URL)
    all_parquet_urls = find_taxi_parquet_urls(all_urls)
    taxi_data = get_and_clean_taxi_data(all_parquet_urls)
    return taxi_data

taxi_data = get_taxi_data()

def get_and_clean_uber_month(url):
    try:
        filename = url.split('/')[-1]
        if os.path.exists(f"data/{filename}"):
            uber_df = pd.read_parquet(f"data/{filename}")
        else:
            uber_df = pd.read_parquet(url)
            os.makedirs("data", exist_ok=True)
            uber_df.to_parquet(f"data/{filename}")
        
        # get sample
        population_size2 = len(uber_df)
        sample_size2 = calculate_sample_size(population_size2)
        uber_df = uber_df.sample(n=sample_size2, random_state=42)
        
        required_columns = [
            'hvfhs_license_num', 'pickup_datetime', 'dropoff_datetime', 'PULocationID', 'DOLocationID'
        ]
        
        optional_columns = [
            'trip_miles', 'base_passenger_fare', 'tolls', 'sales_tax', 'congestion_surcharge',
            'airport_fee', 'driver_pay', 'bcf'
        ]
        if not all(col in uber_df.columns for col in required_columns):
            raise ValueError(f"Missing required columns: {[col for col in required_columns if col not in uber_df.columns]}")

        # get available column and uber data only
        available_columns = required_columns + [col for col in optional_columns if col in uber_df.columns]
        uber_df = uber_df[available_columns]
        uber_df = uber_df[uber_df['hvfhs_license_num'] == 'HV0003']
        
        # load taxi zone
        loaded_taxi_zones = load_taxi_zones()
        
        # apply to cord
        uber_df['pickup_coords'] = uber_df['PULocationID'].apply(
            lambda loc_id: lookup_coords_for_taxi_zone_id(loc_id, loaded_taxi_zones)
        )
        uber_df['dropoff_coords'] = uber_df['DOLocationID'].apply(
            lambda loc_id: lookup_coords_for_taxi_zone_id(loc_id, loaded_taxi_zones)
        )
        uber_df = uber_df.dropna(subset=['pickup_coords', 'dropoff_coords'])
        
        # datetime
        uber_df['pickup_datetime'] = pd.to_datetime(uber_df['pickup_datetime'])
        uber_df['dropoff_datetime'] = pd.to_datetime(uber_df['dropoff_datetime'])
        uber_df["weekday_num"] = uber_df["dropoff_datetime"].dt.weekday + 1
        
        # total amount
        uber_df['total_amount'] = uber_df.apply(
            lambda row: (
                row['base_passenger_fare'] + row['tolls'] + row['sales_tax'] + 
                row['airport_fee'] + row['congestion_surcharge'] + 
                row['driver_pay'] + row['bcf']
            ), 
            axis=1
        )
                #get airport info
        def haversine(lat1, lon1, lat2, lon2):
            R = 6371  
            lat1, lon1, lat2, lon2 = map(radians,[lat1, lon1, lat2, lon2])
            dlat = lat2 - lat1
            dlon = lon2 - lon1
            a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
            c = 2 * atan2(sqrt(a), sqrt(1 - a))
            distance = R * c  
            return distance


        airports = {
            "JFK": {"lat": 40.6413, "lon": -73.7781, "radius": 5},
            "LGA": {"lat": 40.7769, "lon": -73.8740, "radius": 5},
            "EWR": {"lat": 40.6895, "lon": -74.1745, "radius": 5}
        }
        
        def assign_airport(row):
            pickup_coords = row['pickup_coords']
            dropoff_coords = row['dropoff_coords']
            pickup_lat, pickup_lon = pickup_coords
            dropoff_lat, dropoff_lon = dropoff_coords
            
            for airport, info in airports.items():
                pickup_distance = haversine(pickup_lat, pickup_lon, info['lat'], info['lon'])
                if pickup_distance <= info['radius']:
                    return airport
                    
            for airport, info in airports.items():
                dropoff_distance = haversine(dropoff_lat, dropoff_lon, info['lat'], info['lon'])
                if dropoff_distance <= info['radius']:
                    return airport
            
            return "not airport"
        
        uber_df = uber_df.drop(columns=['PULocationID', 'DOLocationID'])
        
        columns_to_fill = [
            'trip_miles', 'base_passenger_fare', 'tolls', 'sales_tax', 'congestion_surcharge',
            'airport_fee', 'driver_pay', 'bcf'
        ]
        uber_df[columns_to_fill] = uber_df[columns_to_fill].fillna(0)
        uber_df['pickup_coords'] = uber_df['pickup_coords'].apply(lambda x: f"{x[0]},{x[1]}")
        uber_df['dropoff_coords'] = uber_df['dropoff_coords'].apply(lambda x: f"{x[0]},{x[1]}")
        
        return uber_df
        
    except Exception as e:
        print(f"Error processing {url}: {e}")
        return None

def get_and_clean_uber_data(parquet_urls):
    all_uber_dataframes = []
    
    for parquet_url in parquet_urls:
        # maybe: first try to see if you've downloaded this exact
        # file already and saved it before trying again
        dataframe = get_and_clean_uber_month(parquet_url)
        # maybe: if the file hasn't been saved, save it so you can
        # avoid re-downloading it if you re-run the function
        
        all_uber_dataframes.append(dataframe)

    # create one gigantic dataframe with data from every month needed
    uber_data = pd.concat(all_uber_dataframes)
    return uber_data

def get_uber_data():
    all_urls = get_all_urls_from_tlc_page(TLC_URL)
    all_parquet_urls = find_uber_parquet_urls(all_urls)
    taxi_data = get_and_clean_uber_data(all_parquet_urls)
    return taxi_data

uber_data = get_uber_data()



