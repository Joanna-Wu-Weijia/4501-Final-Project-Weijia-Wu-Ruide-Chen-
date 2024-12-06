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
       # 检查是否已下载
       filename = url.split('/')[-1]
       if os.path.exists(f"data/{filename}"):
           taxi_df = pd.read_parquet(f"data/{filename}")
       else:
           # 下载数据
           taxi_df = pd.read_parquet(url)
           # 保存到本地
           os.makedirs("data", exist_ok=True)
           taxi_df.to_parquet(f"data/{filename}")
       
       # 计算样本量并抽样
       population_size = len(taxi_df)
       sample_size = calculate_sample_size(population_size)
       taxi_df = taxi_df.sample(n=sample_size, random_state=42)
       
       # 定义必需列和可选列
       required_columns = [
           'tpep_pickup_datetime', 'tpep_dropoff_datetime',
           'PULocationID', 'DOLocationID', 'RatecodeID'
       ]
       optional_columns = [
           'trip_distance', 'extra', 'mta_tax', 'tip_amount', 
           'tolls_amount', 'improvement_surcharge', 'total_amount',
           'congestion_surcharge', 'Airport_fee'
       ]
       # 检查必需列是否存在
       missing_columns = [col for col in required_columns if col not in taxi_df.columns]
       if missing_columns:
           raise ValueError(f"Missing required columns: {missing_columns}")
       
       # 获取可用的列并筛选
       available_columns = required_columns + [col for col in optional_columns if col in taxi_df.columns]
       taxi_df = taxi_df[available_columns]
       
       # 加载taxi zones并处理坐标
       loaded_taxi_zones = load_taxi_zones()
       taxi_df['pickup_coords'] = taxi_df['PULocationID'].apply(
           lambda loc_id: lookup_coords_for_taxi_zone_id(loc_id, loaded_taxi_zones)
       )
       taxi_df['dropoff_coords'] = taxi_df['DOLocationID'].apply(
           lambda loc_id: lookup_coords_for_taxi_zone_id(loc_id, loaded_taxi_zones)
       )
       
       # 清理坐标数据
       taxi_df = taxi_df.dropna(subset=['pickup_coords', 'dropoff_coords'])
       
       def is_in_nyc(coords):
           if not coords:
               return False
           lat, lon = coords
           return (NEW_YORK_BOX_COORDS[0][0] <= lat <= NEW_YORK_BOX_COORDS[1][0] and
                  NEW_YORK_BOX_COORDS[0][1] <= lon <= NEW_YORK_BOX_COORDS[1][1])
       
       taxi_df = taxi_df[taxi_df['pickup_coords'].apply(is_in_nyc) & 
                        taxi_df['dropoff_coords'].apply(is_in_nyc)]
       
       # 处理时间相关数据
       taxi_df['tpep_pickup_datetime'] = pd.to_datetime(taxi_df['tpep_pickup_datetime'])
       taxi_df['tpep_dropoff_datetime'] = pd.to_datetime(taxi_df['tpep_dropoff_datetime'])
       taxi_df["weekday_num"] = taxi_df["tpep_dropoff_datetime"].dt.weekday + 1
       
       # 计算total_amount
       taxi_df['total_amount'] = taxi_df.apply(
           lambda row: (
               row['extra'] + row['fare_amount'] + row['mta_tax'] + 
               row['airport_fee'] + row['Improvement_surcharge'] + 
               row['tolls_amount'] + row['congestion_surcharge']
           ) if pd.isna(row['total_amount']) and 
                row[['fare_amount', 'mta_tax']].notna().all()
           else row['total_amount'],
           axis=1
       )
       
       # 设置机场信息
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
       
       return taxi_df
       
   except Exception as e:
       print(f"Error processing {url}: {e}")
       return None

def get_and_clean_uber_month(url):
    try:
        # 检查是否已下载
        filename = url.split('/')[-1]
        if os.path.exists(f"data/{filename}"):
            uber_df = pd.read_parquet(f"data/{filename}")
        else:
            # 下载数据
            uber_df = pd.read_parquet(url)
            # 保存到本地
            os.makedirs("data", exist_ok=True)
            uber_df.to_parquet(f"data/{filename}")
        
        # 计算样本量
        population_size2 = len(uber_df)
        sample_size2 = calculate_sample_size(population_size2)
        uber_df = uber_df.sample(n=sample_size2, random_state=42)
        
        # 定义必需列和可选列
        required_columns = [
            'hvfhs_license_num', 'pickup_datetime', 'dropoff_datetime', 'PULocationID', 'DOLocationID'
        ]
        
        optional_columns = [
            'trip_miles', 'base_passenger_fare', 'tolls', 'sales_tax', 'congestion_surcharge',
            'airport_fee', 'driver_pay', 'bcf'
        ]
        
        # 检查必需列是否存在
        if not all(col in uber_df.columns for col in required_columns):
            raise ValueError(f"Missing required columns: {[col for col in required_columns if col not in uber_df.columns]}")
        
        # 获取可用的可选列
        available_columns = required_columns + [col for col in optional_columns if col in uber_df.columns]
        uber_df = uber_df[available_columns]
        
        # 过滤Uber数据，只选择hvfhs_license_num为'HV0003'的数据
        uber_df = uber_df[uber_df['hvfhs_license_num'] == 'HV0003']
        
        # 加载 taxi zones 数据
        loaded_taxi_zones = load_taxi_zones()
        
        # 转换位置ID到坐标
        uber_df['pickup_coords'] = uber_df['PULocationID'].apply(
            lambda loc_id: lookup_coords_for_taxi_zone_id(loc_id, loaded_taxi_zones)
        )
        uber_df['dropoff_coords'] = uber_df['DOLocationID'].apply(
            lambda loc_id: lookup_coords_for_taxi_zone_id(loc_id, loaded_taxi_zones)
        )
        uber_df = uber_df.dropna(subset=['pickup_coords', 'dropoff_coords'])
        
        # 验证坐标是否在纽约范围内
        def is_in_nyc(coords):
            if not coords:
                return False
            lat, lon = coords
            return (NEW_YORK_BOX_COORDS[0][0] <= lat <= NEW_YORK_BOX_COORDS[1][0] and
                    NEW_YORK_BOX_COORDS[0][1] <= lon <= NEW_YORK_BOX_COORDS[1][1])
        
        uber_df = uber_df[uber_df['pickup_coords'].apply(is_in_nyc) & 
                          uber_df['dropoff_coords'].apply(is_in_nyc)]
        
        # 数据类型转换和时间验证
        uber_df['pickup_datetime'] = pd.to_datetime(uber_df['pickup_datetime'])
        uber_df['dropoff_datetime'] = pd.to_datetime(uber_df['dropoff_datetime'])
        uber_df["weekday_num"] = uber_df["dropoff_datetime"].dt.weekday + 1
        
        # 计算总金额
        uber_df['total_amount'] = uber_df.apply(
            lambda row: (
                row['base_passenger_fare'] + row['tolls'] + row['sales_tax'] + 
                row['airport_fee'] + row['congestion_surcharge'] + 
                row['driver_pay'] + row['bcf']
            ), 
            axis=1
        )
        
        # 删除不需要的列
        uber_df = uber_df.drop(columns=['PULocationID', 'DOLocationID'])
        
        # 填充空白值为 0
        columns_to_fill = [
            'trip_miles', 'base_passenger_fare', 'tolls', 'sales_tax', 'congestion_surcharge',
            'airport_fee', 'driver_pay', 'bcf'
        ]
        uber_df[columns_to_fill] = uber_df[columns_to_fill].fillna(0)
        
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


def get_all_weather_csvs(directory=None):
    weather_urls = [
        "https://raw.githubusercontent.com/Joanna-Wu-Weijia/4501-Final-Project/refs/heads/main/weather%20data/2020_weather.csv",
        "https://raw.githubusercontent.com/Joanna-Wu-Weijia/4501-Final-Project/refs/heads/main/weather%20data/2021_weather.csv",
        "https://raw.githubusercontent.com/Joanna-Wu-Weijia/4501-Final-Project/refs/heads/main/weather%20data/2022_weather.csv",
        "https://raw.githubusercontent.com/Joanna-Wu-Weijia/4501-Final-Project/refs/heads/main/weather%20data/2023_weather.csv",
        "https://raw.githubusercontent.com/Joanna-Wu-Weijia/4501-Final-Project/refs/heads/main/weather%20data/2024_weather.csv",
    ]
    return weather_urls



