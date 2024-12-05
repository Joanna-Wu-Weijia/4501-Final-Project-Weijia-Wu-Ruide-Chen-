def get_all_weather_csvs(directory=None):
    weather_urls = [
        "https://raw.githubusercontent.com/Joanna-Wu-Weijia/4501-Final-Project/refs/heads/main/weather%20data/2020_weather.csv",
        "https://raw.githubusercontent.com/Joanna-Wu-Weijia/4501-Final-Project/refs/heads/main/weather%20data/2021_weather.csv",
        "https://raw.githubusercontent.com/Joanna-Wu-Weijia/4501-Final-Project/refs/heads/main/weather%20data/2022_weather.csv",
        "https://raw.githubusercontent.com/Joanna-Wu-Weijia/4501-Final-Project/refs/heads/main/weather%20data/2023_weather.csv",
        "https://raw.githubusercontent.com/Joanna-Wu-Weijia/4501-Final-Project/refs/heads/main/weather%20data/2024_weather.csv",
    ]
    return weather_urls

def clean_month_weather_data_hourly(csv_file):
    weather_data = pd.read_csv(csv_file)
   # 使用给定的列名选择和重命名列
    weather_data = weather_data[['DATE', 'HourlyPresentWeatherType', 'HourlyDryBulbTemperature', 'HourlyPrecipitation','HourlyWindSpeed']]
    weather_data = weather_data.rename(columns={
        'DATE': 'date',
        'HourlyPresentWeatherType': 'hourly weather type', 
        'HourlyDryBulbTemperature': 'hourly temperature',
        'HourlyPrecipitation': 'hourly precipitation',
        'HourlyWindSpeed': 'hourly windspeed'
    })
   
   # 转换数据类型
    weather_data['hourly temperature'] = pd.to_numeric(weather_data['hourly temperature'], errors='coerce')
    weather_data['hourly precipitation'] = pd.to_numeric(weather_data['hourly precipitation'], errors='coerce')
    weather_data['hourly windspeed'] = pd.to_numeric(weather_data['hourly windspeed'], errors='coerce')

   # 天气类型映射
    weather_mapping = {
       '-RA:02 |RA |RA': 'rain',
       '-RA:02 BR:1 |RA |RA': 'rain/mist',
       'BR:1 ||': 'mist',
       'HZ:7 |FU |HZ': 'haze/smoke',
       'RA:02 BR:1 |RA |RA': 'rain/mist',
       'FG:2 |FG |': 'fog',
       '-SN:03 |SN |': 'snow',
       '-SN:03 BR:1 |SN |': 'snow/mist',
       '+RA:02 |RA |RA': 'rain',
       '|SN |': 'snow',
       '+SN:03 |SN s |': 'heavy snow',
       '+SN:03 FZ:8 FG:2 |FG SN |': 'snow/frezzing/fog',
       '-SN:03 FZ:8 FG:2 |FG SN |': 'snow/frezzing/fog',
       'SN:03 FZ:8 FG:2 |FG SN |': 'snow/frezzing/fog',
       '+RA:02 FG:2 |FG RA |RA': 'rain/fog',
       'HZ:7 ||HZ': 'haze',
       '|RA |': 'rain',
       'RA:02 |RA |RA': 'rain',
       'UP:09 ||': 'unknown',
       'UP:09 BR:1 ||': 'mist',
       '+RA:02 BR:1 |RA |RA': 'rain',
       '-RA:02 ||': 'rain',
       'RA:02 FG:2 |FG RA |RA': 'rain/fog',
       '-RA:02 FG:2 |FG RA |RA': 'rain/fog',
       'SN:03 |SN s |s': 'snow',
       '-SN:03 FG:2 |FG SN |': 'snow/fog',
       'SN:03 FG:2 |FG SN |': 'snow/fog'
   }
   # 应用天气类型映射
    weather_data['hourly weather type'] = weather_data['hourly weather type'].map(weather_mapping)
   
   # 处理precipitation为0和缺失值的情况
    weather_data.loc[weather_data['hourly precipitation'] == 0, 'hourly weather type'] = 'sunny'
    weather_data.loc[
       (weather_data['hourly precipitation'].isna()) & 
       (weather_data['hourly weather type'].isna()), 
       'hourly weather type'
    ] = 'unknown'
   
   # 转换日期并添加新列
    weather_data['date'] = pd.to_datetime(weather_data['date'])
    weather_data['hour'] = weather_data['date'].dt.hour
    weather_data['weekday_num'] = weather_data['date'].dt.weekday
   
   # 创建极端天气标志
    severe_weather_conditions = [
       '-SN:03 |SN |', '-SN:03 BR:1 |SN |', '+RA:02 |RA |RA', '|SN |',
       '+SN:03 |SN s |', '+SN:03 FZ:8 FG:2 |FG SN |', '-SN:03 FZ:8 FG:2 |FG SN |',
       'SN:03 FZ:8 FG:2 |FG SN |', '+RA:02 FG:2 |FG RA |RA', '+RA:02 BR:1 |RA |RA',
       'SN:03 |SN s |s', '-SN:03 FG:2 |FG SN |', 'SN:03 FG:2 |FG SN |'
    ]
    
    weather_data['severe weather'] = 0  # 默认为0
    
    # 设置极端天气条件为1
    weather_data.loc[weather_data['hourly weather type'].isin(
   [weather_mapping[condition] for condition in severe_weather_conditions]
    ), 'severe weather'] = 1
    # unknown天气设为空
    weather_data.loc[weather_data['hourly weather type'] == 'unknown', 'severe weather'
    ] = None
    
    return weather_data






import pandas as pd
import numpy as np

def clean_month_weather_data_daily(csv_file):
    # Load the weather data
    weather_data = pd.read_csv(csv_file)
    weather_data = weather_data[['DATE', 'HourlyPresentWeatherType']]
    weather_data.columns = ['date', 'weather_type']

    # Weather mapping dictionary
    weather_mapping = {
        '-RA:02 |RA |RA': 'rain',
        '-RA:02 BR:1 |RA |RA': 'rain', 
        'RA:02 BR:1 |RA |RA': 'rain',
        '+RA:02 |RA |RA': 'rain',
        '+RA:02 FG:2 |FG RA |RA': 'rain',
        '|RA |': 'rain',
        'RA:02 |RA |RA': 'rain',
        '+RA:02 BR:1 |RA |RA': 'rain',
        '-RA:02 ||': 'rain',
        'RA:02 FG:2 |FG RA |RA': 'rain',
        '-RA:02 FG:2 |FG RA |RA': 'rain',
        '-SN:03 |SN |': 'snow',
        '-SN:03 BR:1 |SN |': 'snow',
        '|SN |': 'snow',
        '+SN:03 |SN s |': 'snow',
        '+SN:03 FZ:8 FG:2 |FG SN |': 'snow',
        '-SN:03 FZ:8 FG:2 |FG SN |': 'snow',
        'SN:03 FZ:8 FG:2 |FG SN |': 'snow',
        'SN:03 |SN s |s': 'snow',
        '-SN:03 FG:2 |FG SN |': 'snow',
        'SN:03 FG:2 |FG SN |': 'snow',
        'BR:1 ||': 'other',
        'HZ:7 |FU |HZ': 'other',
        'FG:2 |FG |': 'other',
        'HZ:7 ||HZ': 'other',
        'UP:09 ||': 'unknown',
        'UP:09 BR:1 ||': 'other'
    }

    # Map weather types to more general categories
    weather_data['MappedWeather'] = weather_data['weather_type'].map(weather_mapping).fillna('other')

    # Group by DATE and aggregate
    def determine_weather_type(weather_series):
        if 'snow' in weather_series.values:
            return 'snow'
        elif 'rain' in weather_series.values:
            return 'rain'
        else:
            return 'other'

    # Aggregating daily data
    daily_aggregated = weather_data.groupby('date').agg(
        DailyWeatherType=('MappedWeather', determine_weather_type)
    ).reset_index()

    return daily_aggregated
