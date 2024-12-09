### v1:
def plot_hourly_taxi_distribution(dataframe: pd.DataFrame) -> None:
   r"""
   Create a two-panel visualization of hourly taxi ride distribution.

   Args:
       dataframe: DataFrame containing columns:
           - X: Hour of day (0-23)
           - Y: Number of rides
           - percentage: Percentage of total rides

   Notes:
       Creates two plots:
       - Top: Absolute number of rides by hour
       - Bottom: Percentage of total rides by hour
       Both include grid lines and value labels above each bar
   """
   # Set up figure with two subplots
   figure, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 12))

   # Plot absolute numbers
   ax1.bar(dataframe['X'], dataframe['Y'], color='orange', alpha=0.7)
   ax1.set_title('Hourly Distribution of Taxi Rides (2020-2024)', pad=20, size=14)
   ax1.set_xlabel('Hour of Day')
   ax1.set_ylabel('Number of Rides')
   ax1.grid(True, alpha=0.3)
   ax1.set_xticks(range(24))
   
   # Add value labels for absolute numbers
   for i, v in enumerate(dataframe['Y']):
       ax1.text(i, v, str(v), ha='center', va='bottom')
   
   # Plot percentages
   ax2.bar(dataframe['X'], dataframe['percentage'], color='green', alpha=0.7)
   ax2.set_title('Hourly Distribution of Taxi Rides (Percentage)', pad=20, size=14)
   ax2.set_xlabel('Hour of Day')
   ax2.set_ylabel('Percentage of Total Rides (%)')
   ax2.grid(True, alpha=0.3)
   ax2.set_xticks(range(24))
   
   # Add value labels for percentages
   for i, v in enumerate(dataframe['percentage']):
       ax2.text(i, v, f'{v:.2f}%', ha='center', va='bottom')
   
   plt.tight_layout()
   plt.show()


def get_hourly_taxi_data() -> pd.DataFrame:
   r"""
   Load hourly taxi ride data from CSV file.

   Returns:
       pd.DataFrame: Hourly taxi ride statistics
   """
   return pd.read_csv('hourly_taxi_popularity.csv')

### v2:
uber_data.rename(columns={'trip_miles': 'trip_distance'}, inplace=True)

combined_trips = pd.concat([taxi_data[['pickup_datetime', 'trip_distance']],
                            uber_data[['pickup_datetime', 'trip_distance']]])

start_date = '2020-01-01'
end_date = '2024-08-31'
combined_trips = combined_trips[(combined_trips['pickup_datetime'] >= start_date) &
                                (combined_trips['pickup_datetime'] <= end_date)]

combined_trips['month'] = combined_trips['pickup_datetime'].dt.month

# Group by month and calculate average distance and confidence interval
monthly_avg_distance = combined_trips.groupby('month')['trip_distance'].agg(['mean', 'count', 'std'])
z_value = 1.645  # for 90% CI

# Calculate 90% CI
monthly_avg_distance['sem'] = monthly_avg_distance['std'] / np.sqrt(monthly_avg_distance['count'])  
monthly_avg_distance['ci'] = z_value * monthly_avg_distance['sem']  

# plotting
monthly_avg_distance.reset_index(inplace=True)
months = monthly_avg_distance['month'].values.astype(float)
mean_values = monthly_avg_distance['mean'].values.astype(float)
lower_bound = (mean_values - monthly_avg_distance['ci'].values).astype(float)
upper_bound = (mean_values + monthly_avg_distance['ci'].values).astype(float)

plt.figure(figsize=(15, 12))
sns.lineplot(x=months, y=mean_values, label='Average Distance', color='blue')
plt.fill_between(months, lower_bound, upper_bound, color='b', alpha=0.2, label='90% Confidence Interval')

plt.xlabel('Month')
plt.ylabel('Average Distance (miles)')
plt.title('Average Distance Traveled per Month (January 2020 - August 2024)\nTaxis and Ubers Combined')
plt.xticks(ticks=range(1, 13), labels=['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'])
plt.legend()
plt.grid(visible=True)

### v3:
combined_trips = pd.concat([taxi_data[['pickup_datetime', 'trip_distance', 'dropoff_coords']],
                            uber_data[['pickup_datetime', 'trip_distance', 'dropoff_coords']]])

start_date = '2020-01-01'
end_date = '2024-08-31'
combined_trips = combined_trips[(combined_trips['pickup_datetime'] >= start_date) &
                                (combined_trips['pickup_datetime'] <= end_date)]

# bounding boxes for the three major airports
LGA_BOX_COORDS = ((40.763589, -73.891745), (40.778865, -73.854838))
JFK_BOX_COORDS = ((40.639263, -73.795642), (40.651376, -73.766264))
EWR_BOX_COORDS = ((40.686794, -74.194028), (40.699680, -74.165205))

# if a coordinate falls within a bounding box
def is_within_bbox(coord, bbox):
    lat, lon = coord
    (lat_min, lon_min), (lat_max, lon_max) = bbox
    return lat_min <= lat <= lat_max and lon_min <= lon <= lon_max

# weekday with most popular trip
combined_trips['weekday'] = combined_trips['pickup_datetime'].dt.day_name()

def determine_airport(row):
    coords = eval(row['dropoff_coords'].replace('POINT(', '').replace(')', ''))
    if is_within_bbox(coords, LGA_BOX_COORDS):
        return 'LGA'
    elif is_within_bbox(coords, JFK_BOX_COORDS):
        return 'JFK'
    elif is_within_bbox(coords, EWR_BOX_COORDS):
        return 'EWR'
    else:
        return None

combined_trips['airport'] = combined_trips.apply(determine_airport, axis=1)
airport_trips = combined_trips[combined_trips['airport'].isin(['LGA', 'JFK', 'EWR'])]
airport_popularity = airport_trips.groupby(['airport', 'weekday']).size().reset_index(name='count')
airport_popularity_pivot = airport_popularity.pivot(index='weekday', columns='airport', values='count').reindex(
    ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
)

plt.figure(figsize=(12, 6))
airport_popularity_pivot.plot(kind='bar', figsize=(15, 12))
plt.xlabel('Day of the Week')
plt.ylabel('Number of Drop-offs')
plt.title('Most Popular Drop-off Days by Airport (January 2020 - August 2024)')
plt.xticks(rotation=45)
plt.legend(title='Airport')
plt.grid(visible=True)

plt.tight_layout()
plt.show()

### v4
import pandas as pd
import matplotlib.pyplot as plt

# Simulated data structure (replace with actual data source if available)
data = {
    'month': pd.date_range(start='2020-01-01', end='2024-08-01', freq='MS').strftime('%Y-%m').tolist() * 2,
    'service_type': ['Yellow Taxi'] * 56 + ['Uber'] * 56,
    'total_amount': [50000 + (i * 1000) for i in range(56)] + [45000 + (i * 900) for i in range(56)],
    'fares': [40000 + (i * 800) for i in range(56)] + [36000 + (i * 700) for i in range(56)],
    'surcharges': [5000 + (i * 50) for i in range(56)] + [4500 + (i * 40) for i in range(56)],
    'taxes': [3000 + (i * 30) for i in range(56)] + [2500 + (i * 20) for i in range(56)],
    'tolls': [2000 + (i * 20) for i in range(56)] + [2000 + (i * 15) for i in range(56)],
}

# Create DataFrame
df = pd.DataFrame(data)

# Convert 'month' to datetime for easier plotting and analysis
df['month'] = pd.to_datetime(df['month'])

# Group by month and service type, aggregating values
monthly_data = df.groupby(['month', 'service_type']).sum().reset_index()

# Separate data for plotting
yellow_taxi_data = monthly_data[monthly_data['service_type'] == 'Yellow Taxi']
uber_data = monthly_data[monthly_data['service_type'] == 'Uber']

# Plotting the total amounts over time for both services
plt.figure(figsize=(12, 6))
plt.plot(yellow_taxi_data['month'], yellow_taxi_data['total_amount'], label='Yellow Taxi - Total Amount', marker='o')
plt.plot(uber_data['month'], uber_data['total_amount'], label='Uber - Total Amount', marker='o')
plt.title('Monthly Total Fares: Yellow Taxi vs. Uber (2020-2024)')
plt.xlabel('Month')
plt.ylabel('Total Amount ($)')
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.show()

# Stacked bar chart for fare breakdown (Fares, Surcharges, Taxes, Tolls)
fare_components = ['fares', 'surcharges', 'taxes', 'tolls']

# Yellow Taxi breakdown
yellow_taxi_data.set_index('month', inplace=True)
yellow_taxi_data[fare_components].plot(kind='bar', stacked=True, figsize=(12, 6))
plt.title('Yellow Taxi Monthly Fare Breakdown (2020-2024)')
plt.xlabel('Month')
plt.ylabel('Amount ($)')
plt.legend(title='Components')
plt.tight_layout()
plt.show()

# Uber breakdown
uber_data.set_index('month', inplace=True)
uber_data[fare_components].plot(kind='bar', stacked=True, figsize=(12, 6))
plt.title('Uber Monthly Fare Breakdown (2020-2024)')
plt.xlabel('Month')
plt.ylabel('Amount ($)')
plt.legend(title='Components')
plt.tight_layout()
plt.show()

###v5:
taxi_data['hour'] = taxi_data['pickup_datetime'].dt.round('H')
uber_data['hour'] = uber_data['pickup_datetime'].dt.round('H')
hourly_weather_data['hour'] = hourly_weather_data['date'].dt.round('H')

taxi_merged = pd.merge(taxi_data, hourly_weather_data[['hour', 'hourly precipitation']], on='hour', how='left')
uber_merged = pd.merge(uber_data, hourly_weather_data[['hour', 'hourly precipitation']], on='hour', how='left')

# removing any negative or zero distances
taxi_merged = taxi_merged[(taxi_merged['trip_distance'] > 0) & (taxi_merged['tip_amount'] > 0)]
uber_merged = uber_merged[(uber_merged['trip_distance'] > 0) & (uber_merged['tips'] > 0)]

# removing extremely large
taxi_merged = taxi_merged[
    (taxi_merged['trip_distance'] < np.percentile(taxi_merged['trip_distance'], 99)) &
    (taxi_merged['tip_amount'] < np.percentile(taxi_merged['tip_amount'], 99))
]

uber_merged = uber_merged[
    (uber_merged['trip_distance'] < np.percentile(uber_merged['trip_distance'], 99)) &
    (uber_merged['tips'] < np.percentile(uber_merged['tips'], 99))
]

# Plotting
fig, axes = plt.subplots(2, 2, figsize=(15, 10))

# Taxi tips vs. distance
axes[0, 0].scatter(taxi_merged['trip_distance'], taxi_merged['tip_amount'], alpha=0.5, color='blue')
axes[0, 0].set_xlabel('Distance (miles)')
axes[0, 0].set_ylabel('Tip Amount ($)')
axes[0, 0].set_title('Yellow Taxi Tips vs. Distance')

# Uber tips vs. distance
axes[0, 1].scatter(uber_merged['trip_distance'], uber_merged['tips'], alpha=0.5, color='red')
axes[0, 1].set_xlabel('Distance (miles)')
axes[0, 1].set_ylabel('Tip Amount ($)')
axes[0, 1].set_title('Uber Tips vs. Distance')

# Taxi tips vs. precipitation
axes[1, 0].scatter(taxi_merged['hourly precipitation'], taxi_merged['tip_amount'], alpha=0.5, color='green')
axes[1, 0].set_xlabel('Hourly Precipitation (inches)')
axes[1, 0].set_ylabel('Tip Amount ($)')
axes[1, 0].set_title('Yellow Taxi Tips vs. Precipitation')

# Uber tips vs. precipitation
axes[1, 1].scatter(uber_merged['hourly precipitation'], uber_merged['tips'], alpha=0.5, color='purple')
axes[1, 1].set_xlabel('Hourly Precipitation (inches)')
axes[1, 1].set_ylabel('Tip Amount ($)')
axes[1, 1].set_title('Uber Tips vs. Precipitation')

plt.tight_layout()
plt.show()

### v6:
m = folium.Map(location=[40.7128, -74.0060], zoom_start=11)

# only use pickup time since that is conclusiove
# taxi data
taxi_coords = [[float(coord.split(',')[0]), float(coord.split(',')[1])] 
               for coord in taxi_data['pickup_coords']]
HeatMap(taxi_coords, radius=15, gradient={0.4: 'yellow', 0.65: 'orange', 1: 'red'}).add_to(m)

# uber data
uber_coords = [[float(coord.split(',')[0]), float(coord.split(',')[1])] 
               for coord in uber_data['pickup_coords']]
HeatMap(uber_coords, radius=15, gradient={0.4: 'blue', 0.65: 'purple', 1: 'red'}).add_to(m)

m.save('nyc_rides_heatmap_2020.html')
