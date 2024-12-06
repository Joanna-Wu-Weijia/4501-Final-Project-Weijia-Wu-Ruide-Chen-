### v1:
def plot_hourly_taxi_distribution(dataframe):
    """
    Plot the hourly distribution of taxi rides with both ride counts and percentages.
    """
    figure, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 12))

    ax1.bar(dataframe['X'], dataframe['Y'], color='orange', alpha=0.7)
    ax1.set_title('Hourly Distribution of Taxi Rides (2020-2024)', pad=20, size=14)
    ax1.set_xlabel('Hour of Day')
    ax1.set_ylabel('Number of Rides')
    ax1.grid(True, alpha=0.3)
    ax1.set_xticks(range(24))
    
    for i, v in enumerate(dataframe['Y']):
        ax1.text(i, v, str(v), ha='center', va='bottom')
    
    ax2.bar(dataframe['X'], dataframe['percentage'], color='green', alpha=0.7)
    ax2.set_title('Hourly Distribution of Taxi Rides (Percentage)', pad=20, size=14)
    ax2.set_xlabel('Hour of Day')
    ax2.set_ylabel('Percentage of Total Rides (%)')
    ax2.grid(True, alpha=0.3)
    ax2.set_xticks(range(24))
    
    for i, v in enumerate(dataframe['percentage']):
        ax2.text(i, v, f'{v:.2f}%', ha='center', va='bottom')
    
    plt.tight_layout()
    plt.show()

def get_hourly_taxi_data():
    return pd.read_csv('hourly_taxi_popularity.csv')

taxi_data = get_hourly_taxi_data()
plot_hourly_taxi_distribution(taxi_data)

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
plt.show()
