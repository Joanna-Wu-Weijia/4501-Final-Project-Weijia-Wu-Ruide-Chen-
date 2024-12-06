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
