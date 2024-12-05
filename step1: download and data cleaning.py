import re
import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
import numpy as np

# Define Cochran's sample size formula as a function
def cochran_sample_size(population_size, confidence_level=0.95, margin_of_error=0.05, p=0.5):
    """
    Calculate sample size using Cochran's formula.
    
    population_size: Total number of data points available (population size)
    confidence_level: Confidence level (default is 0.95)
    margin_of_error: Margin of error (default is 0.05)
    p: Estimated proportion of the population (default is 0.5)
    
    Returns: Sample size rounded up to the nearest integer
    """
    # Z-score for the given confidence level
    z_dict = {0.9: 1.645, 0.95: 1.96, 0.99: 2.576}
    z = z_dict.get(confidence_level, 1.96)
    
    # Cochran's formula to calculate sample size
    numerator = (z ** 2) * p * (1 - p)
    denominator = margin_of_error ** 2
    sample_size = numerator / denominator
    
    # Adjust sample size for finite population
    adjusted_sample_size = sample_size / (1 + ((sample_size - 1) / population_size))
    
    return int(np.ceil(adjusted_sample_size))

# Calculate sample size for Yellow Taxi and HVFHV datasets

# Define paths to Parquet files
yellow_taxi_path = os.path.join(os.path.expanduser('~'), 'Desktop', '4501_final_proj', 'nyc_taxi_data')
hvfhv_path = os.path.join(os.path.expanduser('~'), 'Desktop', '4501_final_proj', 'nyc_taxi_data')

for ride_type, path in [('Yellow Taxi', yellow_taxi_path), ('HVFHV', hvfhv_path)]:
    for file in os.listdir(path):
        if file.endswith('.parquet'):
            filepath = os.path.join(path, file)
            df = pd.read_parquet(filepath)
            population_size = len(df)
            sample_size = cochran_sample_size(population_size)
            sample_df = df.sample(n=sample_size, random_state=42)
            print(f"Sample size for {file} ({ride_type}): {sample_size}")

            # Save sampled data to a new file
            sampled_filepath = os.path.join(path, f"sampled_{file}")
            sample_df.to_parquet(sampled_filepath)
            print(f"Saved sampled data to {sampled_filepath}")

  weather_path = os.path.join(os.path.expanduser('~'), 'Desktop', '4501_final_proj', 'weather data')

# Iterate through each year's weather CSV file
for file in os.listdir(weather_path):
    if file.endswith('.csv'):
        filepath = os.path.join(weather_path, file)
        df = pd.read_csv(filepath)
        population_size = len(df)
        sample_size = cochran_sample_size(population_size)
        sample_df = df.sample(n=sample_size, random_state=42)
        print(f"Sample size for {file} (Weather Data): {sample_size}")

        # Save sampled data to a new file
        sampled_filepath = os.path.join(weather_path, f"sampled_{file}")
        sample_df.to_csv(sampled_filepath, index=False)
        print(f"Saved sampled data to {sampled_filepath}")
