import pandas as pd
import matplotlib.pyplot as plt

def generate_visualizations(hdfs_url, file_path):
    # File paths (adjust to your local file paths)
    full_file_path = f'{hdfs_url}{file_path}'

    # Load data into a pandas DataFrame
    df = pd.read_parquet(full_file_path)

    # Display the first few rows of the DataFrame
    print(df.head())

    # Ensure that 'date', 'total_cases', and 'total_deaths' columns exist
    # If column names are different, adjust them accordingly
    df['date'] = pd.to_datetime(df['date'])  # Ensure 'date' column is in datetime format

    # Plot total cases and deaths over time
    plt.figure(figsize=(12, 6))
    plt.plot(df['date'], df['total_cases'], label='Total Cases', color='blue')
    plt.plot(df['date'], df['total_deaths'], label='Total Deaths', color='red')
    plt.xlabel('Date')
    plt.ylabel('Count')
    plt.title('COVID-19 Total Cases and Deaths Over Time')
    plt.legend()
    plt.show()

