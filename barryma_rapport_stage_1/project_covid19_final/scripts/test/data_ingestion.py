import pandas as pd

def load_raw_data(file_path):
    """
    Load raw data from a CSV file.
    
    :param file_path: Path to the CSV file
    :return: DataFrame containing the data
    """
    return pd.read_csv(file_path)

def save_transformed_data(df, file_path):
    """
    Save the transformed data to a CSV file.
    
    :param df: DataFrame containing the transformed data
    :param file_path: Path to the CSV file
    """
    df.to_csv(file_path, index=False)

# Example usage
if __name__ == "__main__":
    raw_global_cases_path = "../data/raw/RAW_global_confirmed_cases.csv"
    transformed_global_cases_path = "../data/transformed/CONVENIENT_global_confirmed_cases.csv"

    df = load_raw_data(raw_global_cases_path)
    # Perform transformations (to be added)
    save_transformed_data(df, transformed_global_cases_path)
