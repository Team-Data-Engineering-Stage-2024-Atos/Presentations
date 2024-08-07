import pandas as pd
import plotly.express as px

def plot_us_cases(file_path):
    """
    Plot the US confirmed cases.
    
    :param file_path: Path to the CSV file
    """
    df = pd.read_csv(file_path)
    fig = px.scatter(df, x='State', y='Total_Cases', size='Max_7_day_avg', color='Min_7_day_avg', 
                     title='COVID-19 Cases by State in the US', 
                     hover_data=['Avg_7_day_avg', 'High_Case_Days', 'Stddev_7_day_avg'])
    fig.show()

# Example usage
if __name__ == "__main__":
    us_cases_path = "../data/transformed/CONVENIENT_us_confirmed_cases.csv"
    plot_us_cases(us_cases_path)
