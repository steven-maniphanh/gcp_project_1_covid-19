import pandas as pd
import datetime as dt

#Convert csv file into a Dataframe
df = pd.read_csv("/Users/steven/Documents/00_DATA LEARNING/006_GCP_Project/PROJECT1_COVID-19/0 - DATASETS/Dataset 2 - Ourworld/1 - Cleaned Data/cases_deaths_cleaned_v1.1.csv")
"""print(df.head())
print(df.describe())
print(df.info())
print(df.dtypes)"""

#Convert 'date' column into a 'date' object
df['date'] = pd.to_datetime(df['date']).dt.date
assert df['date'].dtype == 'object'

#Split by year the dataset
df_2020 = df[df['date'] <= dt.date(2020, 12, 31)]
df_2021 = df[(df['date'] >= dt.date(2021, 1, 1)) & (df['date'] <= dt.date(2021, 12, 31))]
df_2022 = df[(df['date'] >= dt.date(2022, 1, 1)) & (df['date'] <= dt.date(2022, 12, 31))]
df_2023 = df[(df['date'] >= dt.date(2023, 1, 1)) & (df['date'] <= dt.date(2023, 12, 31))]
df_2024 = df[(df['date'] >= dt.date(2024, 1, 1)) & (df['date'] <= dt.date(2024, 12, 31))]
df_2025 = df[(df['date'] >= dt.date(2025, 1, 1)) & (df['date'] <= dt.date(2025, 12, 31))]
print(f"{df_2022} \n {df_2021} \n  {df_2022} \n  {df_2023} \n  {df_2024} \n  {df_2025}")

#Export Daframes spliited by years into CSV files
df_2020.to_csv('/Users/steven/Documents/00_DATA LEARNING/006_GCP_Project/PROJECT1_COVID-19/0 - DATASETS/Dataset 2 - Ourworld/1 - Cleaned Data/By Year/cases_deaths_2020.csv', sep=',', index=False)
df_2021.to_csv('/Users/steven/Documents/00_DATA LEARNING/006_GCP_Project/PROJECT1_COVID-19/0 - DATASETS/Dataset 2 - Ourworld/1 - Cleaned Data/By Year/cases_deaths_2021.csv', sep=',', index=False)
df_2022.to_csv('/Users/steven/Documents/00_DATA LEARNING/006_GCP_Project/PROJECT1_COVID-19/0 - DATASETS/Dataset 2 - Ourworld/1 - Cleaned Data/By Year/cases_deaths_2022.csv', sep=',', index=False)
df_2023.to_csv('/Users/steven/Documents/00_DATA LEARNING/006_GCP_Project/PROJECT1_COVID-19/0 - DATASETS/Dataset 2 - Ourworld/1 - Cleaned Data/By Year/cases_deaths_2023.csv', sep=',', index=False)
df_2024.to_csv('/Users/steven/Documents/00_DATA LEARNING/006_GCP_Project/PROJECT1_COVID-19/0 - DATASETS/Dataset 2 - Ourworld/1 - Cleaned Data/By Year/cases_deaths_2024.csv', sep=',', index=False)
df_2025.to_csv('/Users/steven/Documents/00_DATA LEARNING/006_GCP_Project/PROJECT1_COVID-19/0 - DATASETS/Dataset 2 - Ourworld/1 - Cleaned Data/By Year/cases_deaths_2025.csv', sep=',', index=False) 