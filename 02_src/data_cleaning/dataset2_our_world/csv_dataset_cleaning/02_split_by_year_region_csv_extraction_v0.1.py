#v0.1 = Semi-automated

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

#Variables definition
regions = ['Europe', 'Eastern Mediterranean', 'Americas', 'Western Pacific', 'South-East Asia', 'Africa']
region_labels = ['europe', 'eastern_mediterranean', 'americas ', 'western_pacific','south_east_asia ', 'africa']
years = [2020, 2021, 2022, 2023, 2024, 2025]

# Split by year and region using dictionary mapping
region_dfs = {} #Dictionnary that will store all the df_year_label

for year in years :
    for region_label, region, in zip(region_labels, regions):
        #Store Dataframes generated into the dictonnary 'region_dfs'
        #example of dataframe stored in dict: "df_2020_europe"
        region_dfs[f"df_{year}_{region_label}"] = df[df['region'] == region] #ATTENTION CODE PAS BON il s'appuie pas sur "df_year" généré précedemment --> Solution Voir version v0.2
        #Generate csv file for each dataframes
        region_dfs[f"df_{year}_{region_label}"].to_csv(f"/Users/steven/Documents/00_DATA LEARNING/006_GCP_Project/PROJECT1_COVID-19/0 - DATASETS/Dataset 2 - Ourworld/1 - Cleaned Data/By Continent & Year/cases_deaths_{year}_{region_label}.csv", sep=',', index=False)

print(region_dfs.keys())


"""
#Manually
df_2020_europe = df[df['region'] == 'Europe']
df_2020_eastern_mediterranean = df[df['region'] == 'Eastern Mediterranean']
df_2020_americas = df[df['region'] == 'Americas']
df_2020_western_pacific = df[df['region'] == 'Western Pacific']
df_2020_south_east_asia = df[df['region'] == 'South-East Asia']
df_2020_africa = df[df['region'] == 'Africa']"""

"""# Create a data
for key, df_region in region_dfs.items():
    print(f"📌 DataFrame: {key}")
    print(df_region.head())  
    print("\n" + "="*50 + "\n")"""