#Version note v0.2 : Fully Automated

import pandas as pd
import datetime as dt

#Convert csv file into a Dataframe
df = pd.read_csv("/Users/steven/Documents/00_DATA LEARNING/006_GCP_Project/PROJECT1_COVID-19_LOCAL_GIT_REPO/data/dataset2_our_world/01_filtered/cases_deaths_filtered_v1.1.csv")

#1) Convert 'date' column into a 'date' object
df['date'] = pd.to_datetime(df['date']).dt.date
assert df['date'].dtype == 'object'

#2) Split by year the dataset
years = [2020, 2021, 2022, 2023, 2024, 2025]
year_dfs= {} 

for year in years :
    #Store Dataframes filtered by year into the year_dfs dictionnary
    #example of dataframe stored in dict: "df_2020"
    year_dfs[f"df_{year}"] = df[(df['date'] >= dt.date(year, 1, 1)) & (df['date'] <= dt.date(year, 12, 31))]
    #print(year_dfs[f"df_{year}"].head())
    year_dfs[f"df_{year}"].to_csv(f"/Users/steven/Documents/00_DATA LEARNING/006_GCP_Project/PROJECT1_COVID-19_LOCAL_GIT_REPO/data/dataset2_our_world/02_cleaned/split_by_year/cases_deaths_{year}.csv", sep=',', index=False)
print(year_dfs.keys())

#Manual Query extraction of the dictionnary 'year_dfs' using a key dataframe:"df_2020" with a specific 'Region' and a Boolean Mask
#mask = year_dfs["df_2020"]['region'] == 'Europe'
#print(mask)
#df_2020_europe = year_dfs["df_2020"][mask]
#print(df_2020_europe)

#3) Split by year and region using dictionary mapping
regions = ['Europe', 'Eastern Mediterranean', 'Americas', 'Western Pacific', 'South-East Asia', 'Africa']
region_labels = ['europe', 'eastern_mediterranean', 'americas', 'western_pacific','south_east_asia', 'africa']
region_dfs = {} #Dictionnary that will store all the df_year_label

for year in years :
    for region_label, region, in zip(region_labels, regions):
        #Store Dataframes generated into the dictonnary 'region_dfs'
        #example of dataframe stored in dict : df_2020_europe', 'df_2020_eastern_mediterranean', 'df_2020_americas', 'df_2020_western_pacific', 'df_2020_south_east_asia ', 'df_2020_africa',...
        mask = year_dfs[f"df_{year}"]['region'] == region
        region_dfs[f"df_{year}_{region_label}"] = year_dfs[f"df_{year}"][mask]
        #Generate csv file for each dataframes
        region_dfs[f"df_{year}_{region_label}"].to_csv(f"/Users/steven/Documents/00_DATA LEARNING/006_GCP_Project/PROJECT1_COVID-19_LOCAL_GIT_REPO/data/dataset2_our_world/02_cleaned/split_by_year_continent/cases_deaths_{year}_{region_label}.csv", sep=',', index=False)
print(region_dfs.keys())
