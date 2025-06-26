import pandas as pd

#Convert csv files into Dataframes
df1_data = pd.read_csv("/Users/steven/Documents/00_DATA LEARNING/006_GCP_Project/PROJECT1_COVID-19_LOCAL_GIT_REPO/data/dataset2_our_world/01_filtered/cases_deaths_filtered_v1.0.csv")
df2_coordinates = pd.read_csv("/Users/steven/Documents/00_DATA LEARNING/006_GCP_Project/PROJECT1_COVID-19_LOCAL_GIT_REPO/data/dataset0_transverse/02_cleaned/countries_coordinates_V1.1.csv")

print(df1_data.info())
print(df2_coordinates.info())

# Keep only 'id' and 'name' from df1, and 'id' and 'score' from df2
df_merged = pd.merge(df1_data[['country', 'date', 'new_cases', 'total_cases', 'new_deaths', 'total_deaths']],
                  df2_coordinates[['country_name', 'region']],
                  left_on='country',
                  right_on='country_name',
                  how='inner')

print(df_merged.info())
print(df_merged.head())
print(df_merged['region'].unique())

#print(df_merged["region"].isnull().any())

df_merged[['country', 'region', 'date', 'new_cases', 'total_cases', 'new_deaths', 'total_deaths' ]].to_csv('/Users/steven/Documents/00_DATA LEARNING/006_GCP_Project/PROJECT1_COVID-19_LOCAL_GIT_REPO/data/dataset2_our_world/01_filtered/cases_deaths_filtered_v1.1.csv', sep=',', index=False)