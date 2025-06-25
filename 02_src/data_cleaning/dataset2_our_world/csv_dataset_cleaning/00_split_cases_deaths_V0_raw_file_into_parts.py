import pandas as pd

#Import csv into a dataframe
df0 = pd.read_csv("/Users/steven/Documents/00_DATA LEARNING/006_GCP_Project/PROJECT1_COVID-19_LOCAL_GIT_REPO/01_data/dataset2_our_world/00_raw/cases_deaths_v0.csv")

#Split dataframe into 4 dataframes
df_part1 = df0.iloc[0:125001]
df_part2 = df0.iloc[125001:255001]
df_part3 = df0.iloc[255001:375001]
df_part4 = df0.iloc[375001:500001]

#Load dataframes into respective csv file
df_part1.to_csv('/Users/steven/Documents/00_DATA LEARNING/006_GCP_Project/PROJECT1_COVID-19_LOCAL_GIT_REPO/01_data/dataset2_our_world/00_raw/cases_deaths_v0_part1.csv', sep=',', index=False)
df_part2.to_csv('/Users/steven/Documents/00_DATA LEARNING/006_GCP_Project/PROJECT1_COVID-19_LOCAL_GIT_REPO/01_data/dataset2_our_world/00_raw/cases_deaths_v0_part2.csv', sep=',', index=False)
df_part3.to_csv('/Users/steven/Documents/00_DATA LEARNING/006_GCP_Project/PROJECT1_COVID-19_LOCAL_GIT_REPO/01_data/dataset2_our_world/00_raw/cases_deaths_v0_part3.csv', sep=',', index=False)
df_part4.to_csv('/Users/steven/Documents/00_DATA LEARNING/006_GCP_Project/PROJECT1_COVID-19_LOCAL_GIT_REPO/01_data/dataset2_our_world/00_raw/cases_deaths_v0_part4.csv', sep=',', index=False)
