### deploy.sh 
v0.1 : inputs hardcoded
v0.2 : inputs using dynamic variables

### gcs_to_bq_.py Script
v0.1 : prototype
v0.2 : script cleaned with no hard coded variables, using the deploy.sh data for environnemment varriable
v0.3 : "create_partionned_clustered_table" function added to script 
    --> Enabling Creation of BQ table with partitioning/clustering