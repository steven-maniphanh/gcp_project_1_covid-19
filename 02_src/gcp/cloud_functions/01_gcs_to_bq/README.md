### deploy.sh

v0.1 : inputs hardcoded
v0.2 : inputs using dynamic variables

v0.3 : Minor update

v0.4 : Migration to new GCP account and to project "[covid19-dbt-analytics-2]()"

### gcs_to_bq_.py Script

v0.1 : prototype
v0.2 : script cleaned with no hard coded variables, using the deploy.sh data for environnemment varriable
v0.3 : "create_partionned_clustered_table" function added to script --> Enabling Creation of BQ table with partitioning/clustering

v0_4 :

| Feature                   | V0.3               | V0.4 ✅ (Improved)                    |
| ------------------------- | ------------------ | ------------------------------------- |
| Table creation            | Implicit           | Explicit with partitioning/clustering |
| Schema enforcement        | Basic / implicit   | Explicit `SchemaField`definition    |
| Partitioning / Clustering | ❌ Not implemented | ✅ Implemented                        |
| Column validation         | ❌ No              | ✅ Missing columns check              |
| Ingestion timestamp       | ❌ No              | ✅ Added                              |
| Metadata logging          | Basic              | ✅ Enriched                           |
| Error logging             | Minimal            | ✅ Context-rich                       |
