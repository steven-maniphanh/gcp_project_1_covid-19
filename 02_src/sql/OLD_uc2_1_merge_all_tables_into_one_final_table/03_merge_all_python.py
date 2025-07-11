from google.cloud import bigquery

def generate_and_run_merge_query():
    client = bigquery.Client()
    dataset_id = "dataset_2_covid19_our_world"
    project_id = "covid19-1-463112"

    # Get list of raw tables
    tables = client.list_tables(dataset=dataset_id)

    # Filter only tables starting with "raw_covid_"
    raw_tables = [table.table_id for table in tables if table.table_id.startswith("raw_covid_")]

    if not raw_tables:
        print("No tables found!")
        return

    # Generate the UNION ALL SQL
    union_sql = "\nUNION ALL\n".join([
        f"SELECT * FROM `{project_id}.{dataset_id}.{table}`"
        for table in raw_tables
    ])

    # Final query with partitioning and clustering
    final_sql = f"""
    CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.covid19_final`
    PARTITION BY DATE(date)
    CLUSTER BY country AS
    {union_sql}
    """

    print("Running query:")
    print(final_sql)

    # Run the query
    client.query(final_sql).result()