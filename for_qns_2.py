

from google.cloud import bigquery

# Construct a BigQuery client object.
client = bigquery.Client()

# TODO(developer): Set table_id to the ID of the destination table.
table_id = "my-project-ling-yang-feng.for_qns_2.for_qns_2"

job_config = bigquery.QueryJobConfig(destination=table_id)

sql = """
    select country, count(*) as port_count
    from `my-project-ling-yang-feng.geo_international_ports.world_port_index_copy`
    where cargo_wharf = true
    group by country
    order by count(*) desc
    ;
"""

# Start the query, passing in the extra configuration.
query_job = client.query(sql, job_config=job_config)  # Make an API request.
query_job.result()  # Wait for the job to complete.

print("Query results loaded to the table {}".format(table_id))