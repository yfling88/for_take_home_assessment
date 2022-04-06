

from google.cloud import bigquery

# Construct a BigQuery client object.
client = bigquery.Client()

# TODO(developer): Set table_id to the ID of the destination table.
table_id = "my-project-ling-yang-feng.for_qns_2.for_qns_3"

job_config = bigquery.QueryJobConfig(destination=table_id)

sql = """
    with pre_process as
    (
        select a.*, 0.5 * (abs(abs(32.610982) - abs(port_latitude)) * abs(abs(-38.706256) - abs(port_longitude))) as distance_away
        from `my-project-ling-yang-feng.geo_international_ports.world_port_index_copy` as a
    )
    select a.country, a.port_name, a.port_latitude, a.port_longitude
    from pre_process as a
    where a.provisions = true
    and a.water = true
    and a.fuel_oil = true
    and a.diesel = true
    order by a.distance_away asc
    ;
"""

# Start the query, passing in the extra configuration.
query_job = client.query(sql, job_config=job_config)  # Make an API request.
query_job.result()  # Wait for the job to complete.

print("Query results loaded to the table {}".format(table_id))