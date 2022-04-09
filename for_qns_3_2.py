

from google.cloud import bigquery
from google.oauth2.service_account import Credentials
import pandas as pd
pd.set_option("display.max_rows", None, "display.max_columns", None)

# Construct a BigQuery client object.
client = bigquery.Client()

# TODO(developer): Set table_id to the ID of the destination table.
table_id = "my-project-ling-yang-feng.for_qns_2.for_qns_1"

job_config = bigquery.QueryJobConfig(destination = table_id)

sql = """
        select a.*
        from `my-project-ling-yang-feng.geo_international_ports.world_port_index_copy` as a
"""

query_output_table = client.query(sql).to_dataframe()
# query_output_table_2 = query_output_table.iloc[0:2]

query_output_table['distance_away'] = 0.5 * (abs(abs(32.610982) - abs(query_output_table.port_latitude)) * abs(abs(-38.706256) - abs(query_output_table.port_longitude)))
query_output_table_2 = query_output_table.loc[(query_output_table.provisions == True) & (query_output_table.water == True) & (query_output_table.fuel_oil == True) & (query_output_table.diesel == True)].sort_values(by='distance_away', ascending = True).head(1)


query_output_table_3 = query_output_table_2[['port_name', 'country', 'port_latitude', 'port_longitude']]
# query_output_table_3 = query_output_table_2.iloc[0, [2,3,4,5]]
# query_output_table_3 = query_output_table_2.reset_index(drop=True)
# query_output_table_4 = query_output_table_3.loc[0, ['port_name', 'country', 'port_latitude', 'port_longitude']]
print(query_output_table_3)


# # Define target table in BQ
target_table = "for_assessment.for_qns_3_2"
project_id = "my-project-ling-yang-feng"
credential_file = "//home/lingyangfeng/bigquery-demo/my-project-ling-yang-feng-170b6b93adf9.json"
credential = Credentials.from_service_account_file(credential_file)
# Location for BQ job, it needs to match with destination table location
job_location = "US"

# Save Pandas dataframe to BQ
query_output_table_3.to_gbq(target_table, project_id=project_id, if_exists='replace',
          location=job_location, progress_bar=True, credentials=credential)



# Start the query, passing in the extra configuration.
# query_job = client.query(sql, job_config=job_config)  # Make an API request.
# query_job.result()  # Wait for the job to complete.

# print("Query results loaded to the table {}".format(table_id))