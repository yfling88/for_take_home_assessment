

from google.cloud import bigquery
from google.oauth2.service_account import Credentials
import pandas as pd
import math
import numpy
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

# query_output_table = query_output_table.assign(port_latitude_pointer = query_output_table.loc[query_output_table.port_name == 'JURONG ISLAND'].port_latitude)
# query_output_table = query_output_table.assign(port_longitude_pointer = query_output_table.loc[query_output_table.port_name == 'JURONG ISLAND'].port_longitude)

# print(query_output_table['port_latitude_pointer'])
# print(query_output_table['port_longitude_pointer'])

query_output_table['port_latitude_pointer'] = 1.283333
query_output_table['port_longitude_pointer'] = 103.733333

# print(query_output_table.loc[query_output_table.port_name == 'JURONG ISLAND'].port_latitude.values)
# print(query_output_table.loc[query_output_table.port_name == 'JURONG ISLAND'].port_longitude.values)
# print(query_output_table.head(1))


query_output_table['R'] = 6371000 # metres
query_output_table['q1'] = query_output_table.port_latitude_pointer * 3.142/180 # φ, λ in radians
query_output_table['q2'] = query_output_table.port_latitude * 3.142/180
query_output_table['beta'] = (query_output_table.port_latitude- query_output_table.port_latitude_pointer) * 3.142/180
query_output_table['lda'] = (query_output_table.port_longitude- query_output_table.port_longitude_pointer) * 3.142/180


query_output_table['a'] = (numpy.sin(query_output_table.beta/2) * numpy.sin(query_output_table.beta/2) + numpy.cos(query_output_table.q1) * numpy.cos(query_output_table.q2) * numpy.sin(query_output_table.lda/2) * numpy.sin(query_output_table.lda/2))
query_output_table['c'] = 2 * numpy.arctan2(numpy.sqrt(query_output_table.a), numpy.sqrt(1-query_output_table.a))

query_output_table['distance_in_meters'] = query_output_table.R * query_output_table.c # in metres

query_output_table_2 = query_output_table.sort_values(by='distance_in_meters', ascending = True).head(6)
query_output_table_3 = query_output_table_2[['port_name', 'distance_in_meters']]
print(query_output_table_3)

# # Define target table in BQ
target_table = "for_assessment.for_qns_1_2"
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