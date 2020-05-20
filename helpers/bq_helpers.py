def create_BQ_dataset(client, name, description=""):

    # TODO(developer): Set dataset_id to the ID of the dataset to create.
    dataset_id = "{}.{}".format(client.project, name)

    # Construct a full Dataset object to send to the API.
    dataset = bigquery.Dataset(dataset_id)
    dataset.description = "Metadata in support of ETL process"
    
    # TODO(developer): Specify the geographic location where the dataset should reside.
    dataset.location = "US"

    # Send the dataset to the API for creation.
    # Raises google.api_core.exceptions.Conflict if the Dataset already
    # exists within the project.
    dataset = client.create_dataset(dataset)  # Make an API request.
    
    return dataset

# dataset = create_BQ_dataset(BQ_client, 'etl_metadata','Metadata in support of ETL process')
# print("Created dataset {}.{} with description {}".format(client.project, dataset.dataset_id, 
#                                                         dataset.description))

def delete_BQ_dataset(BQ_client, name):

    dataset_id = '{}.{}'.format(client.project, name)

    # Use the delete_contents parameter to delete a dataset and its contents.
    # Use the not_found_ok parameter to not receive an error if the dataset has already been deleted.
    client.delete_dataset(
        dataset_id, delete_contents=True, not_found_ok=True
    )  

# delete_BQ_dataset(BQ_client, 'etl_metadata')
# print("Deleted dataset '{}'.".format(dataset_id))


def BQ_table_exists(BQ_client, dataset, table):
    table_id = "{}.{}.{}".format(client.project, dataset, table)

    try:
        client.get_table(table_id)  # Make an API request.
        return True
    except NotFound:
        return False

# if BQ_table_exists(BQ_client, 'etl_metadata', 'etl_metadata'):
#     print("Table already exists.")
# else:
#     print("Table is not found.")


def create_BQ_table(BQ_client, dataset, table, schema):
    table_id = "{}.{}.{}".format(client.project, dataset, table)
    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table)  # Make an API request.
    
    return table

# table = create_BQ_table(BQ_client,'etl_metadata', 'etl_metadata', schema)
# print(
#     "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
# )

# if BQ_table_exists(BQ_client, 'etl_metadata', 'etl_metadata'):
#     print("Table already exists.")
# else:
#     print("Table is not found.")
    


def delete_BQ_Table(BQ_client, dataset, table):
    table_id = "{}.{}.{}".format(client.project, dataset, table)

    # If the table does not exist, delete_table raises
    # google.api_core.exceptions.NotFound unless not_found_ok is True.
    client.delete_table(table_id, not_found_ok=True)  # Make an API request.
def load_BQ_from_json(BQ_client, dataset, table, json_rows, aschema):
    table_id = "{}.{}.{}".format(client.project, dataset, table)

    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    job_config.write_disposition = 'WRITE_APPEND'
    job_config.schema = aschema
    
    data = io.StringIO(json_rows)
    #     print(json_rows)
    job = client.load_table_from_file(data, table_id, job_config=job_config)

    try:
        result = job.result()
    except BadRequest as ex:

        for err in ex.errors:
            print(err)
        return job

#     table = client.get_table(table_id)  # Make an API request.
    
    return table
