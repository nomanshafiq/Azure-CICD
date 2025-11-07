import logging
import azure.functions as func
from azure.data.tables import TableServiceClient, UpdateMode
from azure.core.exceptions import ResourceNotFoundError
import os
import json

def get_table_client():
    """
    Connect to the Cosmos DB Table API using the connection string
    stored in environment variables.
    """
    connection_string = os.environ.get("COSMOS_CONNECTION_STRING")
    table_name = os.environ.get("COSMOS_TABLE_NAME", "visitorcounter")

    if not connection_string:
        raise ValueError("Missing COSMOS_CONNECTION_STRING in Function App settings")

    service = TableServiceClient.from_connection_string(conn_str=connection_string)
    return service.get_table_client(table_name=table_name)

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("VisitorCounter function started processing request...")

    partition_key = "1"
    row_key = "1"

    try:
        table_client = get_table_client()

        try:
            # ✅ Try to get existing entity
            entity = table_client.get_entity(partition_key=partition_key, row_key=row_key)
            # Increment the counter
            entity["counterId"] = int(entity.get("counterId", 0)) + 1
            table_client.update_entity(entity, mode=UpdateMode.MERGE)
            logging.info(f"Updated counter: {entity['counterId']}")
        except ResourceNotFoundError:
            # ✅ Entity not found — create it
            entity = {
                "PartitionKey": partition_key,
                "RowKey": row_key,
                "counterId": 1
            }
            table_client.create_entity(entity)
            logging.info("Created new visitor counter record.")

        # ✅ Return JSON response
        return func.HttpResponse(
            json.dumps({"visitor_count": entity["counterId"]}),
            mimetype="application/json",
            status_code=200
        )

    except Exception as e:
        logging.error(f"Error processing visitor counter: {e}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            mimetype="application/json",
            status_code=500
        )
