from typing import Dict, List, Optional, Union
from fastapi import FastAPI, HTTPException, Response, status
from pydantic import BaseModel as PydanticBaseModel, ConfigDict
import uuid
import json
import logging
from datetime import datetime

app = FastAPI(title="Minimal Apache Iceberg REST Catalog Server")

logger = logging.getLogger('uvicorn.error')
logger.setLevel(logging.DEBUG)

# In-memory storage
namespaces = {}
tables = {}
table_metadata_by_id = {}
transactions = {}

class IcebergBaseModel(PydanticBaseModel):
    model_config = ConfigDict(populate_by_name=True)

class Namespace(IcebergBaseModel):
    namespace: List[str]
    properties: Dict[str, str] = {}

class TableIdentifier(IcebergBaseModel):
    namespace: List[str]
    name: str

class TableMetadata(IcebergBaseModel):
    format_version: int = 1,
    identifier: TableIdentifier
    table_uuid: str = None
    location: str = None
    properties: Dict[str, str] = {}
    schema: Dict = {}
    partition_spec: List = []
    sort_orders: List = []
    current_schema_id: int = 0
    default_sort_order_id: int = 0
    last_sequence_number: int = 0
    current_snapshot_id: int = None
    snapshots: List = []
    snapshot_log: List = []
    metadata_log: List = []
    last_updated_ms: int = None
    schema_by_id: Dict = {}
    partition_spec_by_id: Dict = {}
    sort_order_by_id: Dict = {}

class NamespaceResponse(IcebergBaseModel):
    namespace: List[str]
    properties: Dict[str, str]

class NamespaceListResponse(IcebergBaseModel):
    namespaces: List[List[str]]

class TableListResponse(IcebergBaseModel):
    identifiers: List[TableIdentifier] = []

class CommitTransactionRequest(IcebergBaseModel):
    updates: List[Dict] = []
    requirements: List[Dict] = []

class CommitTransactionResponse(IcebergBaseModel):
    metadata: Dict

class CreateTransactionRequest(IcebergBaseModel):
    table_identifier: TableIdentifier

class CreateTransactionResponse(IcebergBaseModel):
    transaction_id: str

@app.get("/")
def read_root():
    return {"message": "Minimal Apache Iceberg REST Catalog Server is running"}

# Namespace operations
# =============================================================================
@app.post("/v1/namespaces", response_model=NamespaceResponse, status_code=status.HTTP_201_CREATED)
def create_namespace(namespace: Namespace):
    ns_key = ".".join(namespace.namespace)
    if ns_key in namespaces:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Namespace {ns_key} already exists"
        )
    namespaces[ns_key] = namespace.model_dump()
    return namespace

@app.get("/v1/namespaces", response_model=NamespaceListResponse)
def list_namespaces():
    return NamespaceListResponse(
        namespaces=[namespace["namespace"] for namespace in namespaces.values()]
    )

@app.get("/v1/namespaces/{namespace_path}", response_model=NamespaceResponse)
def get_namespace(namespace_path: str):
    if namespace_path not in namespaces:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Namespace {namespace_path} not found"
        )
    return NamespaceResponse(**namespaces[namespace_path])

@app.delete("/v1/namespaces/{namespace_path}", status_code=status.HTTP_204_NO_CONTENT)
def delete_namespace(namespace_path: str):
    if namespace_path not in namespaces:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Namespace {namespace_path} not found"
        )
    del namespaces[namespace_path]
    return Response(status_code=status.HTTP_204_NO_CONTENT)

# Table operations
# =============================================================================
class CreateTableRequest(IcebergBaseModel):
    name: str
    location: Optional[str] = None
    schema: Dict = {}
    partition_spec: List = []
    sort_orders: List = []
    properties: Dict[str, str] = {}

@app.post("/v1/namespaces/{namespace_path}/tables", status_code=status.HTTP_201_CREATED)
def create_table(namespace_path: str, request: CreateTableRequest):
    logger.debug(request)
    namespace_parts = namespace_path.split(".")

    # Check if namespace exists
    if namespace_path not in namespaces:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Namespace {namespace_path} not found"
        )

    # Generate a table identifier
    identifier = TableIdentifier(
        namespace=namespace_parts,
        name=request.name
    )

    # Create table key
    table_key = f"{namespace_path}.{request.name}"

    # Check if table already exists
    if table_key in tables:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Table {table_key} already exists"
        )

    # Create table metadata
    table_uuid = str(uuid.uuid4())
    current_time_ms = int(datetime.now().timestamp() * 1000)

    table_metadata = TableMetadata(
        format_version=1,
        identifier=identifier,
        table_uuid=table_uuid,
        location=request.location,
        properties=request.properties,
        schema=request.schema,
        partition_spec=request.partition_spec,
        sort_orders=request.sort_orders,
        last_updated_ms=current_time_ms,
        schema_by_id={},
        partition_spec_by_id={},
        sort_order_by_id={},
        snapshots=[],
        snapshot_log=[],
        metadata_log=[]
    )

    # Store the table
    tables[table_key] = table_metadata.model_dump()
    table_metadata_by_id[table_uuid] = table_metadata.model_dump()

    logger.debug(table_metadata)
    return table_metadata

@app.get("/v1/namespaces/{namespace_path}/tables", response_model=TableListResponse)
def list_tables(namespace_path: str):
    namespace_parts = namespace_path.split(".")

    if namespace_path not in namespaces:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Namespace {namespace_path} not found"
        )

    # Filter tables by namespace
    namespace_tables = []
    for key, table in tables.items():
        if ".".join(table["identifier"]["namespace"]) == namespace_path:
            namespace_tables.append(TableIdentifier(**table["identifier"]))

    return TableListResponse(identifiers=namespace_tables)

@app.get("/v1/namespaces/{namespace_path}/tables/{table_name}")
def get_table(namespace_path: str, table_name: str):
    table_key = f"{namespace_path}.{table_name}"

    if table_key not in tables:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Table {table_key} not found"
        )

    return tables[table_key]

@app.delete("/v1/namespaces/{namespace_path}/tables/{table_name}", status_code=status.HTTP_204_NO_CONTENT)
def delete_table(namespace_path: str, table_name: str):
    table_key = f"{namespace_path}.{table_name}"

    if table_key not in tables:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Table {table_key} not found"
        )

    # Get the table UUID
    table_uuid = tables[table_key]["table_uuid"]

    # Delete the table
    del tables[table_key]
    del table_metadata_by_id[table_uuid]

    return Response(status_code=status.HTTP_204_NO_CONTENT)

# Transaction operations
# =============================================================================
@app.post("/v1/transactions", response_model=CreateTransactionResponse)
def create_transaction(request: CreateTransactionRequest):
    # Generate a transaction ID
    transaction_id = str(uuid.uuid4())

    # Find the table
    namespace_path = ".".join(request.table_identifier.namespace)
    table_key = f"{namespace_path}.{request.table_identifier.name}"

    if table_key not in tables:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Table {table_key} not found"
        )

    # Store the transaction
    transactions[transaction_id] = {
        "table_key": table_key,
        "table_uuid": tables[table_key]["table_uuid"],
        "created_at": datetime.now().isoformat(),
        "status": "open"
    }

    return CreateTransactionResponse(transaction_id=transaction_id)

@app.post("/v1/transactions/{transaction_id}/commit", response_model=CommitTransactionResponse)
def commit_transaction(transaction_id: str, request: CommitTransactionRequest):
    if transaction_id not in transactions:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Transaction {transaction_id} not found"
        )

    transaction = transactions[transaction_id]

    if transaction["status"] != "open":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Transaction {transaction_id} is not open"
        )

    # Get the table
    table_key = transaction["table_key"]
    table = tables[table_key]

    # Apply updates
    for update in request.updates:
        # In a real implementation, this would handle different update types
        # For now, just update the last_updated_ms
        table["last_updated_ms"] = int(datetime.now().timestamp() * 1000)

    # Check requirements
    for requirement in request.requirements:
        # In a real implementation, this would validate requirements
        pass

    # Update the table
    tables[table_key] = table
    table_metadata_by_id[table["table_uuid"]] = table

    # Mark transaction as committed
    transaction["status"] = "committed"
    transactions[transaction_id] = transaction

    return CommitTransactionResponse(metadata=table)

@app.get("/v1/config")
def get_config():
    return {
        "defaults": {},
        "overrides": {}
    }

# Handlers for table-by-uuid endpoints
@app.get("/v1/tables/table/{table_uuid}")
def get_table_by_uuid(table_uuid: str):
    if table_uuid not in table_metadata_by_id:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Table with UUID {table_uuid} not found"
        )

    return table_metadata_by_id[table_uuid]

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="debug")
