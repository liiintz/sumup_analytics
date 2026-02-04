import dlt
import openpyxl
import os
from dotenv import load_dotenv

# Load .env file immediately
load_dotenv()

# Constants for paths
# Default to data/raw if not specified in env
RAW_DATA_PATH = os.environ.get("RAW_DATA_PATH", "data/raw") 

def load_excel(path):
    """Simple Excel reader returning list of dicts."""
    wb = openpyxl.load_workbook(path, data_only=True, read_only=True)
    try:
        data = list(wb.active.values)
        headers = data[0]
        return [dict(zip(headers, row)) for row in data[1:] if any(row)]
    finally:
        wb.close()

def run_pipeline():
    db_url = os.getenv("DESTINATION__SQLALCHEMY__CREDENTIALS")
    
    pipeline = dlt.pipeline(pipeline_name="sumup", destination="sqlalchemy", dataset_name="main")

    sources = {
        "stores": (os.path.join(RAW_DATA_PATH, "stores.xlsx"), [{"name": "id", "data_type": "bigint"}]),
        "devices": (os.path.join(RAW_DATA_PATH, "devices.xlsx"), [{"name": "id", "data_type": "bigint",}, {"name": "store_id", "data_type": "bigint"},{"name": "created_at", "data_type": "timestamp"},{"name": "type", "data_type": "bigint"}]),
        "transactions": (os.path.join(RAW_DATA_PATH, "transactions.xlsx"), [
            {"name": "id", "data_type": "bigint"},
            {"name": "device_id", "data_type": "bigint"},
            {"name": "product_sku", "data_type": "text"},
            {"name": "card_number", "data_type": "text"},
            {"name": "happened_at", "data_type": "timestamp"},
            {"name": "created_at", "data_type": "timestamp"}
        ])
    }

    for table, (file_path, hints) in sources.items():
        if os.path.exists(file_path):
            print(f"Loading {table} from {file_path}...")
            # Note: I use write_disposition="replace" here to ensure a clean state 
            # for every run of this take-home challenge. 
            # In a production environment, we would use an incremental loading strategy 
            # (e.g., merge or append) to handle large datasets efficiently depending on use case.
            pipeline.run(
                dlt.resource(load_excel(file_path), name=table, columns=hints),
                write_disposition="replace"
            )
        else:
            print(f"Warning: File not found at {file_path}")

    print("Pipeline finished!")

if __name__ == "__main__":
    run_pipeline()
