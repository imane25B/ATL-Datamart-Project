import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from minio import Minio
import io

def get_minio_client():
    """Create and return a MinIO client instance"""
    return Minio(
        "localhost:9000",
        access_key="minio",
        secret_key="minio123",
        secure=False
    )

def read_parquet_from_minio(client, bucket_name, object_name):
    """Read a parquet file from MinIO into a pandas DataFrame"""
    try:
        # Get the object data
        data = client.get_object(bucket_name, object_name).read()
        # Create a buffer from the data
        buffer = io.BytesIO(data)
        # Read the parquet file using pandas
        df = pd.read_parquet(buffer, engine='pyarrow')
        # Convert column names to lowercase
        df.columns = df.columns.str.lower()
        return df
    except Exception as e:
        print(f"Error reading parquet file {object_name}: {e}")
        return None

def main():
    # MinIO configuration
    bucket_name = "newyork-data-bucket"
    parquet_files = [
        "yellow_tripdata_2023-01.parquet",
        "yellow_tripdata_2023-02.parquet"
    ]
    
    # Create MinIO client
    minio_client = get_minio_client()
    
    # Create PostgreSQL engine
    db_engine = create_engine(
        "postgresql://postgres:admin@localhost:15432/nyc_warehouse"
    )
    
    try:
        # Process each parquet file
        for i, parquet_file in enumerate(parquet_files):
            print(f"Processing {parquet_file}...")
            
            # Read parquet file from MinIO
            df = read_parquet_from_minio(minio_client, bucket_name, parquet_file)
            
            if df is not None:
                # Write to PostgreSQL
                df.to_sql(
                    name="nyc_raw",
                    con=db_engine,
                    if_exists="append" if i > 0 else "replace",
                    index=False,
                    method='multi',
                    chunksize=10000
                )
                print(f"Successfully processed {parquet_file}")
            
    except Exception as e:
        print(f"Error during processing: {e}")
    finally:
        db_engine.dispose()

if __name__ == "__main__":
    main()