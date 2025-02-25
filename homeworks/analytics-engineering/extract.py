import os
import gzip
import urllib.request
from google.cloud import storage
import shutil
import pandas as pd
from typing import List, Tuple
from concurrent.futures import ThreadPoolExecutor

green_dtype = {
    'VendorID': 'string',
    'lpep_pickup_datetime': 'string',
    'lpep_dropoff_datetime': 'string',
    'store_and_fwd_flag': 'string',
    'RatecodeID': 'string',
    'PULocationID': 'string',
    'DOLocationID': 'string',
    'passenger_count': 'string',
    'trip_distance': 'float64',
    'fare_amount': 'float64',
    'extra': 'float64',
    'mta_tax': 'float64',
    'tip_amount': 'float64',
    'tolls_amount': 'float64',
    'ehail_fee': 'float64',
    'improvement_surcharge': 'float64',
    'total_amount': 'float64',
    'payment_type': 'string',
    'trip_type': 'string',
    'congestion_surcharge': 'float64'
}

yellow_dtype = {
    'VendorID': 'string',
    'tpep_pickup_datetime': 'string',
    'tpep_dropoff_datetime': 'string',
    'passenger_count': 'string',
    'trip_distance': 'float64',
    'RatecodeID': 'string',
    'store_and_fwd_flag': 'string',
    'PULocationID': 'string',  
    'DOLocationID': 'string',  
    'payment_type': 'string',
    'fare_amount': 'float64',
    'extra': 'float64',
    'mta_tax': 'float64',
    'tip_amount': 'float64',
    'tolls_amount': 'float64',
    'improvement_surcharge': 'float64',
    'total_amount': 'float64',
    'congestion_surcharge': 'float64'
}

fhv_dtype = {
    'dispatching_base_num': 'string',
    'pickup_datetime': 'string',
    'dropOff_datetime': 'string',
    'PUlocationID': 'string',
    'DOlocationID': 'string',
    'SR_Flag': 'string',
    'Affiliated_base_number': 'string'
}

def download_and_extract(taxi_type, year_month, dtype):
    """Download and extract taxi data"""
    csv_filename = f"{taxi_type}_tripdata_{year_month}.csv"
    parquet_filename = f"{taxi_type}_tripdata_{year_month}.parquet"
    url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{taxi_type}/{csv_filename}.gz"
    
    print(f"Downloading {url}...")
    
    # Download and decompress in one step
    with urllib.request.urlopen(url) as response:
        with gzip.GzipFile(fileobj=response) as uncompressed:
            with open(csv_filename, 'wb') as outfile:
                shutil.copyfileobj(uncompressed, outfile)
    
    # Convert CSV to Parquet
    print(f"Converting {csv_filename} to parquet...")
    df = pd.read_csv(csv_filename, dtype=dtype)
    df.to_parquet(parquet_filename, engine='pyarrow')
    
    # Remove CSV file
    os.remove(csv_filename)
    
    return parquet_filename

def upload_to_gcs(bucket_name, source_file_name, timeout=300):
    """Upload file to GCS"""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(source_file_name)

    print(f"Uploading {source_file_name} to GCS bucket {bucket_name}...")
    blob.upload_from_filename(source_file_name, timeout=timeout)
    print(f"File {source_file_name} uploaded to gs://{bucket_name}/{source_file_name}")

def cleanup(filename):
    """Remove local file after upload"""
    if os.path.exists(filename):
        os.remove(filename)
        print(f"Removed local file: {filename}")

def process_month(taxi_type: str, year_month: str, bucket_name: str, dtype) -> Tuple[str, bool]:
    """Process a single month of taxi data"""
    try:
        # Download and extract
        filename = download_and_extract(taxi_type, year_month, dtype)
        
        # Upload to GCS
        upload_to_gcs(bucket_name, filename)
        
        # Cleanup
        cleanup(filename)
        
        return year_month, True
    except Exception as e:
        print(f"Error processing {year_month}: {e}")
        return year_month, False


def main():
    # Configuration
    BUCKET_NAME = "dezoomcamp_hw3_2025_riodpp"  # Replace with your GCS bucket name
    TAXI_TYPE = "fhv"  # or "green"
    MONTHS = [f"{i:02d}" for i in range(1, 13)]
    YEAR = "2019"  # Format: YYYY-MM
    MAX_WORKERS = 4 
    dtype = fhv_dtype

    year_months = [f"{YEAR}-{month}" for month in MONTHS]

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(process_month, TAXI_TYPE, year_month, BUCKET_NAME, dtype)
            for year_month in year_months
        ]

        # Process results as they complete
        for future in futures:
            year_month, success = future.result()
            status = "Successfully processed" if success else "Failed to process"
            print(f"{status} {year_month}")

    print("\nAll processing completed!")

if __name__ == "__main__":
    main()