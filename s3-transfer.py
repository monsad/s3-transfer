import boto3
from multiprocessing import Pool, cpu_count
import logging


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

source_bucket = 'myproduction-bucket'
destination_bucket = 'mytest-bucket'
prefix = 'store/'

s3 = boto3.client('s3')

def list_mp4_objects():

    mp4_keys = []
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=source_bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.mp4'):
                mp4_keys.append(obj['Key'])
    return mp4_keys

def copy_object(key):

    try:
        copy_source = {'Bucket': source_bucket, 'Key': key}
        s3.copy_object(CopySource=copy_source, Bucket=destination_bucket, Key=key)
        logging.info(f"Successfully copied {key}")
    except Exception as e:
        logging.error(f"Error copying {key}: {e}")

def main():
    mp4_keys = list_mp4_objects()
    logging.info(f"Found {len(mp4_keys)} .mp4 files to copy.")

    num_processes = cpu_count()
    logging.info(f"Using {num_processes} processes for copying.")

    with Pool(processes=num_processes) as pool:
        pool.map(copy_object, mp4_keys)

    logging.info("All files copied successfully.")

if __name__ == '__main__':
    main()