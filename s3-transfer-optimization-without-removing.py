import asyncio
import aioboto3
import logging
from botocore.exceptions import ClientError
from tenacity import retry, stop_after_attempt, wait_exponential


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

source_bucket = 'myproduction-bucket'
destination_bucket = 'mytest-bucket'
prefix = 'store/'

session = aioboto3.Session()

async def get_bucket_region(s3_client, bucket_name):
    response = await s3_client.get_bucket_location(Bucket=bucket_name)
    location = response['LocationConstraint']
    return 'us-east-1' if location is None else location

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
async def copy_object(s3_client, key):
    try:
        obj = await s3_client.head_object(Bucket=source_bucket, Key=key)
        obj_size = obj['ContentLength']
        
        if obj_size > 5 * 1024 * 1024 * 1024:  # 5GB threshold
            mp = await s3_client.create_multipart_upload(Bucket=destination_bucket, Key=key)
            parts = []
            part_size = 100 * 1024 * 1024  # 100MB chunks
            for i in range(0, obj_size, part_size):
                end = min(i + part_size - 1, obj_size - 1)
                part = await s3_client.upload_part_copy(
                    Bucket=destination_bucket,
                    Key=key,
                    PartNumber=len(parts) + 1,
                    UploadId=mp['UploadId'],
                    CopySource={'Bucket': source_bucket, 'Key': key},
                    CopySourceRange=f'bytes={i}-{end}'
                )
                parts.append({'PartNumber': len(parts) + 1, 'ETag': part['CopyPartResult']['ETag']})
            await s3_client.complete_multipart_upload(
                Bucket=destination_bucket,
                Key=key,
                UploadId=mp['UploadId'],
                MultipartUpload={'Parts': parts}
            )
        else:
            await s3_client.copy_object(
                CopySource={'Bucket': source_bucket, 'Key': key},
                Bucket=destination_bucket,
                Key=key
            )
        logging.info(f"Successfully copied {key}")
        return True
    except ClientError as e:
        logging.error(f"Error copying {key}: {e}")
        raise

async def list_mp4_objects(s3_client):
    mp4_keys = []
    paginator = s3_client.get_paginator('list_objects_v2')
    async for page in paginator.paginate(Bucket=source_bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.mp4'):
                mp4_keys.append(obj['Key'])
    return mp4_keys

async def main():
    async with session.client('s3') as default_s3:
        source_region = await get_bucket_region(default_s3, source_bucket)
        dest_region = await get_bucket_region(default_s3, destination_bucket)

    if source_region != dest_region:
        logging.error(f"Source bucket is in {source_region}, destination is in {dest_region}. Skipping transfer.")
        return

    async with session.client('s3', region_name=source_region) as s3_client:
        mp4_keys = await list_mp4_objects(s3_client)
        logging.info(f"Found {len(mp4_keys)} .mp4 files to copy.")
        tasks = [copy_object(s3_client, key) for key in mp4_keys]
        await asyncio.gather(*tasks)
        logging.info("All files copied successfully.")

if __name__ == '__main__':
    asyncio.run(main())