import asyncio
import aioboto3
import loggingfrom botocore.config import Config
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

s3_config = Config(
    s3={
        'use_accelerate_endpoint': True,  
        'payload_signing_enabled': False,  
        'addressing_style': 'virtual'  
    retries={
        'max_attempts': 10,
        'mode': 'adaptive' 
    },
    max_pool_connections=50, 
    region_name=None  
)

session = aioboto3.Session()

async def get_bucket_region(s3_client, bucket_name):
    try:
        response = await s3_client.get_bucket_location(Bucket=bucket_name)
        location = response['LocationConstraint']
        return 'us-east-1' if location is None else location
    except ClientError as e:
        logging.error(f"Error getting bucket region for {bucket_name}: {e}")
        return 'us-east-1'  # Default fallback

async def check_transfer_acceleration(s3_client, bucket_name):
    try:
        response = await s3_client.get_bucket_accelerate_configuration(Bucket=bucket_name)
        status = response.get('Status', 'Suspended')
        logging.info(f"Transfer Acceleration status for {bucket_name}: {status}")
        return status == 'Enabled'
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchConfiguration':
            logging.warning(f"Transfer Acceleration not configured for {bucket_name}")
        else:
            logging.error(f"Error checking acceleration for {bucket_name}: {e}")
        return False

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
async def copy_object(source_s3, dest_s3, key):
    try:
        obj = await source_s3.head_object(Bucket=source_bucket, Key=key)
        obj_size = obj['ContentLength']
        
        logging.debug(f"Copying {key} (size: {obj_size / (1024*1024):.2f} MB)")
        
        if obj_size > 5 * 1024 * 1024 * 1024:  # 5GB threshold for multipart
            logging.info(f"Using multipart copy for large file: {key}")
            mp = await dest_s3.create_multipart_upload(
                Bucket=destination_bucket, 
                Key=key,
                # Add metadata preservation and storage class optimization
                Metadata=obj.get('Metadata', {}),
                StorageClass='STANDARD'
            )
            parts = []
            part_size = 100 * 1024 * 1024  # 100MB parts
            
            for i in range(0, obj_size, part_size):
                end = min(i + part_size - 1, obj_size - 1)
                part_number = len(parts) + 1
                
                part = await dest_s3.upload_part_copy(
                    Bucket=destination_bucket,
                    Key=key,
                    PartNumber=part_number,
                    UploadId=mp['UploadId'],
                    CopySource={'Bucket': source_bucket, 'Key': key},
                    CopySourceRange=f'bytes={i}-{end}'
                )
                parts.append({
                    'PartNumber': part_number, 
                    'ETag': part['CopyPartResult']['ETag']
                })
                
            await dest_s3.complete_multipart_upload(
                Bucket=destination_bucket,
                Key=key,
                UploadId=mp['UploadId'],
                MultipartUpload={'Parts': parts}
            )
        else:
            await dest_s3.copy_object(
                CopySource={'Bucket': source_bucket, 'Key': key},
                Bucket=destination_bucket,
                Key=key,
                MetadataDirective='COPY',
                StorageClass='STANDARD'
            )
            
        logging.info(f"Successfully copied {key}")
        return True
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['NoSuchKey', 'InvalidObjectState']:
            logging.warning(f"Skipping {key}: {error_code}")
            return False
        logging.error(f"Error copying {key}: {e}")
        raise

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
async def delete_object(source_s3, key):
    try:
        await source_s3.delete_object(Bucket=source_bucket, Key=key)
        logging.info(f"Deleted {key} from source")
    except ClientError as e:
        logging.error(f"Error deleting {key}: {e}")
        raise

async def list_mp4_objects(source_s3):
    mp4_keys = []
    
    try:
        paginator = source_s3.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(
            Bucket=source_bucket, 
            Prefix=prefix,
            PaginationConfig={
                'PageSize': 1000,
                'MaxItems': None
            }
        )
        
        async for page in page_iterator:
            contents = page.get('Contents', [])
            logging.debug(f"Processing page with {len(contents)} objects")
            
            for obj in contents:
                if obj['Key'].endswith('.mp4'):
                    mp4_keys.append(obj['Key'])
                    
    except ClientError as e:
        logging.error(f"Error listing objects: {e}")
        raise
    
    return mp4_keys

async def move_object(source_s3, dest_s3, key):
    """Move object with proper error handling"""
    try:
        if await copy_object(source_s3, dest_s3, key):
            await delete_object(source_s3, key)
            return True
    except Exception as e:
        logging.error(f"Failed to move {key}: {e}")
        return False
    return False

async def main():
    logging.info("Starting S3 MP4 transfer with acceleration...")
    logging.info(f"Source: s3://{source_bucket}/{prefix}")
    logging.info(f"Destination: s3://{destination_bucket}/")
    
    async with session.client('s3') as default_s3:
        source_region = await get_bucket_region(default_s3, source_bucket)
        dest_region = await get_bucket_region(default_s3, destination_bucket)
        
    logging.info(f"Source bucket region: {source_region}")
    logging.info(f"Destination bucket region: {dest_region}")
    
    source_config = Config(
        s3={
            'use_accelerate_endpoint': True,
            'payload_signing_enabled': False,
            'addressing_style': 'virtual'
        },
        retries={'max_attempts': 10, 'mode': 'adaptive'},
        max_pool_connections=50,
        region_name=source_region
    )
    
    dest_config = Config(
        s3={
            'use_accelerate_endpoint': True,
            'payload_signing_enabled': False,
            'addressing_style': 'virtual'
        },
        retries={'max_attempts': 10, 'mode': 'adaptive'},
        max_pool_connections=50,
        region_name=dest_region
    )
    
    async with session.client('s3', config=source_config) as source_s3, \
               session.client('s3', config=dest_config) as dest_s3:
        
        source_accel = await check_transfer_acceleration(source_s3, source_bucket)
        dest_accel = await check_transfer_acceleration(dest_s3, destination_bucket)
        
        if not source_accel:
            logging.warning(f"Transfer Acceleration not enabled on source bucket: {source_bucket}")
        if not dest_accel:
            logging.warning(f"Transfer Acceleration not enabled on destination bucket: {destination_bucket}")
            
        mp4_keys = await list_mp4_objects(source_s3)
        logging.info(f"Found {len(mp4_keys)} .mp4 files to move.")
        
        if not mp4_keys:
            logging.info("No MP4 files found. Nothing to transfer.")
            return
        
        total_size = 0
        for key in mp4_keys[:10]:  # Sample first 10 files for size estimation
            try:
                obj = await source_s3.head_object(Bucket=source_bucket, Key=key)
                total_size += obj['ContentLength']
            except Exception as e:
                logging.debug(f"Could not get size for {key}: {e}")
        
        if total_size > 0:
            estimated_total = (total_size / min(len(mp4_keys), 10)) * len(mp4_keys)
            logging.info(f"Estimated total size: {estimated_total / (1024**3):.2f} GB")
        
        semaphore = asyncio.Semaphore(50)  # Limit concurrent operations
        
        async def transfer_with_semaphore(key):
            async with semaphore:
                return await move_object(source_s3, dest_s3, key)
        
        logging.info("Starting transfers...")
        tasks = [transfer_with_semaphore(key) for key in mp4_keys]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        successful = sum(1 for r in results if r is True)
        failed = len(results) - successful
        
        logging.info("=" * 50)
        logging.info("TRANSFER COMPLETE")
        logging.info("=" * 50)
        logging.info(f"Total files: {len(mp4_keys)}")
        logging.info(f"Successful: {successful}")
        logging.info(f"Failed: {failed}")
        
        if failed > 0:
            logging.warning(f"{failed} transfers failed. Check logs for details.")
        else:
            logging.info("All files moved successfully!")

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Transfer interrupted by user")
    except Exception as e:
        logging.error(f"Fatal error: {e}", exc_info=True)