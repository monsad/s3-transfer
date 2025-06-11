import asyncio
import aioaioboto3
import logging
import time
import sys
from dataclasses import dataclass
from typing import List, Optional, Dict, Any
from botocore.exceptions import ClientError, NoCredentialsError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import signal

@dataclass
class Config:
    source_bucket: str = 'myproduction-bucket'
    destination_bucket: str = 'mytest-bucket'
    prefix: str = 'store/'
    max_concurrent: int = 50
    multipart_size: int = 100 * 1024 * 1024 
    retry_attempts: int = 3
    retry_wait: tuple = (2, 10) 
    batch_size: int = 1000
    progress_interval: int = 100

@dataclass
class Stats:
    total_files: int = 0
    completed: int = 0
    failed: int = 0
    bytes_transferred: int = 0
    start_time: float = 0
    failed_transfers: List[str] = None
    
    def __post_init__(self):
        self.start_time = time.time()
        self.failed_transfers = []
    
    @property
    def progress(self) -> float:
        return (self.completed / self.total_files * 100) if self.total_files > 0 else 0
    
    @property
    def transfer_rate(self) -> float:
        elapsed = time.time() - self.start_time
        return (self.bytes_transferred / (1024 * 1024)) / elapsed if elapsed > 0 else 0

class GracefulShutdown:
    def __init__(self):
        self.shutdown = False
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        logging.warning(f"Received signal {signum}. Initiating graceful shutdown...")
        self.shutdown = True

def setup_logging() -> logging.Logger:
    logger = logging.getLogger('s3_transfer')
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    for handler in [logging.StreamHandler(sys.stdout), logging.FileHandler('s3_transfer.log')]:
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    return logger

logger = setup_logging()
config = Config()
stats = Stats()
shutdown_handler = GracefulShutdown()

@retry(
    stop=stop_after_attempt(config.retry_attempts),
    wait=wait_exponential(multiplier=1, min=config.retry_wait[0], max=config.retry_wait[1]),
    retry=retry_if_exception_type((ClientError, ConnectionError))
)
async def get_object_info(s3_client, key: str) -> Optional[Dict[str, Any]]:
    try:
        response = await s3_client.head_object(Bucket=config.source_bucket, Key=key)
        return {
            'key': key,
            'size': response['ContentLength'],
            'storage_class': response.get('StorageClass', 'STANDARD'),
            'etag': response['ETag']
        }
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return None
        raise

@retry(
    stop=stop_after_attempt(config.retry_attempts),
    wait=wait_exponential(multiplier=1, min=config.retry_wait[0], max=config.retry_wait[1]),
    retry=retry_if_exception_type((ClientError, ConnectionError))
)
async def copy_object(s3_client, obj_info: Dict[str, Any]) -> bool:
    key = obj_info['key']
    size = obj_info['size']
    
    try:
        if size > config.multipart_size:
            mp_response = await s3_client.create_multipart_upload(
                Bucket=config.destination_bucket,
                Key=key,
                StorageClass='STANDARD'
            )
            upload_id = mp_response['UploadId']
            parts = []
            
            for part_number, start_byte in enumerate(range(0, size, config.multipart_size), 1):
                end_byte = min(start_byte + config.multipart_size - 1, size - 1)
                part_response = await s3_client.upload_part_copy(
                    Bucket=config.destination_bucket,
                    Key=key,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    CopySource={'Bucket': config.source_bucket, 'Key': key},
                    CopySourceRange=f'bytes={start_byte}-{end_byte}'
                )
                parts.append({
                    'PartNumber': part_number,
                    'ETag': part_response['CopyPartResult']['ETag']
                })
            
            await s3_client.complete_multipart_upload(
                Bucket=config.destination_bucket,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={'Parts': parts}
            )
        else:
            await s3_client.copy_object(
                CopySource={'Bucket': config.source_bucket, 'Key': key},
                Bucket=config.destination_bucket,
                Key=key,
                StorageClass='STANDARD'
            )
        return True
    except ClientError as e:
        if e.response['Error']['Code'] in ['NoSuchKey', 'InvalidObjectState']:
            return False
        raise

async def verify_copy(s3_client, key: str, original_size: int) -> bool:
    try:
        response = await s3_client.head_object(Bucket=config.destination_bucket, Key=key)
        return response['ContentLength'] == original_size
    except ClientError:
        return False

async def transfer_object(s3_client, obj_info: Dict[str, Any]) -> bool:
    key = obj_info['key']
    
    try:
        if not await copy_object(s3_client, obj_info):
            stats.failed += 1
            stats.failed_transfers.append(f"{key}: Copy failed")
            return False
        
        if not await verify_copy(s3_client, key, obj_info['size']):
            stats.failed += 1
            stats.failed_transfers.append(f"{key}: Size verification failed")
            return False
        
        stats.completed += 1
        stats.bytes_transferred += obj_info['size']
        
        if stats.completed % config.progress_interval == 0:
            logger.info(
                f"Progress: {stats.completed}/{stats.total_files} "
                f"({stats.progress:.1f}%) - {stats.transfer_rate:.2f} MB/s"
            )
        
        return True
    except Exception as e:
        logger.error(f"Failed to transfer {key}: {e}")
        stats.failed += 1
        stats.failed_transfers.append(f"{key}: {str(e)}")
        return False

async def list_objects(s3_client) -> List[Dict[str, Any]]:
    objects = []
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        async for page in paginator.paginate(
            Bucket=config.source_bucket,
            Prefix=config.prefix,
            PaginationConfig={'PageSize': config.batch_size}
        ):
            if shutdown_handler.shutdown:
                break
            
            mp4_keys = [obj['Key'] for obj in page.get('Contents', []) if obj['Key'].endswith('.mp4')]
            if mp4_keys:
                tasks = [get_object_info(s3_client, key) for key in mp4_keys]
                obj_infos = await asyncio.gather(*tasks, return_exceptions=True)
                
                for obj_info in obj_infos:
                    if isinstance(obj_info, dict) and obj_info:
                        if obj_info['storage_class'] in ['STANDARD', 'REDUCED_REDUNDANCY', 'STANDARD_IA']:
                            objects.append(obj_info)
        
        return objects
    except ClientError as e:
        logger.error(f"Error listing objects: {e}")
        raise

async def main():
    logger.info("Starting S3 transfer process...")
    session = aioaioboto3.Session()
    
    try:
        async with session.client('s3') as s3_client:
            objects = await list_objects(s3_client)
            
            if not objects:
                logger.warning("No files found to transfer")
                return
            
            stats.total_files = len(objects)
            total_size = sum(obj['size'] for obj in objects)
            logger.info(f"Found {stats.total_files} files to transfer ({total_size / (1024**3):.2f} GB)")
            
            semaphore = asyncio.Semaphore(config.max_concurrent)
            async def transfer_with_semaphore(obj_info):
                async with semaphore:
                    if shutdown_handler.shutdown:
                        return False
                    return await transfer_object(s3_client, obj_info)
            
            tasks = [transfer_with_semaphore(obj_info) for obj_info in objects]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            successful = sum(1 for r in results if r is True)
            failed = len(results) - successful
            
            logger.info("=" * 60)
            logger.info(f"Transfer complete: {successful} successful, {failed} failed")
            logger.info(f"Total data: {stats.bytes_transferred / (1024**3):.2f} GB")
            logger.info(f"Transfer rate: {stats.transfer_rate:.2f} MB/s")
            logger.info("=" * 60)
            
            if failed > 0:
                sys.exit(1)
                
    except NoCredentialsError:
        logger.error("AWS credentials not found")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Transfer interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)