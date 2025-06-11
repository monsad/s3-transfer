import asyncio
import aioboto3
import logging
import time
import sys
from dataclasses import dataclass
from typing import List, Optional, Dict, Any, Set
from botocore.exceptions import ClientError, NoCredentialsError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from concurrent.futures import ThreadPoolExecutor

@dataclass
class S3Config:
    source_bucket: str = 'myproduction-bucket'
    destination_bucket: str = 'mytest-bucket'
    prefix: str = 'store/'
    max_concurrent: int = 50
    multipart_size: int = 100 * 1024 * 1024
    retry_attempts: int = 3
    retry_wait: tuple = (2, 10)
    batch_size: int = 1000
    progress_interval: int = 100
    storage_classes: Set[str] = frozenset(['STANDARD', 'REDUCED_REDUNDANCY', 'STANDARD_IA'])

@dataclass
class TransferMetrics:
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
    
    @property
    def elapsed_time(self) -> float:
        return time.time() - self.start_time

class S3Transfer:
    def __init__(self):
        self.config = S3Config()
        self.metrics = TransferMetrics()
        self._setup_logging()
        self._executor = ThreadPoolExecutor(max_workers=4)
    
    def _setup_logging(self):
        self.logger = logging.getLogger('s3_transfer')
        self.logger.setLevel(logging.INFO)
        self.logger.handlers.clear()
        
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        for handler in [logging.StreamHandler(sys.stdout), logging.FileHandler('s3_transfer.log')]:
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
    
    @retry(
        stop=stop_after_attempt(S3Config.retry_attempts),
        wait=wait_exponential(multiplier=1, min=S3Config.retry_wait[0], max=S3Config.retry_wait[1]),
        retry=retry_if_exception_type((ClientError, ConnectionError))
    )
    async def _get_object_info(self, s3_client, key: str) -> Optional[Dict[str, Any]]:
        try:
            response = await s3_client.head_object(Bucket=self.config.source_bucket, Key=key)
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
        stop=stop_after_attempt(S3Config.retry_attempts),
        wait=wait_exponential(multiplier=1, min=S3Config.retry_wait[0], max=S3Config.retry_wait[1]),
        retry=retry_if_exception_type((ClientError, ConnectionError))
    )
    async def _copy_object(self, s3_client, obj_info: Dict[str, Any]) -> bool:
        key = obj_info['key']
        size = obj_info['size']
        
        try:
            if size > self.config.multipart_size:
                mp_response = await s3_client.create_multipart_upload(
                    Bucket=self.config.destination_bucket,
                    Key=key,
                    StorageClass='STANDARD'
                )
                upload_id = mp_response['UploadId']
                parts = []
                
                for part_number, start_byte in enumerate(range(0, size, self.config.multipart_size), 1):
                    end_byte = min(start_byte + self.config.multipart_size - 1, size - 1)
                    part_response = await s3_client.upload_part_copy(
                        Bucket=self.config.destination_bucket,
                        Key=key,
                        PartNumber=part_number,
                        UploadId=upload_id,
                        CopySource={'Bucket': self.config.source_bucket, 'Key': key},
                        CopySourceRange=f'bytes={start_byte}-{end_byte}'
                    )
                    parts.append({
                        'PartNumber': part_number,
                        'ETag': part_response['CopyPartResult']['ETag']
                    })
                
                await s3_client.complete_multipart_upload(
                    Bucket=self.config.destination_bucket,
                    Key=key,
                    UploadId=upload_id,
                    MultipartUpload={'Parts': parts}
                )
            else:
                await s3_client.copy_object(
                    CopySource={'Bucket': self.config.source_bucket, 'Key': key},
                    Bucket=self.config.destination_bucket,
                    Key=key,
                    StorageClass='STANDARD'
                )
            return True
        except ClientError as e:
            if e.response['Error']['Code'] in ['NoSuchKey', 'InvalidObjectState']:
                return False
            raise
    
    async def _verify_copy(self, s3_client, key: str, original_size: int) -> bool:
        try:
            response = await s3_client.head_object(Bucket=self.config.destination_bucket, Key=key)
            return response['ContentLength'] == original_size
        except ClientError:
            return False
    
    async def _transfer_object(self, s3_client, obj_info: Dict[str, Any]) -> bool:
        key = obj_info['key']
        
        try:
            if not await self._copy_object(s3_client, obj_info):
                self.metrics.failed += 1
                self.metrics.failed_transfers.append(f"{key}: Copy failed")
                return False
            
            if not await self._verify_copy(s3_client, key, obj_info['size']):
                self.metrics.failed += 1
                self.metrics.failed_transfers.append(f"{key}: Size verification failed")
                return False
            
            self.metrics.completed += 1
            self.metrics.bytes_transferred += obj_info['size']
            
            if self.metrics.completed % self.config.progress_interval == 0:
                self.logger.info(
                    f"Progress: {self.metrics.completed}/{self.metrics.total_files} "
                    f"({self.metrics.progress:.1f}%) - {self.metrics.transfer_rate:.2f} MB/s"
                )
            
            return True
        except Exception as e:
            self.logger.error(f"Failed to transfer {key}: {e}")
            self.metrics.failed += 1
            self.metrics.failed_transfers.append(f"{key}: {str(e)}")
            return False
    
    async def _list_objects(self, s3_client) -> List[Dict[str, Any]]:
        objects = []
        try:
            paginator = s3_client.get_paginator('list_objects_v2')
            async for page in paginator.paginate(
                Bucket=self.config.source_bucket,
                Prefix=self.config.prefix,
                PaginationConfig={'PageSize': self.config.batch_size}
            ):
                mp4_keys = [obj['Key'] for obj in page.get('Contents', []) if obj['Key'].endswith('.mp4')]
                if mp4_keys:
                    tasks = [self._get_object_info(s3_client, key) for key in mp4_keys]
                    obj_infos = await asyncio.gather(*tasks, return_exceptions=True)
                    
                    for obj_info in obj_infos:
                        if isinstance(obj_info, dict) and obj_info:
                            if obj_info['storage_class'] in self.config.storage_classes:
                                objects.append(obj_info)
            
            return objects
        except ClientError as e:
            self.logger.error(f"Error listing objects: {e}")
            raise
    
    def _print_summary(self):
        self.logger.info("=" * 60)
        self.logger.info("Transfer Summary:")
        self.logger.info(f"Total files: {self.metrics.total_files}")
        self.logger.info(f"Successful: {self.metrics.completed}")
        self.logger.info(f"Failed: {self.metrics.failed}")
        self.logger.info(f"Total data: {self.metrics.bytes_transferred / (1024**3):.2f} GB")
        self.logger.info(f"Transfer rate: {self.metrics.transfer_rate:.2f} MB/s")
        self.logger.info(f"Duration: {self.metrics.elapsed_time / 60:.1f} minutes")
        self.logger.info("=" * 60)
    
    async def run(self):
        self.logger.info("Starting S3 transfer process...")
        session = aioboto3.Session()
        
        try:
            async with session.client('s3') as s3_client:
                objects = await self._list_objects(s3_client)
                
                if not objects:
                    self.logger.warning("No files found to transfer")
                    return
                
                self.metrics.total_files = len(objects)
                total_size = sum(obj['size'] for obj in objects)
                self.logger.info(f"Found {self.metrics.total_files} files to transfer ({total_size / (1024**3):.2f} GB)")
                
                semaphore = asyncio.Semaphore(self.config.max_concurrent)
                async def transfer_with_semaphore(obj_info):
                    async with semaphore:
                        return await self._transfer_object(s3_client, obj_info)
                
                tasks = [transfer_with_semaphore(obj_info) for obj_info in objects]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                successful = sum(1 for r in results if r is True)
                failed = len(results) - successful
                
                self._print_summary()
                
                if failed > 0:
                    sys.exit(1)
                    
        except NoCredentialsError:
            self.logger.error("AWS credentials not found")
            sys.exit(1)
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}", exc_info=True)
            sys.exit(1)
        finally:
            self._executor.shutdown(wait=True)

def main():
    try:
        transfer = S3Transfer()
        asyncio.run(transfer.run())
    except KeyboardInterrupt:
        logging.info("Transfer interrupted by user")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)

if __name__ == '__main__':
    main() 