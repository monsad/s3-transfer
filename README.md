Repository included different files to transfer 3TB files between S3 AWS
Every file include in name added feature like example transfer between regions 
If you want a run file with acceleration, you have to first enable Transfer Acceleration on bucket
```
aws s3api put-bucket-accelerate-configuration \
    --bucket myproduction-bucket \
    --accelerate-configuration Status=Enabled
```

To run any file first install requirements
```
pip install -r requirements.txt
```

Add also AWS credentials
```
export AWS_ACCESS_KEY_ID=your_access_key_id
export AWS_SECRET_ACCESS_KEY=your_secret_access_key
export AWS_DEFAULT_REGION=us-east-1
```

This is not production ready setup. 
Not testes enough. I am not responsible for any changes to your AWS account.
