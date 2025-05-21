import os
from etls.aws_etl import connect_to_s3, create_bucket_if_not_exists, upload_file_to_s3
from utils.constants import AWS_BUCKET_NAME, AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

def upload_s3_pipeline(ti):
    """
    Upload Reddit data to S3 bucket
    
    Args:
        ti: TaskInstance object from Airflow
        
    Returns:
        str: Status message
    """
    try:
        # Get the file path from the previous task
        file_path = ti.xcom_pull(task_ids='extract_reddit_data')
        
        if not file_path or not os.path.exists(file_path):
            raise Exception(f"File not found: {file_path}")
            
        print(f"Uploading file {file_path} to S3...")
        
        # Connect to S3
        s3 = connect_to_s3()
        
        # Create bucket if it doesn't exist
        create_bucket_if_not_exists(s3, AWS_BUCKET_NAME)
        
        # Get the file name from the path
        file_name = os.path.basename(file_path)
        
        # Upload the file
        upload_file_to_s3(s3, file_path, AWS_BUCKET_NAME, file_name)
        
        return f"Successfully uploaded {file_name} to S3 bucket {AWS_BUCKET_NAME}"
        
    except Exception as e:
        error_message = f"Error in S3 upload pipeline: {str(e)}"
        print(error_message)
        raise Exception(error_message)

    
