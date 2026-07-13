
import boto3
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm



# Set up your R2 credentials
R2_ACCESS_KEY_ID = 'ac976e3fd83503231b5e86c4d5cb1d8a'
R2_SECRET_ACCESS_KEY = '2eaa933e8390d67850165ff401676fc389078cb3beff39f90d254e37650f4b64'
R2_BUCKET_NAME = 'dubustorage'
R2_ENDPOINT_URL = 'https://6e4393074f2f84c090c5043ce756a217.r2.cloudflarestorage.com'



# Create S3 client
s3 = boto3.client(
    's3',
    aws_access_key_id=R2_ACCESS_KEY_ID,
    aws_secret_access_key=R2_SECRET_ACCESS_KEY,
    endpoint_url=R2_ENDPOINT_URL
)


def download_specific_file_from_r2(remote_folder_path, filename, local_download_dir='.'):
    """
    Download a specific file from a folder in R2 bucket
    
    Args:
        remote_folder_path: Folder path in R2 bucket (e.g., 'dataset/copy2')
        filename: Name of the file to download (e.g., 'myfile.txt' or 'subfolder/myfile.txt')
        local_download_dir: Local directory where to download (default: current directory)
    
    Returns:
        bool: True if successful, False otherwise
    """
    
    # Ensure remote_folder_path doesn't start with '/' and format properly
    remote_folder_path = remote_folder_path.strip('/')
    
    # Construct the full key in the bucket
    bucket_key = f"{remote_folder_path}/{filename.lstrip('/')}"
    
    # Create local path
    local_path = os.path.join(local_download_dir, filename)
    
    print(f"🔍 Attempting to download: {bucket_key}")
    
    try:
        # Check if the file exists
        s3.head_object(Bucket=R2_BUCKET_NAME, Key=bucket_key)
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(local_path) if os.path.dirname(local_path) else '.', exist_ok=True)
        
        # Download the file
        s3.download_file(R2_BUCKET_NAME, bucket_key, local_path)
        
        print(f"✅ Successfully downloaded: {filename}")
        print(f"📂 Saved to: {local_path}")
        return True
        
    except s3.exceptions.NoSuchKey:
        print(f"❌ File not found: {bucket_key}")
        return False
    except Exception as e:
        print(f"❌ Error downloading {filename}: {str(e)}")
        return False


# Example usage
if __name__ == "__main__":
    # Download a specific file from 'dataset/copy2' folder
    download_specific_file_from_r2(
        remote_folder_path='raw_gui',
        filename='1000x.zip',  # Replace with your actual filename
        local_download_dir='./'
    )
    
