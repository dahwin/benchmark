
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

# Download a single file from R2
def download_file_from_r2(bucket_key, local_path):
    try:
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        s3.download_file(R2_BUCKET_NAME, bucket_key, local_path)
        return bucket_key, True, None
    except Exception as e:
        return bucket_key, False, str(e)

# Download folder from R2 with concurrent downloads
def download_folder_from_r2(remote_folder_path, local_download_dir='.', workers=10):
    """
    Download a folder from R2 bucket with proper pagination support

    Args:
        remote_folder_path: Path in R2 bucket (e.g., 'dataset/Full_Videos/10_20k/Videos/')
        local_download_dir: Local directory where to download (default: current directory)
        workers: Number of concurrent downloads
    """

    # Ensure remote_folder_path doesn't start with '/' but ends with '/'
    remote_folder_path = remote_folder_path.strip('/') + '/'

    print(f"🔍 Listing files in: {remote_folder_path}")

    try:
        files_to_download = []
        
        # Get the original folder name from remote path
        folder_parts = remote_folder_path.strip('/').split('/')
        original_folder_name = folder_parts[-1]

        # ⭐ FIX: Use paginator instead of single list_objects_v2 call
        paginator = s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=R2_BUCKET_NAME, Prefix=remote_folder_path)

        # Iterate through all pages
        for page in pages:
            if 'Contents' not in page:
                continue

            for obj in page['Contents']:
                bucket_key = obj['Key']

                # Skip if it's just a folder marker (ends with /)
                if bucket_key.endswith('/'):
                    continue

                # Create local path maintaining folder structure
                relative_path = bucket_key[len(remote_folder_path):]
                local_path = os.path.join(local_download_dir, original_folder_name, relative_path)

                files_to_download.append((bucket_key, local_path))

        if not files_to_download:
            print(f"❌ No files found to download in {remote_folder_path}")
            return

        print(f"📊 Found {len(files_to_download)} files to download")
        print(f"📂 Downloading to: {os.path.join(local_download_dir, original_folder_name)}")

        # Use ThreadPoolExecutor for concurrent downloads
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(download_file_from_r2, bk, lp) for bk, lp in files_to_download]

            successful_downloads = 0
            failed_downloads = 0

            # tqdm progress bar
            for future in tqdm(as_completed(futures), total=len(futures), desc="Downloading", unit="file"):
                bucket_key, success, error = future.result()
                if success:
                    successful_downloads += 1
                else:
                    failed_downloads += 1
                    print(f"❌ Failed {bucket_key}: {error}")

        print(f"\n✅ Download complete!")
        print(f"   ✓ {successful_downloads} successful")
        print(f"   ✗ {failed_downloads} failed")
        print(f"📁 Files saved to: {os.path.join(local_download_dir, original_folder_name)}")

    except Exception as e:
        print(f"❌ Error: {str(e)}")

# Example usage
if __name__ == "__main__":
    download_folder_from_r2(
        remote_folder_path='raw_gui',
        local_download_dir="./",
        workers=20
    )
