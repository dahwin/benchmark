import zipfile
import os
import time
from concurrent.futures import ThreadPoolExecutor

zip_file_path = "organized_images.zip"
extract_dir = "./"

start_time = time.time()

with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
    # Get all file names
    file_list = zip_ref.namelist()
    print(f"Found {len(file_list)} files to extract")
    
    # Extract files in parallel using ThreadPoolExecutor
    def extract_file(filename):
        zip_ref.extract(filename, extract_dir)
    
    # Automatically use CPU count * 2 for I/O-bound tasks
    max_workers = os.cpu_count() * 2
    print(f"Using {max_workers} workers")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(extract_file, file_list)

end_time = time.time()
elapsed_time = end_time - start_time

print(f"Extraction complete.")
print(f"Time taken: {elapsed_time:.2f} seconds")
