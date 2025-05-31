import lithops
import os
import concurrent.futures
"""
Upload datasets to the specified storage bucket.
"""
# Initialize Lithops with default configuration
fexec = lithops.FunctionExecutor()

# Define the bucket name and dataset paths
bucket_name = fexec.storage.bucket

# Get all files from imagenet_images/10kds/ locally
DATASET = '10kds'
dataset_local_path = f'../datasets/imagenet_images/{DATASET}'


# List all files in the dataset directory
dataset_path = os.path.abspath(dataset_local_path)
files = [f for f in os.listdir(dataset_path) if os.path.isfile(os.path.join(dataset_path, f))]

# Upload each file to the bucket
def upload_file(file):
    local_path = os.path.join(dataset_path, file)
    s3_key = f'{DATASET}/{file}'   # <- This is the S3 object key!
    print(f'Uploading {local_path} as {s3_key} to bucket {bucket_name}')
    with open(local_path, 'rb') as f:
        fexec.storage.put_object(bucket_name, s3_key, f.read())

with concurrent.futures.ThreadPoolExecutor(max_workers=200) as executor:
    executor.map(upload_file, files)

# Get list of keys in the dataset. Check in S3 if the files were uploaded correctly
keys = fexec.storage.list_objects(bucket_name, prefix=f'{DATASET}/')
keys = [key['Key'] for key in keys if key['Key'].startswith(f'{DATASET}/')]

# Save the keys to a text file
keys_file_path = f'../datasets/imagenet_keys/{DATASET}.txt'
with open(keys_file_path, 'w') as keys_file:
    for key in keys:
        keys_file.write(f'{key}\n')