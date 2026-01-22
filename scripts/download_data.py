import kagglehub
import shutil
import os

# Download latest version
print("Downloading dataset...")
path = kagglehub.dataset_download("olistbr/brazilian-ecommerce")

print("Path to dataset files:", path)

# Define target directory
target_dir = "/home/jpg/proyectos/MCGE/data/raw"

# Ensure target directory exists
os.makedirs(target_dir, exist_ok=True)

# Move files to target directory
print(f"Moving files to {target_dir}...")
for filename in os.listdir(path):
    src_file = os.path.join(path, filename)
    dst_file = os.path.join(target_dir, filename)
    if os.path.isfile(src_file):
        shutil.copy2(src_file, dst_file)
        print(f"Copied {filename}")

print("Download and setup complete.")
