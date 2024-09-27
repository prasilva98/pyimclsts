import os
import shutil

# Define the main path
path = '/mnt/sdb1/Missions/REP_24/lauv-noptilus-3/20240917'

item_list = []

for item in os.listdir(path):
    
    item_list.append(os.path.join(path, item))
    item_list.sort()

# Function to copy contents from src to dst
def copy_contents(src, dst):
    # Copy all files and directories
    for item in os.listdir(src):
        src_item = os.path.join(src, item)
        dst_item = os.path.join(dst, item)

        # Check if the item is a directory or file and copy accordingly
        if os.path.isdir(src_item):
            shutil.copytree(src_item, dst_item, dirs_exist_ok=True)  # Copy directories
        else:
            shutil.copy2(src_item, dst_item)  # Copy files

for main_path in item_list:

  # Iterate over all items in the target directory
  for item in os.listdir(main_path):
      
      item_path = os.path.join(main_path, item)
      mra_path = os.path.join(main_path, 'mra')  
      csv_path = os.path.join(main_path, 'csv')
      # Check if the item is not the folder to keep
      if item != 'mra':
          # Delete directories and their contents
          if os.path.isdir(item_path):
              shutil.rmtree(item_path)  # Remove the directory and its contents
          else:
              os.remove(item_path)  # Remove the file

  print(f"Deleted all contents in '{main_path}' except for the 'mra' folder.")

  # Copy everything from mra and csv to the target directory
  copy_contents(mra_path, main_path)
  copy_contents(csv_path, main_path)

  print(f"Copied contents from '{mra_path}' and '{csv_path}' to '")

  # Iterate over all items in the target directory
  for item in os.listdir(main_path):
      item_path = os.path.join(main_path, item)

      # Check if the item is not the folder to keep
      if item == 'csv' or item == 'mra':
          # Delete directories and their contents
          if os.path.isdir(item_path):
              shutil.rmtree(item_path)  # Remove the directory and its contents
          else:
              os.remove(item_path)  # Remove the file

