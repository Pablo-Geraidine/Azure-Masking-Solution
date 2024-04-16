# Databricks notebook source
# MAGIC %pip install openpyxl faker azure-identity -q

# COMMAND ----------

pip install azure-storage-blob

# COMMAND ----------

from pyspark.sql import SparkSession
import pandas as pd
import os
import uuid
import string
import random
import time
import re
from datetime import datetime
from faker import Faker
import shutil
from shutil import copyfile
from openpyxl import load_workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.worksheet.datavalidation import DataValidation

# COMMAND ----------

start_runtime = time.time()

# COMMAND ----------

configs = {
  "fs.azure.account.auth.type": "OAuth",
  "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id": "f53acde7-87dc-4744-b03b-05b298a79ab6",
  "fs.azure.account.oauth2.client.secret": "bjx8Q~z5o7RL3F49WF4ej.quAj9Gdyn0ECmVgcjn", 
  "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/8f72f2f2-1c4b-4d6a-a13a-3825abea5a3c/oauth2/token",
  "fs.azure.createRemoteFileSystemDuringInitialization": "true"
}

# COMMAND ----------

# Checks if mount point exists; if not, create it
if any(mount.mountPoint == '/mnt/masking_config' for mount in dbutils.fs.mounts()):
  mount_point = '/mnt/masking_config'
  print('mount point already exists')
else:
  dbutils.fs.mount(
    source = "abfss://masking-config@pabloconfig.dfs.core.windows.net/",
    mount_point = "/mnt/masking_config",
    extra_configs = configs)
  print('datalake mount done')

# COMMAND ----------

#dbutils.widgets.removeAll()
dbutils.widgets.dropdown("Project", "Test Project 1", ['Test Project 1', 'Test Project 2'])

# COMMAND ----------

#This cell defines paths for the unique masking definition files (intake form) and mask dictionaries (lookup table), grouped by project.

project_definition = dbutils.widgets.get("Project")
print(f"Project: {project_definition}")
mount_point_path = '/dbfs/mnt/masking_config/Test Project 1/'

if project_definition == 'Test Project 1':
  #Enter missing locations once ADLS2 account created
  masking_rules_location = '/dbfs/mnt/masking_config/Test Project 1/Configurable Masking Rulebook - blank -macro.xlsm'
  #masking_rules_location_no_macro = '/dbfs/mnt/masking_config/Test Project 1/Configurable Masking Rulebook - blank.xlsx'
  new_excel_path = '/dbfs/mnt/masking_config/Test Project 1/Configurable Masking Rulebook Modified.xlsm'
  #new_excel_path_no_macro = '/dbfs/mnt/masking_config/Test Project 1/Configurable Masking Rulebook Modified.xlsx'
  local_path = "/tmp/Configurable Masking Rulebook Modified.xlsm"
  #copyfile(masking_rules_location, new_excel_path)
  #masking_dictionary_location = '/dbfs/mnt/masking_config/Test Project 1/mask_dictionary.csv'
elif project_definition == 'Test Project 2':
  #Enter missing locations once ADLS2 account created
  masking_rules_location = ''
  masking_dictionary_location = ''

Masking_definition_file = masking_rules_location
#mask_map_table_path = masking_dictionary_location

# COMMAND ----------

# Configure ADLS connection to input location
df_read_location = pd.read_excel(Masking_definition_file, engine='openpyxl', sheet_name='Read Location')
display(df_read_location)

read_storage_account = df_read_location['Storage Account'][0]
read_container = df_read_location['Container'][0]
read_directory_path = df_read_location['Directory Path'][0]
if str(read_directory_path) == 'nan':
    read_directory_path = ''
print(f'Read storage account: {read_storage_account}')
print(f'Read container: {read_container}')
print(f'Read directory path: {read_directory_path}')

# Checks if mount point exists; if not, create it
input_mount_point_name = ('/mnt/input_path_' + read_storage_account + '_' + read_container)
if any(mount.mountPoint == input_mount_point_name for mount in dbutils.fs.mounts()):
  mount_point = input_mount_point_name
  print('mount point already exists')
else:
  dbutils.fs.mount(
    source = "abfss://" + read_container + "@" + read_storage_account + ".dfs.core.windows.net/",
    mount_point = input_mount_point_name,
    extra_configs = configs)
  print('datalake mount done')

if str(read_directory_path) != '':
    input_file_locations = "/dbfs" + input_mount_point_name + '/' + read_directory_path
else:
    input_file_locations = "/dbfs" + input_mount_point_name
print(input_file_locations)

# COMMAND ----------

mount_point_path = input_mount_point_name + '/' + read_directory_path
print(mount_point_path)

# COMMAND ----------

'''
files_columns_dict = {}
file_abs_paths = os.path.abspath(input_file_locations)
all_files = []
for dirpath, dirnames, filenames in os.walk(file_abs_paths):
    # Walk through all files under the input_file_location
    for file in filenames:
        print(file)
        try:
            if file.endswith(('.csv', '.parquet')):
                path = os.path.join(dirpath, file)
                print(path)
                df = None
            if file.endswith('.csv'):
                df = pd.read_csv(path)

            elif file.endswith('.parquet'):
                df = pd.read_parquet(path)

            files_columns_dict[file] = df.columns
                
        except Exception as e:
            print(f"Could not process {path}: {e}")
            pass

validation_list = pd.DataFrame(dict([(k, pd.Series(v)) for k, v in files_columns_dict.items()]))
display(validation_list)

'''

# COMMAND ----------

'''

files_columns_dict = {}
file_abs_paths = os.path.abspath(input_file_locations)

# Walk through all files and directories under the input_file_location
for dirpath, dirnames, filenames in os.walk(file_abs_paths):
    # Process each directory of files
    for dirname in dirnames:
        directory_path = os.path.join(dirpath, dirname)
        try:
            # Attempt to read all CSV files in the directory into a DataFrame
            csv_files = [os.path.join(directory_path, f) for f in os.listdir(directory_path) if f.endswith('.csv')]
            if csv_files:
                df = pd.concat([pd.read_csv(f) for f in csv_files], ignore_index=True)
                files_columns_dict[dirname] = df.columns.tolist()

            # Attempt to read all Parquet files in the directory into a DataFrame
            parquet_files = [os.path.join(directory_path, f) for f in os.listdir(directory_path) if f.endswith('.parquet')]
            if parquet_files:
                df = pd.read_parquet(directory_path)  # Reading from a directory directly
                files_columns_dict[dirname] = df.columns.tolist()

        except Exception as e:
            print(f"Could not process directory {directory_path}: {e}")

    # Process individual files within the current dirpath
    for file in filenames:
        try:
            path = os.path.join(dirpath, file)
            df = None
            if file.endswith('.csv'):
                df = pd.read_csv(path)
            elif file.endswith('.parquet'):
                df = pd.read_parquet(path)

            if df is not None:
                files_columns_dict[file] = df.columns.tolist()

        except Exception as e:
            print(f"Could not process file {path}: {e}")

# Convert the dictionary to a DataFrame for better visualization
validation_list = pd.DataFrame(dict([(k, pd.Series(v)) for k, v in files_columns_dict.items()]))
display(validation_list)
'''
# COMMAND ----------

# Dictionary to store columns for each directory/subdirectory
files_columns_dict = {}
# Dictionary to log errors for directories with multiple unique schemas
error_handling = {}

file_abs_paths = os.path.abspath(input_file_locations)

def aggregate_directory_columns(path):
    """ Helper function to read all parquet files within a directory and return column names """
    parquet_files = [os.path.join(path, f) for f in os.listdir(path) if f.endswith('.parquet')]
    if parquet_files:
        df = pd.concat([pd.read_parquet(f) for f in parquet_files], ignore_index=True)
        return df.columns.tolist()
    return None

# Walk through all files and directories under the input_file_location
for dirpath, dirnames, filenames in os.walk(file_abs_paths):
    # Process subdirectories within each directory
    for dirname in dirnames:
        directory_path = os.path.join(dirpath, dirname)
        subdirectory_key = os.path.join(os.path.basename(dirpath), dirname)
        try:
            columns = aggregate_directory_columns(directory_path)
            if columns:
                if subdirectory_key in files_columns_dict:
                    # Check for schema consistency within the subdirectory
                    if files_columns_dict[subdirectory_key] != columns:
                        # Log error if current schema doesn't match previous entries
                        if subdirectory_key not in error_handling:
                            error_handling[subdirectory_key] = [files_columns_dict[subdirectory_key], columns]
                        else:
                            error_handling[subdirectory_key].append(columns)
                    files_columns_dict[subdirectory_key] = columns  # Overwrite if mismatch for simplicity
                else:
                    files_columns_dict[subdirectory_key] = columns
        except Exception as e:
            print(f"Could not process directory {directory_path}: {e}")

# Print errors if any
if error_handling:
    print("Schema mismatches found in the following directories:")
    for key, value in error_handling.items():
        print(f"{key}: {value}")

# Convert the dictionary to a DataFrame for better visualization
validation_list = pd.DataFrame(dict([(k, pd.Series(v)) for k, v in files_columns_dict.items()]))
display(validation_list)


# COMMAND ----------

# Create a dictionary to map modified column names bacl to their original colun names - to be used to revert column names eventually
original_to_modified = {name: name.replace(' ', '_').replace('-', '_') for name in validation_list.columns}

# Remove whitespaces and dashes from the filenames so that VBA macros can still work
validation_list.columns = validation_list.columns.str.replace(' |-', '_', regex=True)
for column in validation_list.columns:
    print(column)
for i in original_to_modified:
    print(original_to_modified[i])

# COMMAND ----------

# Load workbook
copyfile(masking_rules_location, local_path)
wb = load_workbook(local_path, keep_vba=True)
file_mapping_sheet = wb['file mapping']
sheet1 = wb['Masking Definition']

# Clear existing data in 'file mapping'
for row in file_mapping_sheet.iter_rows(min_row=1, max_row=file_mapping_sheet.max_row, min_col=1, max_col=file_mapping_sheet.max_column):
    for cell in row:
        cell.value = None

# Write validation_list dataframe to 'file mapping'
for r_idx, row in enumerate(dataframe_to_rows(validation_list, index=False, header=True), start=1):
    for c_idx, value in enumerate(row, start=1):
        file_mapping_sheet.cell(row=r_idx, column=c_idx, value=value)

# Create Data Validation for sheet1. Formula: Reference first row in 'file mapping' sheet
dv = DataValidation(type='list', formula1="'file mapping'!$1:$1", showDropDown=False)
sheet1.add_data_validation(dv)

# Apply data validation to column A in sheet1
for row in range(1, sheet1.max_row + 1):
    dv.add(sheet1.cell(row=row, column=1))
    #sheet1.cell(row=row, column=3).value=f'=IF(ISBLANK(A{row}), "", A{row})'

wb.save(local_path)
copyfile(local_path, new_excel_path)

# COMMAND ----------

#Calculate and display total script runtime
end_runtime = time.time()
runtime = end_runtime - start_runtime

def seconds_to_time(seconds):
    int_seconds = int(seconds)

    # Calculate hours, minutes, and seconds
    hours = int_seconds // 3600
    int_seconds %= 3600
    minutes = int_seconds // 60
    remaining_seconds = int_seconds % 60
    fraction_seconds = seconds - int(seconds)

    formatted_time = "{:02}h:{:02}m:{:02}.{:03}s".format(hours, minutes, remaining_seconds, int(fraction_seconds*1000))

    return formatted_time
print(f'Total Runtime: {seconds_to_time(runtime)}')
