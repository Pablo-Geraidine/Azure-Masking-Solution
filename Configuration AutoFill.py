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
  masking_rules_location_no_macro = '/dbfs/mnt/masking_config/Test Project 1/Configurable Masking Rulebook - blank.xlsx'
  new_excel_path = '/dbfs/mnt/masking_config/Test Project 1/Configurable Masking Rulebook Modified.xlsm'
  new_excel_path_no_macro = '/dbfs/mnt/masking_config/Test Project 1/Configurable Masking Rulebook Modified.xlsx'
  local_path = "/tmp/Configurable Masking Rulebook Modified.xlsm"
  #copyfile(masking_rules_location, new_excel_path)
  masking_dictionary_location = '/dbfs/mnt/masking_config/Test Project 1/mask_dictionary.csv'
elif project_definition == 'Test Project 2':
  #Enter missing locations once ADLS2 account created
  masking_rules_location = ''
  masking_dictionary_location = ''

Masking_definition_file = masking_rules_location
mask_map_table_path = masking_dictionary_location

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

# Initialize Spark session
spark = SparkSession.builder.appName("Dataframe Column Names").getOrCreate()

# Base path for the mount point
mount_point_path = input_mount_point_name

# List all files in the mount point
file_paths = dbutils.fs.ls(mount_point_path)
valid_files = [file.name for file in file_paths if file.name.endswith(('.csv', '.parquet'))]

# Dictionary to hold filenames and their columns
#long_dict = {}
files_columns_dict = {}
#list_of_dicts = []
# Try to create a DataFrame from each file and collect column names
for file in valid_files:
    try:
        path = os.path.join(mount_point_path, file)
        df = None
        if file.endswith('.csv'):
            df = spark.read.csv(path, header=True, inferSchema=True)
        elif file.endswith('.parquet'):
            df = spark.read.parquet(path)
        # If a DataFrame was successfully created, store the column names
        if df:
            files_columns_dict[file] = df.columns
            #long_dict.append(files_columns_dict)
        #display(df)
        print(files_columns_dict)
        #files_columns_dict_df = pd.DataFrame(files_columns_dict)
        #display(files_columns_dict_df)
        #list_of_dicts.append(files_columns_dict)
    except Exception as e:
        print(f"Could not process {file}: {e}")
validation_list = pd.DataFrame(dict([(k, pd.Series(v)) for k, v in files_columns_dict.items()]))
display(validation_list)

# COMMAND ----------

list_of_dicts_df = pd.DataFrame(list_of_dicts)
display(list_of_dicts_df)

# COMMAND ----------

copyfile(masking_rules_location, local_path)
wb = load_workbook(local_path, keep_vba=True)

#validation_list.to_excel(local_path, engine='openpyxl', sheet_name='file mapping', index=False)

if 'file mapping' in wb.sheetnames:
    wb.remove(wb['file mapping'])

with pd.ExcelWriter(local_path, engine='openpyxl') as writer:
    writer.book = wb

    # Your dataframe to be added (I'm using validation_list as a placeholder for your actual DataFrame)
    validation_list.to_excel(writer, sheet_name='file mapping', index=False)


copyfile(local_path, new_excel_path)

# COMMAND ----------

masking_rules_location_no_macro

copyfile(masking_rules_location, local_path)
wb = load_workbook(local_path)

#validation_list.to_excel(local_path, engine='openpyxl', sheet_name='file mapping', index=False)

if 'file mapping' in wb.sheetnames:
    wb.remove(wb['file mapping'])

with pd.ExcelWriter(local_path, engine='openpyxl') as writer:
    writer.book = wb

    # Your dataframe to be added (I'm using validation_list as a placeholder for your actual DataFrame)
    validation_list.to_excel(writer, sheet_name='file mapping', index=False)


copyfile(local_path, new_excel_path_no_macro)

# COMMAND ----------

copyfile(masking_rules_location, local_path)
wb = load_workbook(local_path, keep_vba=True)

#validation_list.to_excel(local_path, engine='openpyxl', sheet_name='file mapping', index=False)

if 'file mapping' in wb.sheetnames:
    wb.remove(wb['file mapping'])

with pd.ExcelWriter(local_path, engine='openpyxl') as writer:
    writer.book = wb

    # Your dataframe to be added (I'm using validation_list as a placeholder for your actual DataFrame)
    validation_list.to_excel(writer, sheet_name='file mapping', index=False)


copyfile(local_path, new_excel_path)

# COMMAND ----------

excel_path = Masking_definition_file
#rulebook_df = pd.read_excel(excel_path, sheet_name=0) # Assuming the first sheet is the one we need
#display(rulebook_df)

# COMMAND ----------

from openpyxl import load_workbook
from openpyxl.worksheet.datavalidation import DataValidation
import shutil

# Assuming the rest of the code from the previous snippet has run and files_columns_dict is populated

# Load the workbook and select the active sheet
wb = load_workbook(excel_path)
sheet = wb.active

# Create a data validation object for column 'A' (Table names)
dv_tables = DataValidation(type="list", formula1='"{}"'.format(','.join(files_columns_dict.keys())), showDropDown=True)
sheet.add_data_validation(dv_tables)

# Add the validation to all the cells in column 'A' that might be used
for row in range(2, sheet.max_row + 1):  # Assuming row 1 is the header
    dv_tables.add(sheet.cell(row=row, column=1))

# Now we set up the data validation for column 'B' (Attributes)
# Note: True dependent dropdowns cannot be created directly via openpyxl, because they require INDIRECT formula,
# which is not preserved when saving the workbook with openpyxl. 
# We will create a validation for each cell in column B based on corresponding file in column A
for row in range(2, sheet.max_row + 1):
    file_name_cell = 'A{}'.format(row)
    dv_attributes = DataValidation(type="list", formula1='"{}"'.format(','.join(files_columns_dict.get(sheet[file_name_cell].value, []))), showDropDown=True)
    sheet.add_data_validation(dv_attributes)
    dv_attributes.add(sheet.cell(row=row, column=2))

# Save the workbook with a new name
#new_excel_path = os.path.join(mount_point_path, "Configurable Masking Rulebook Modified.xlsx")


# Save the workbook to the local file system of the driver node
wb.save(local_path)

# Use dbutils to copy the file from the local file system to the mount point
copyfile(local_path, new_excel_path)

# ORemove the temporary local file after copying
os.remove(local_path)


#wb.save(new_excel_path)

# COMMAND ----------

from openpyxl.utils import get_column_letter

# Create a new sheet for the Table-Attribute mapping
wb.create_sheet(title="Table_Attribute_Mapping")
mapping_sheet = wb["Table_Attribute_Mapping"]

# Populate the mapping sheet with table names and attributes
for col_index, (table_name, attributes) in enumerate(files_columns_dict.items(), start=1):
    # Set the table name as the header of the column
    mapping_sheet.cell(row=1, column=col_index).value = table_name

    # List attributes below the table name
    for row_index, attribute in enumerate(attributes, start=2):
        mapping_sheet.cell(row=row_index, column=col_index).value = attribute

# The rest of your script for setting data validation in column A
# ... (your existing code)

# Now, instead of creating the validation in column B with openpyxl,
# you leave the column B validation blank and will manually add it in Excel

# Save the workbook to a temporary file
#with local_path.NamedTemporaryFile() as tmp:
wb.save(local_path)
#tmp.seek(0)

    # Now upload the file from local to DBFS
#try:
#    dbutils.fs.cp(f'file:{tmp.name}', new_excel_path)
#except Exception as e:
#    print(f"Error while saving to DBFS: {e}")
copyfile(local_path, new_excel_path)

# COMMAND ----------


