# Databricks notebook source
# MAGIC %pip install openpyxl faker azure-identity -q

# COMMAND ----------

pip install azure-storage-blob

# COMMAND ----------

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
fake = Faker()

# COMMAND ----------

start_runtime = time.time()

# COMMAND ----------

'''
client_id = dbutils.secrets.get(scope='KeyVault1Pablo', key='app1-client-id')
client_secret = dbutils.widgets.get(scope='KeyVault1Pablo', key='app1-client-secret')
tenant_id = dbutils.widgets.get(scope='KeyVault1Pablo', key='app1-tenant-id')
'''

# COMMAND ----------

'''
configs = {
  "fs.azure.account.auth.type": "OAuth",
  "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id": client_id,
  "fs.azure.account.oauth2.client.secret": client_secret, 
  "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
  "fs.azure.createRemoteFileSystemDuringInitialization": "true"
}
'''

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

dbutils.fs.ls("/mnt/masking_config")

# COMMAND ----------

# MAGIC %md
# MAGIC Masking Projects

# COMMAND ----------

#dbutils.widgets.removeAll()
dbutils.widgets.dropdown("Project", "Test Project 1", ['Test Project 1', 'Test Project 2'])

# COMMAND ----------

# MAGIC %md
# MAGIC Project File Paths

# COMMAND ----------

#This cell defines paths for the unique masking definition files (intake form) and mask dictionaries (lookup table), grouped by project.

project_definition = dbutils.widgets.get("Project")
print(f"Project: {project_definition}")

if project_definition == 'Test Project 1':
  #Enter missing locations once ADLS2 account created
  masking_rules_location = '/dbfs/mnt/masking_config/Test Project 1/Configurable Masking Rulebook.xlsm'
  masking_dictionary_location = '/dbfs/mnt/masking_config/Test Project 1/mask_dictionary.csv'
  reference_filename_mapping = f'/dbfs/mnt/masking_config/Test Project 1/{project_definition} Reference Filename Mapping.csv'
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


# Configure ADLS connection to output location
df_write_location = pd.read_excel(Masking_definition_file, engine='openpyxl', sheet_name='Write Location')
display(df_write_location)

write_storage_account = df_write_location['Storage Account'][0]
write_container = df_write_location['Container'][0]
write_directory_path = df_write_location['Directory Path'][0]
if str(write_directory_path) == 'nan':
    write_directory_path = ''
print(f'Write storage account: {write_storage_account}')
print(f'Write container: {write_container}')
print(f'Write directory path: {write_directory_path}')

# Checks if mount point exists; if not, create it
output_mount_point_name = ('/mnt/output_path_' + write_storage_account + '_' + write_container)
if any(mount.mountPoint == output_mount_point_name for mount in dbutils.fs.mounts()):
  mount_point = output_mount_point_name
  print('mount point alwritey exists')
else:
  dbutils.fs.mount(
    source = "abfss://" + write_container + "@" + write_storage_account + ".dfs.core.windows.net/",
    mount_point = output_mount_point_name,
    extra_configs = configs)
  print('datalake mount done')

if str(write_directory_path) != '':
    output_file_locations = "/dbfs" + output_mount_point_name + '/' + write_directory_path
else:
    output_file_locations = "/dbfs" + output_mount_point_name
print(output_file_locations)

# COMMAND ----------

input_path = input_file_locations
output_path = output_file_locations

# COMMAND ----------

# Defines Standard masking function built to maintain data integrity. Called by the higher-level mask_value function

def standard_mask(value):
    # Convert the input to string datatype for processing
    original_type = type(value)
    value = str(value)

    masked_value = ''

    for char in value:
        if char.isupper():
            masked_value += random.choice(string.ascii_uppercase)
        elif char.islower():
            masked_value += random.choice(string.ascii_lowercase)
        elif char.isdigit():
            masked_value += random.choice(string.digits)
        else:
            masked_value += char

    # Convert masked value back into original datatype
    return original_type(masked_value)

# COMMAND ----------

# Defines function used to mask each value in the attribute or column selected for masking, based on the masking type defined in the intake form

def mask_value(original, lookup_table, length=None, column_name=None, masking_type=None, ref_integrity=None):

    global masked_values_single_run # Declares the use of a global variable for single-run ref integrity

    # Defines tables/columns for which lookup table from prior masking will be referenced, and for which the mask mapping will be persisted in ADLS

    if ref_integrity == 'Y':
        lookup_table = masked_values
    elif ref_integrity == 'N':
        lookup_table = masked_values_single_run

    if original == 'null' or original is None:
        return original
    
    #Standard masking function -------------------------------------------------------------------------------------------------------------------------------------
    if masking_type == "Standard":
        if original in lookup_table:
            return lookup_table[original]
        else:
            masked_val = standard_mask(original)
            lookup_table[original] = masked_val
            return masked_val
    
    #Age masking function -------------------------------------------------------------------------------------------------------------------------------------
    if masking_type == "Age":
        if original in lookup_table:
            return lookup_table[original]
        else:
            age = random.randint(18, 100)
            masked_val = age
            lookup_table[original] = masked_val
            return masked_val
        
    #Email masking function -------------------------------------------------------------------------------------------------------------------------------------
    if masking_type == "Email":
        if original in lookup_table:
            return lookup_table[original]
        else:
            masked_val = standard_mask(original)
            lookup_table[original] = masked_val
            try:
                # Maintain referential integrity for username portion, as well as full email.
                username, domain = original.split('@')
                masked_username, masked_domain = masked_val.split('@')
                lookup_table[username] = masked_username
            except:
                pass
            return masked_val
    
    #Email - Retain Domain masking function -------------------------------------------------------------------------------------------------------------------------------------
    if masking_type == "Email - Retain Domain":
        try:
            username, domain = original.split('@')
            if username in lookup_table:
                masked_username = lookup_table[username]
            else:
                masked_username = standard_mask(username)
                lookup_table[username] = masked_username
            return f"{masked_username}@{domain}"
        except:
            if original in lookup_table:
                return lookup_table[original]
            else:
                masked_val = standard_mask(original)
                lookup_table[original] = masked_val
                return masked_val
            
    #Full Name masking function -------------------------------------------------------------------------------------------------------------------------------------
    if masking_type == "Full Name":
        if original in lookup_table:
            return lookup_table[original]
        else:
            masked_val = standard_mask(original)
            lookup_table[original] = masked_val

            # Keep record of each individual name from full name, mask and maintain referential integrity for each where specified, matching full name masking
            names = re.sub(r'[\w\s]', '', original)
            masked_names = re.sub(r'[\w\s]', '', masked_val)
            original_names_list = []
            masked_names_list = []
            for name in names.split(' '):
                original_names_list.append(name)
            for name in masked_names_list.split(' '):
                masked_names_list.append(name)

            for i in range(len(original_names_list)):
                current_name = original_names_list[i]
                if current_name in lookup_table:
                    masked_name = lookup_table[current_name]
                else:
                    lookup_table[current_name] = masked_names_list[i]
                    masked_name = lookup_table[current_name]
                
        return masked_val

    #Physical Address masking function -------------------------------------------------------------------------------------------------------------------------------------
    if masking_type == "Physical Address":
        if original in lookup_table:
            return lookup_table[original]
        else:
            fake_address = fake.address()
            masked_val = fake_address
            lookup_table[original] = masked_val
            return masked_val
    
    #IP Address masking function -------------------------------------------------------------------------------------------------------------------------------------
    if masking_type == "IP Address":
        if original in lookup_table:
            return lookup_table[original]
        else:
            ip = original.split('.')
            masked_val = '.'.join(standard_mask(i) for i in ip)
            lookup_table[original] = masked_val
            return masked_val
    
    #IP Address - Ignore 10. masking function -------------------------------------------------------------------------------------------------------------------------------------
    if masking_type == "IP Address":
        ip = original.split('.')
        if (ip[0] == '10'):
                return original
        elif original in lookup_table:
            return lookup_table[original]
        else:
            masked_val = '.'.join(standard_mask(i) for i in ip)
            lookup_table[original] = masked_val
            return masked_val
    
    #Credit Card masking function -------------------------------------------------------------------------------------------------------------------------------------
    if masking_type == "Credit Card":
        if original in lookup_table:
            return lookup_table[original]
        else:
            masked_val = fake.credit_card_number("visa16")
            lookup_table[original] = masked_val
            return masked_val
    
     #Custom Rule #1 masking function ------------------------------------------------------------------------------------------------------------------------------------
    if masking_type == "Custom Rule #1":
        if original in lookup_table:
            return lookup_table[original]
        else:
            masked_val = standard_mask(original)
            lookup_table[original] = masked_val

            # Keep record of each individual name from full name, mask and maintain referential integrity for each where specified, matching full name masking
            names = re.sub(r'[\w\s]', '', original)
            masked_names = re.sub(r'[\w\s]', '', masked_val)
            original_names_list = []
            masked_names_list = []
            for name in names.split(' '):
                original_names_list.append(name)
            for name in masked_names_list.split(' '):
                masked_names_list.append(name)

            for i in range(len(original_names_list)):
                current_name = original_names_list[i]
                if current_name in lookup_table:
                    masked_name = lookup_table[current_name]
                else:
                    lookup_table[current_name] = masked_names_list[i]
                    masked_name = lookup_table[current_name]
                
        return masked_val

# COMMAND ----------

# Calls the mask_value function to apply masking to specified column

def mask_dataframe(df, column_name, length=None, masking_type=None, ref_integrity=None):

    if masking_type == 'Custom Rule #1':
        masked_column = df.apply(lambda x: mask_value(x[column_name], masked_values, length, column_name=column_name, masking_type=masking_type, ref_integrity=ref_integrity) if not x['IP Address v4'].startswith('10.') else x[xolumn_name], axis=1)
        return masked_column
    
    masked_column = df[column_name].apply(lambda x: mask_value(x, masked_values, length, column_name=column_name, masking_type=masking_type, ref_integrity=ref_integrity))
    return masked_column

# COMMAND ----------

# Import and display the Configurable Masking Rulebook, derived from the selected project
df_mask_def_values = pd.read_excel(Masking_definition_file, engine='openpyxl', sheet_name='Masking Definition')
display(df_mask_def_values)

# COMMAND ----------

reference_filename_mapping_df = pd.read_csv(reference_filename_mapping)
display(reference_filename_mapping_df)

# COMMAND ----------

df_mask_def_values2 = pd.merge(df_mask_def_values, reference_filename_mapping_df, left_on='Table', right_on='Modified', how='left')
display(df_mask_def_values2)

df_mask_def_values['Table'] = df_mask_def_values2['Original'].fillna(df_mask_def_values2['Table'])

display(df_mask_def_values)

# COMMAND ----------

# Create file directory_path_df, a DataFrame that references all files to be masked, and their respective absolute paths, relative paths to the input_files location, the highest level directory they belong to after the path defined by input_files, and ultimately the file name for each.

all_paths = os.path.abspath(input_path)
all_files = []
all_tables = []
all_relative_paths = []
data = []

for dirpath, dirnames, filenames in os.walk(all_paths):
    for filename in filenames:
        full_path = os.path.join(dirpath, filename)
        relative_path = os.path.relpath(dirpath, all_paths)
        highest_level_directory = relative_path.split(os.sep)[0]

        # Replace "highest level directory" with "file name" (minus extension) in instances where the table for masking is not a subdirectory that represents tables of same schema, but is instead a file directly below the directory defined as the input_path as a standalone file by itself. 
        if dirpath == all_paths:
            highest_level_directory, extension = filename.split('.')
        
        all_files.append(full_path)
        all_tables.append(highest_level_directory)
        all_relative_paths.append(relative_path)
        data.append({"full path": full_path, "relative path": relative_path, "highest level directory": highest_level_directory, "file name": filename})
    file_directory_path_df = pd.DataFrame(data)
display(file_directory_path_df)

# COMMAND ----------

# Defines highest-level function for masking, that will be called upon each table schema for masking. This function will itterate over all columns to be masked for each table schema, and will call on the mask_dataframe function to perform masking for each column.

def apply_masking(data_extract, var_file, relative_path, filename, full_directory_path, df_mask_def_values):

    print("-------------------------------------------------------------------------------------------------------------------------------------")
    print(f'Absolute file path: {i}')
    print(f'Schema: {var_file}')
    newdf = df_mask_def_values[df_mask_def_values['Table'] == var_file]
    mask_attr = newdf.Attribute.to_list()
    print(f"Attributes for masking: {mask_attr}\n")
    if len(mask_attr) == 0:
        return
    mask_rule_list = newdf['Masking Rule'].to_list()

    for f in mask_attr:
        print('---')
        print(f'Masked Attribute: {f}')

        masking_rule_series = df_mask_def_values[df_mask_def_values['Attribute'] == f]['Masking Rule']
        ref_integrity_series = df_mask_def_values[df_mask_def_values['Attribute'] == f]['Ref Integrity']
        mask_rule = masking_rule_series.iloc[0]
        print(f'Masking Rule: {mask_rule}')
        ref_integrity = ref_integrity_series.iloc[0]
        print(f'Ref Integrity: {ref_integrity}\n')
        data_extract[f] = mask_dataframe(data_extract, f, 10, mask_rule, ref_integrity)
        dataframes.append(data_extract)

    df_output = data_extract

    # Construct the full directory path where the masked file will be saved
    full_directory_path = os.path.join(output_path, relative_path)

    # Ensure the directory path exists
    os.makedirs(full_directory_path, exist_ok=True)

    # Return df_output (dataframe containing masked table)
    return df_output

# COMMAND ----------

print(all_files)

# COMMAND ----------

# Itterate over each file for masking, call upon the apply_masking function, and save the files to the defined output location

try:
    df_masked_values = pd.read_csv(mask_map_table_path)
except:
    df_masked_values = pd.DataFrame({"Original": [], "Masked Values": []})

masked_values  = dict([a,b] for a,b in zip(df_masked_values['Original'], df_masked_values['Masked Values']))
lookup_table = {"Original": [], "Masked values": []}
data_extract = pd.DataFrame()
files = [f for f in file_directory_path_df['full path'] if file_directory_path_df['full path']]
num_files = 0
for j in files:
    if j.endswith('.csv'):
        num_files += 1
    elif j.endswith('.parquet'):
        num_files += 1

print(f"Number of files for masking: {num_files}\n")
for i in files:
    dataframes = []
    var_file = file_directory_path_df.loc[file_directory_path_df['full path'] == i, "highest level directory"].values[0]
    relative_path = file_directory_path_df.loc[file_directory_path_df['full path'] == i, "relative path"].values[0]
    filename = file_directory_path_df.loc[file_directory_path_df['full path'] == i, "file name"].values[0]

    # Construct the full directory path where the masked file will be saved
    full_directory_path = os.path.join(output_path, relative_path)

    # Ensure the directory path exists
    os.makedirs(full_directory_path, exist_ok=True)

    # Apply masking to parquet files
    if i.endswith('.parquet'):
        data_extract = pd.read_parquet(f"{i}")
        try:
            df_output = apply_masking(data_extract, var_file, relative_path, filename, full_directory_path, df_mask_def_values)
            df_output.to_parquet(f"{full_directory_path}/{filename}", index=False)
        except:
            pass

    # Apply masking to csv files
    elif i.endswith('.csv'):
        data_extract = pd.read_csv(f"{i}")
        try:
            df_output = apply_masking(data_extract, var_file, relative_path, filename, full_directory_path, df_mask_def_values)
            df_output.to_csv(f"{full_directory_path}/{filename}", index=False)
        except:
            pass
    
    # Move log files
    elif i.endswith('.json'):
        copyfile(i, os.path.join(full_directory_path, filename))
    elif i.endswith('.crc'):
        copyfile(i, os.path.join(full_directory_path, filename))
    else:
        pass

# Persist masked values (where ref integrity is set to Y) in lookup table to maintain referential integrity on future runs
metadata_val = {"Original":list(masked_values.keys()), "Masked Values":list(masked_values.values())}
df_metadata = pd.DataFrame.from_dict(metadata_val)
df_metadata.to_csv(mask_map_table_path, index=False)

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
