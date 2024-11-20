""" Azure Blob Tools Module

This module contains functions to interface with the Azure blob.

- create_container(container_name): Creates a new container with the specified 
  variable 'container_name'.
- upload_images_to_blob(): Uploads a folder of images to the specified blob 
  container.
- list_containers_in_blob(connection_string): Lists the containers in the 
  Azure blob associated with the input connection string.
- list_files_in_blob(container, connection_string, png_only=True): Lists all the
  files in the indicated container. Option to list only PNGs. 
"""

# imports

import numpy as np
import pandas as pd
from azure.storage.blob import BlobServiceClient
from utopia_pipeline_tools import config_info
import os

# functions
def create_container(container_name):
    """
    Creates a new container in the Azure Blob storage.

    :param container_name: The name of the new container
    :type container_name: str
    """
    # Test configuration
    if config_info['connection_string'] is None:
        print("ACTION REQUIRED: Enter connection string information.")
    else:
        # connect to the blob storage
        blob_service_client = BlobServiceClient.from_connection_string(
            conn_str=config_info['connection_string'])
        # create the new container
        blob_service_client.create_container(container_name)
        print("INFO: Container created successfully!")

def upload_images_to_blob(container_name, local_directory_path):
    """
    Upload a folder to the Azure Blob storage.

    :param container_name: Indicates which blob container the images will be 
    uploaded to
    :type container_name: str
    :param local_directory_path: The path to the ml folder
    :type local_directory_path: str
    """
    # test configuration
    if config_info['blob_storage_name'] is None:
        print("ACTION REQUIRED: Enter blob storage information.")
    else:
        # upload the folder with an azcopy command
        os.system(f"""azcopy copy '{local_directory_path}' 
                'https://{config_info['blob_storage_name']}.blob.core.windows.
                net/{container_name}' --recursive""")
        print("INFO: Your folder has been uploaded successfully!")

def list_containers_in_blob(connection_string=config_info['connection_string']):
    """
    Returns a list of the ifcb image containers in the specified Azure Blob 
    storage.

    :param connection_string: The blob connection string. Defaults to the 
    information in the __init__ file.
    :type connection_string: str, optional
    """
    # test configuration
    if config_info['connection_string'] is None:
        print("ACTION REQUIRED: Enter connection string information.")
    else:
        # connect to the Azure Blob
        blob_service_client = BlobServiceClient.from_connection_string(
            conn_str=connection_string)
        container_info = blob_service_client.list_containers()
        blob_containers = []

        # list the dataset containers in the blob (all except files)
        # excluding files since the only containers of interest in this 
        # context are the ones with ifcb images
        for item in container_info:
            if item['name'] == 'files':
                pass
            else:
                blob_containers.append(item['name'])
        
        return blob_containers

def list_files_in_blob(container, 
                       connection_string=config_info['connection_string'], 
                       png_only=True):
    """
    Returns a Pandas dataframe of image filepaths if png_only parameter is true.
    Otherwise, this function returns a list of the filepaths of all files in the
    blob container. 

    :param container: Specifies the name of the blob container
    :type container: str
    :param connection_string: The blob connection string. Defaults to the 
    connection string saved in the __init__ file
    :type connection_string: str, optional
    :param png_only: Indicates whether to retrieve all files (False), or images 
    only (True); default value is True
    :type png_only: bool, optional
    """
    # test configuration
    if config_info['connection_string'] is None:
        print("ACTION REQUIRED: Enter connection string information.")
    else:
        # connect to Azure blob
        blob_service_client = BlobServiceClient.from_connection_string(
            conn_str=connection_string)
        container_client = blob_service_client.get_container_client(container)

        # List all blobs in the container
        blob_list = container_client.list_blobs()
        filepaths = [blob.name for blob in blob_list]

        if png_only is True:
            # select only the image files
            png_list = [x for x in filepaths if '.png' in x]
            png_df = pd.DataFrame({'filepath': png_list})

            return png_df
        
        else:
            return filepaths