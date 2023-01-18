#This script uploads both datasets to a microsoft azure storage account

import azure.storage.blob as blob
import os

try:
    
    # make a connection the the azure account
    f = open('C:/Users/Gregory Beauclair/Documents/GitHub/project-Milksoplimit/src/upload/azureKey.config')
    contents = f.readlines()
    accountURL = "https://{}.blob.core.windows.net/".format(contents[0])

    # make a new container
    service = blob.BlobServiceClient(account_url=accountURL, credential=contents[1])
    containerName = "final-project"
    containerClient = service.get_container_client(containerName)

    #'''
    containerClient.create_container()
    #'''
    
    #'''
    # upload dataset1 inside loop
    directory = 'C:/Users/Gregory Beauclair/Documents/GitHub/project-Milksoplimit/dataset1/'
    for file in os.listdir(directory):
        f = os.path.join(directory, file)
        if os.path.isfile(f):
            blobName = file
            blobClient = containerClient.get_blob_client(blob = blobName)
            fileinner = open(f, 'rb')
            blobClient.upload_blob(fileinner)
    #'''
    
    #'''
    # upload dataset2 inside loop
    directory = 'C:/Users/Gregory Beauclair/Documents/GitHub/project-Milksoplimit/dataset2/'
    for file in os.listdir(directory):
        f = os.path.join(directory, file)
        if os.path.isfile(f):
            blobName = file
            blobClient = containerClient.get_blob_client(blob = blobName)
            fileinner = open(f, 'rb')
            blobClient.upload_blob(fileinner)
    #'''
    
# exception handling to know what went wrong if anything does
except Exception as e:
    print(e)
