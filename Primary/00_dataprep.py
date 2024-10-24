# Databricks notebook source
!pip --disable-pip-version-check install --upgrade --force-reinstall -r https://aka.ms/automl_linux_requirements.txt -vvv
!pip install --upgrade --no-deps --force-reinstall mlflow==2.8.1
!pip install azureml-train-automl-runtime
!pip install -U azureml-fsspec mltable

# COMMAND ----------

!pip install --upgrade pandas==1.1.5

# COMMAND ----------

#%python
import azureml.core
from azureml.core.runconfig import JarLibrary
from azureml.core.compute import ComputeTarget, DatabricksCompute
from azureml.exceptions import ComputeTargetException
from azureml.core import Workspace, Experiment
from azureml.pipeline.core import Pipeline, PipelineData
from azureml.pipeline.steps import DatabricksStep
from azureml.core.datastore import Datastore
from azureml.data.data_reference import DataReference
import os
# Check core SDK version number
print("SDK version:", azureml.core.VERSION)

from azureml.core import Workspace


import logging
import os
import random
import time
import json

from matplotlib import pyplot as plt
from matplotlib.pyplot import imshow
import numpy as np
import pandas as pd

import azureml.core
from azureml.core.experiment import Experiment
from azureml.core.workspace import Workspace
from azureml.train.automl import AutoMLConfig
from azureml.train.automl.run import AutoMLRun
from azureml.core import Workspace, Dataset

# COMMAND ----------

from azureml.core.authentication import InteractiveLoginAuthentication
cli_auth = InteractiveLoginAuthentication(tenant_id="5f9dc6bd-f38a-454a-864c-c803691193c5")

# COMMAND ----------

subscription_id = "07e5d557-3ee7-40b3-8b57-43d9748402fc"
resource_group = "rg-poc-scpoc-dev"
workspace_name = "ml-cbcert-dev" 
workspace_region = "eastus2"
ws = Workspace(subscription_id, resource_group, workspace_name)

ws = Workspace.create(name = workspace_name,
                      subscription_id = subscription_id,
                      resource_group = resource_group, 
                      location = workspace_region,                      
                      exist_ok=True)
ws.get_details()
!mkdir /databricks/driver/
ws.write_config(path="/databricks/driver/", file_name="config.json")
print("workspace created")

# COMMAND ----------

# Choose a name for the experiment and specify the project folder.
experiment_name = 'automl-local-classification_saurabh_5feb'

experiment = Experiment(ws, experiment_name)

output = {}
output['SDK version'] = azureml.core.VERSION
output['Subscription ID'] = ws.subscription_id
output['Workspace Name'] = ws.name
output['Resource Group'] = ws.resource_group
output['Location'] = ws.location
output['Experiment Name'] = experiment.name
pd.set_option('display.max_colwidth', -1)
pd.DataFrame(data = output, index = ['']).T

# COMMAND ----------

file_path = '/FileStore/s590632/Airlines.csv'
dataset = spark.read.csv(file_path, header=True, inferSchema=True)
dataset.count()

# COMMAND ----------

dataset=dataset.drop('_c0','id')

# COMMAND ----------

train_df, test_df = dataset.randomSplit(weights=[0.8,0.2], seed=100)

# COMMAND ----------

train_df= train_df.limit(4999)
test_df= test_df.limit(4999)

# COMMAND ----------

print((train_df.count(), len(train_df.columns)))

# COMMAND ----------

print((test_df.count(), len(test_df.columns)))

# COMMAND ----------


# Connect to the Workspace
ws = Workspace.from_config()

# The default datastore is a blob storage container where datasets are stored
datastore = ws.get_default_datastore()

# Register the dataset
ds = Dataset.Tabular.register_spark_dataframe(
        dataframe=train_df, 
        name='airlines_train_dataset_feb5_5k', 
        description='airlines delay data',
        target=datastore
    )

# Display information about the dataset
print(ds.name + " v" + str(ds.version) + ' (ID: ' + ds.id + ")")

# COMMAND ----------

test_df = test_df.drop('Delay')

# COMMAND ----------


# Connect to the Workspace
ws = Workspace.from_config()

# The default datastore is a blob storage container where datasets are stored
datastore = ws.get_default_datastore()

# Register the dataset
ds = Dataset.Tabular.register_spark_dataframe(
        dataframe=test_df, 
        name='airlines_test_dataset_feb5_5k', 
        description='airlines delay data',
        target=datastore
    )

# Display information about the dataset
print(ds.name + " v" + str(ds.version) + ' (ID: ' + ds.id + ")")
