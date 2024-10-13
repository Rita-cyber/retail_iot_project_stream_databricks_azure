# Retail_IOT_Project_Stream_Databricks_Azure

our solution for Real-Time Retail-Margin-Improvement Analytics to improve in-store operations by:  

-Rapidly ingesting all data sources and types at scale 

-Building highly scalable streaming data pipelines with Delta Live Tables to obtain a real-time view of your operation. 

-Leveraging real-time insights to tackle your most pressing in-store information needs. 

                                 # Project Architecture
<img width="629" alt="image" src="https://github.com/user-attachments/assets/be1ae1e9-b6ad-433d-b4c9-b7196c58d7b9">


 

Tech Stack: - 

✔️Blob Storage Account 

✔️Azure Data Factory for Batch Processing 

✔️Azure Key Vault 

✔️Azure Data Lake Storage Gen2 

✔️Azure IoT Hub 

✔️Azure Databricks for Transformations 

✔️Delta Live Table Implementation 

✔️Medallion Architecture Implementation 

This project shows how Delta Live Tables may be used to calculate current stocks for different products across numerous shop locations and build a lakehouse design in almost real-time. The technique divides the logic into two steps rather than going straight from the raw ingested data to the calculated inventory. 

In the first step, which is called Bronze-to-Silver ETL in the diagram, ingested data is modified to make it more accessible. The deconstruction of nested arrays, the deduplication of records, and other operations performed on the data at this step are not meant to apply any business-driven interpretations to the data. The Silver layer of our lakehouse medallion architecture is represented by the tables that were written to in this phase. 

Using the silver tables, we derive our business-aligned output and computed current-state inventory in the second stage (shown in the figure as Silver-to-Gold ETL). Our architecture's Gold layer is represented by the table to which these data are written. 

Simulated Files:
| File Type    | Files                                 |  Path                                                                       |
|--------------|---------------------------------------|-----------------------------------------------------------------------------|
| Change_Event|inventory_change_store001.txt           | /mnt/pcupos/pos/generator/inventory_change_store001.txt                     |
| Change_Event|inventory_change_online.txt             | /mnt/pcupos/pos/generator/inventory_change_online.txt                       |
| Snapshot    |inventory_snapshot_store001.txt         | /mnt/pcupos/pos/generator/inventory_snapshot_store001.txt                   |
| Snapshot    |inventory_snapshot_online.txt           | /mnt/pcupos/pos/generator/inventory_snapshot_online.txt                     |
| Static      |store.txt                               | /mnt/pcupos/pos/static_data/store.txt                                       |
| Static      |item.txt                                | /mnt/pcupos/pos/static_data/item.txt                                        |
| Static      |inventory_change_type.txt               | /mnt/pcupos/pos/static_data/inventory_change_type.txt                       |

Step 1a: Setup the Azure IOT Hub 

To setup and configure the Azure IOT Hub, you will need to: 

Create an Azure IOT Hub. Use Standard tier. 

Add a Device and retrieve the Device Connection String. We used a device with Symmetric key authentication and auto-generated keys enabled to connect to the IOT Hub. 

Retrieve the Azure IOT Hub's Event Hub Endpoint Compatible Endpoint property. 

Set Azure IOT Hub relevant configuration values in the notebook. You can set up a secret scope to manage credentials used in notebooks. 

Step 1b: Setup the Azure Storage Account 

To setup and configure the Azure Storage account, you will need to: 

Create an Azure Storage Account. 

Create a Blob Storage container. 

Retrieve an Account Access Key & Connection String. 

Set Azure Storage Account relevant configuration values in the cell below. You can set up a secret scope to manage credentials used in notebooks. 

 

Create below containers or folders 

 

Container:- 

pcupos 

Folders:- 

pos/generator 

pos/static_data 

pos/inventory_snapshots 

 Step3:- Load the Static Reference Data 

While we've given attention in this and other notebooks to the fact that we are receiving streaming event data and periodic snapshots, we also have reference data we need to access. These data are relatively stable so that we might update them just once daily. 

 

Store 

Schema:- store_schema = StructType([ 

  StructField('store_id', IntegerType()), 

  StructField('name', StringType()) 

  ]) 

 

Items 

item_schema = StructType([ 

  StructField('item_id', IntegerType()), 

  StructField('name', StringType()), 

  StructField('supplier_id', IntegerType()), 

  StructField('safety_stock_quantity', IntegerType()) 

  ]) 

Change_type 

change_type_schema = StructType([ 

  StructField('change_type_id', IntegerType()), 

  StructField('change_type', StringType()) 

  ]) 

  

 
