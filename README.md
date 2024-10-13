# Retail_IOT_Project_Stream_Databricks_Azure

our solution for Real-Time Retail-Margin-Improvement Analytics to improve in-store operations by:  

-Rapidly ingesting all data sources and types at scale 

-Building highly scalable streaming data pipelines with Delta Live Tables to obtain a real-time view of your operation. 

-Leveraging real-time insights to tackle your most pressing in-store information needs. 

                                  Project Architecture
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

Blob Storage Account(Simulated Files):
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

  
Step 4 In a table, load the Kafka data and persist. 
Now let's look at the data from our inventory change event. These are the contents of a JSON document that a retailer sent out summarizing an occurrence that was relevant to the inventory. These occurrences could be sales, restocks, or reports of theft, damage, or loss (shrinkage). Bopis, the fourth event category, denotes a sales transaction that occurs in the online store but is handled by a physical store. Each of these occurrences is sent as a single, combined stream: 

We create a function that returns a Spark dataframe in the same manner as before, then add the necessary components to it. Using patterns from Spark Structured Streaming, the dataframe is defined. The reason we configure the connection using Kafka attributes is that the dataframe is streaming data from the Azure IOT Hub's Kafka endpoint. The dataframe that is read from the IOT Hub has a pre-defined structure because it is a Kafka data source, meaning that no schema needs to be added. To ensure that we don't use up all of the resources allocated for stream processing, the maxOffsetsPerTrigger configuration parameter caps the amount of messages read from the IOT Hub in a single cycle and create inventory_change table. 

Step 5: Snapshots of the Stream Inventory 
We get counts of the inventory items at a specific store location on a regular basis. Retailers often use these inventory snapshots to refresh their knowledge of what products are truly in stock, especially in light of questions over the accuracy of computed inventory amounts. We might want to hold onto the most recent counts for every product at every store location as well as the complete history of inventory snapshots that we have received. As soon as this one data source enters our environment, two different tables are created from it to satisfy this requirement. 

These inventory snapshot data are a little erratic in arriving in this context as CSV files. However, we will want to process them as soon as they arrive so that they may be used to support estimations that are more accurate.However, as soon as they land, we'll want to process them and make them accessible so that we can enable more precise estimations of the existing inventory. 

Step 6: copy all files in blob storage to azure adls gen 2
Use Azure Data Factory (ADF) to copy all files from Azure Blob Storage to Azure Data Lake Storage Gen2 by following these steps:

Create a Pipeline in ADF: To control the flow of data, create a new pipeline.

ForEach Activity: To go across every folder in the Blob Storage, use this. The list of directories is iterated over in this activity.
<img width="527" alt="image" src="https://github.com/user-attachments/assets/12db9122-89d0-4bf4-9078-96285ed9b3be">

<img width="522" alt="image" src="https://github.com/user-attachments/assets/c60b2911-436d-4f56-ab3f-91d8a1b38f99">



Copy Activity: Place the Copy Activity inside each of the activities. Set it up to copy files to the ADLS Gen2 destination from the Blob Storage folder.

<img width="524" alt="image" src="https://github.com/user-attachments/assets/98544f96-83b9-4249-bbb0-bdc0172c6950">


Source Dataset: Configure the dataset's source to point to Blob Storage. For each cycle, use dynamic expressions to fetch files.

Sink Dataset: Assign the ADLS Gen2 location to the sink dataset.

Triggers: Arrange for the pipeline to be triggered as needed

This technique automates the mass copying of files across cloud storage platforms by utilizing ADF's intuitive loop through directory support and handling of numerous datasets.

<img width="647" alt="image" src="https://github.com/user-attachments/assets/dc1417ad-f2dc-4168-8f29-0a93e44751d5">


 
