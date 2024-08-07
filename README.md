# National-Highways-Road-Traffic-Analysis-Project

# national-highways
## first Scenario batch data pipeline using airflow
**data engineering** project that aims to de-congest the national highways by analyzing the road traffic data from different toll plazas.
Each highway is operated by a different toll operator with a different IT setup that uses different file formats 
my job is to collect data available in different formats and consolidate it into a single file.

using airflow  and pandas in this project I did this :
* Extracted data from a csv file
* Extracted data from a tsv file
* Extracted data from a fixed width file
* Transformed the data
* Loaded the transformed data into the staging area

the file of dag I used for this Scenario insid dags/finalassignment 

## second Scenario streaming data pipeline using kafka and mysql 
data engineer project that aims to de-congest the national highways by analyzing the road traffic data from different toll plazas. As a vehicle passes a toll plaza, the vehicle’s data like vehicle_id,vehicle_type,toll_plaza_id and timestamp are streamed to **Kafka** . 
Your job is to create a data pipe line that collects the streaming data and loads it into a database.

In this Scenario I will create a streaming data pipe by performing these steps:

* Create a topic named toll in kafka.
* create the generator program to stream to toll topic.
* create the consumer program to write into a MySQL database table.
* Verify that streamed data is being collected in the database table.
