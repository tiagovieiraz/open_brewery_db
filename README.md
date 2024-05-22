# open_brewery_db
Data Engineering Case Repo - Open Brewery db

## Project Tools and Details
Orchestration: Airflow <br/>
Data Lake: Databricks Community <br/>
Language: PySpark, SparkSQL <br/>

This project was developed mainly on Databricks Community Edition.

To orchestrate the data pipeline, I decided to develop an Airflow Dag that will trigger the Databricks Job containing the bronze and silver notebook.
Since the Databricks Community Edition does not allow users to create a job, I decided to just exemplify how this data pipeline would work.

These are the notebooks and airflow dag:

bronze_open_brewery_db_api: notebook that get the API data and create the bronze layer <br/>
silver_open_brewery_db_api: notebook that create the silver layer, partioning by location <br/>
gold_open_brewery_db_api: notebook that create the aggregated view. <br/>
dag_airflow_open_brewery_db_api: dag that will trigger the databricks job. 
