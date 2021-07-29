# Retail ETL

In this project I used Spark which was installed in localhost where Airflow also resides,
but ideally we can use Azure Databricks or AWS EMR in the production environment for Spark,
and Airflow ....

#### TODO:
* Create a wait script entrypoint for postgresql and airflow web server 

#### Dev Notes:
* Add Spark Kafka option: exactly once
* Add execution datetime column in SQLite for batch parsing
* Do .tbl -> SQLite? or .tbl -> Kafka? or SQLite -> Kafka?
* Add Travis CI