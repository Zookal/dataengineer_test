# Retail ETL

In this project I used Spark which was installed in localhost where Airflow also resides,
but ideally we can use Azure Databricks or AWS EMR in the production environment for Spark,
and Airflow ....

#### TODO:
* Create a wait script entrypoint for postgresql and airflow web server 
* Add Spark Kafka option: exactly once
* Add execution datetime column in SQLite for batch parsing
* Add Travis CI
* Add copy command for ddl.sql in data_gen.txt
* Clean up docker-compose's variables with sensitive information
* Setup Spark in Airflow's connection config [insert image]
* Setup Mysql in Airflow's connection config [insert image]
* Airflow plugins must use airflow variables when referring to a file.

#### Dev Notes:
* All data source tables are created upon docker starts
* Do .tbl -> SQLite? or .tbl -> Kafka? or SQLite -> Kafka?
* You can change the logging level by modifying the AIRFLOW__CORE__LOGGING_LEVEL variable
in the docker-compose.yml file

# Common Errors
* Volume data not found by docker-compose
    * Make sure you already ran the data_gen.txt command.
