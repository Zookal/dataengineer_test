# Retail ETL

In this project I used Spark which was installed in localhost where Airflow also resides,
but ideally we can use Azure Databricks or AWS EMR in the production environment for Spark,
and Airflow ....

#### TODO:
* Create surrogate keys for DW
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
* If you want to run the test suites or just want to contribute/develop,
  please run `pip install -r requirements.txt` first,
  you can also do this inside a new virtual environment.

# Common Errors
* Volume data not found by docker-compose
    * Make sure you already ran the data_gen.txt command.
    * docker system prune -a
* pytest throws docker.errors.APIError
    * This is due to unstable network connection with the docker server,
      try running again the test again.
    * Make sure airflow and other dependent services are down by running:
    `docker-compose down`, then run the test again
      