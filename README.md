# Data Engineer Interview Test

## Tools and technologies
- [Meltano](https://meltano.com/) - very convenient open source tool for building pipelines with:
  - [Singer](https://www.singer.io/) taps and targets - ready-to-use extract and load scripts
  - [dbt](https://www.getdbt.com/product/what-is-dbt/) - transform data with simple SELECT statements and Jinja templating
  - Apache Airflow - the one and only
- [Apache Superset](https://superset.apache.org/) - "open source Tableau" (also comes with a useful SQL editor for ad hoc queries)
- PostgreSQL
- Docker and docker-compose

## ETL (ELT, really)
>1. The data for this exercise can be found on the `data.zip` file. Can you describe the file format?

They seem to be typical flat files delimited by pipe characters. Extra pipe at the end of each line.

>**Super Bonus**: generate your own data through the instructions on the encoded file `bonus_etl_data_gen.txt`.
To get the bonus points, please encoded the file with the instructions were used to generate the files.

**Done ✅**. File was encoded in base64.

>2. Code you scripts to load the data into a database.

>3. Design a star schema model which the data should flow.

>4. Build your process to load the data into the star schema 

>**Bonus** point: 
>- add a fields to classify the customer account balance in 3 groups 
>- add revenue per line item 
>- convert the dates to be distributed over the last 2 years

>5. How to schedule this process to run multiple times per day?
 
>**Bonus**: What to do if the data arrives in random order and times via streaming?

>6. How to deploy this code?

>**Bonus**: Can you make it to run on a container like process (Docker)? 
