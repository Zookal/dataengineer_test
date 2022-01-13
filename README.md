# Data Engineer Interview Test

## Tools and technologies
- [Meltano](https://meltano.com/) - very convenient open source tool for building pipelines with:
  - [Singer](https://www.singer.io/) taps and targets - ready-to-use extract and load scripts
  - [dbt](https://www.getdbt.com/product/what-is-dbt/) - transform data with simple SELECT statements and Jinja templating
  - Apache Airflow
- [Apache Superset](https://superset.apache.org/) - "open source Tableau" (also comes with a useful SQL editor for ad hoc queries)
- PostgreSQL
- Docker and docker-compose

## ETL (ELT, rather)
1. The data for this exercise can be found on the `data.zip` file. Can you describe the file format?
    - They seem to be flat files delimited by pipe characters. Each line has a trailing pipe.

- **Super Bonus**: generate your own data through the instructions on the encoded file `bonus_etl_data_gen.txt`.
To get the bonus points, please encode the file with the instructions used to generate the files.
  - [Done](https://github.com/mkdlt/dataengineer_test/blob/master/bonus_etl_data_gen_answer.txt) ✅. File was encoded in base64.

2. Code your scripts to load the data into a database.
    - Meltano, building on Singer, makes this very simple. Most of the work goes into [configuration](https://github.com/mkdlt/dataengineer_test/blob/master/meltano/meltano.yml).
```
$ meltano add extractor tap-spreadsheets-anywhere
$ meltano add loader target-postgres --variant meltano
$ meltano elt tap-spreadsheets-anywhere target-postgres
```
3. Design a star schema model which the data should follow.
 
![Star schema](star_schema_erd.png)

4. Build your process to load the data into the star schema
    - See [this directory](https://github.com/mkdlt/dataengineer_test/tree/master/meltano/transform/models/star). Again, dbt makes the transform step relatively painless.
```
$ meltano add transformer dbt
$ meltano elt tap-spreadsheets-anywhere target-postgres --transform=run
```

- **Bonus** points: 
  - add a field to classify the customer account balance in 3 groups. [Done](https://github.com/mkdlt/dataengineer_test/blob/master/meltano/transform/models/star/dim_customer.sql) ✅
  - add revenue per line item. [Done](https://github.com/mkdlt/dataengineer_test/blob/master/meltano/transform/models/star/fct_lineitem.sql) ✅
  - convert the dates to be distributed over the last 2 years. (to do)

5. How to schedule this process to run multiple times per day?
    - Meltano makes this very easy once again
```
meltano schedule tbl_to_postgres tap-spreadsheets-anywhere target-postgres --transform=run @hourly
```
 
- **Bonus**: What to do if the data arrives in random order and times via streaming?
  - If we care about real-time/event-driven analytics, we'll have to switch to stream processing tools. So far I've only used the AWS offerings (Lambda, the Kinesis suite), but not extensively.
  - That said, real-time data production doesn't always require real-time data processing. If batch processing will suffice for the client's needs, there's no need to switch.

6. How to deploy this code?
    - Ideally there should be a CI/CD pipeline that builds a Docker image on each push, uploads it to a container repository, and pulls it down to some container orchestration solution like ECS or EKS. The warehouse should probably be on something like Redshift or Snowflake instead of Postgres; I suspect switching tap-postgres for something else would already take you halfway there.

- **Bonus**: Can you make it to run on a container like process (Docker)? (to do)

## Data reporting
1. [What are the top 5 nations in terms of revenue?](https://github.com/mkdlt/dataengineer_test/blob/master/meltano/transform/models/reporting/rpt_top_countries_by_revenue.sql)
2. [From the top 5 nations, what is the most common shipping mode?](https://github.com/mkdlt/dataengineer_test/blob/master/meltano/transform/models/reporting/rpt_top_shipping_modes_in_top_countries.sql)
    - I interpreted this as "For *each* of the top 5 nations, what is the most common shipping mode?" though I recognize it could mean "Only counting the top 5 nations, what is the most common shipping mode?"
3. [What are the top selling months?](https://github.com/mkdlt/dataengineer_test/blob/master/meltano/transform/models/reporting/rpt_top_selling_months.sql)
4. [Who are the top customer in terms of revenue and/or quantity?](https://github.com/mkdlt/dataengineer_test/blob/master/meltano/transform/models/reporting/rpt_top_customers_by_revenue.sql)
5. [Compare the sales revenue of on current period against previous period?](https://github.com/mkdlt/dataengineer_test/blob/master/meltano/transform/models/reporting/rpt_current_vs_previous_revenue.sql)
    - Assumed any period would suffice so I just went with a month to month comparison.

## Data profiling
Considering we're already using dbt, something like [dbt_profiler](https://hub.getdbt.com/data-mie/dbt_profiler/latest/) would probably best suit our needs. I've also heard good things about [Great Expectattions](https://greatexpectations.io/). For datasets as small as the one tackled in this project, though, something like [pandas_profiling](https://pandas-profiling.github.io/pandas-profiling/docs/master/rtd/) would probably suffice.

## Architecture
I think Meltano makes a solid case for Singer + dbt + Airflow for small to mid-sized business cases. Like I said, ideally there would be a CI/CD pipeline from code repo to container repo to ECS/EKS, plus testing and monitoring etc. I'm not sure how Singer compares to ingestion based on Spark when it comes to much bigger workloads, so maybe that's the first component to be reevaluated.
