# DE Zoomcamp 2023 Learning Week 4: Analytics Engineering with dbt
The goal of DE zoomcamp this week is to transform the data loaded in data warehouse to analytical views.

## Prerequisites: 
A running Data Warehouse (BigQuery)
A set of running pipelines ingesting the project dataset: Yellow, Green taxi data year of 2019–2020 and fhv data year 2019 (data source: https://github.com/DataTalksClub/nyc-tlc-data/releases)

## Setting up dbt for using BigQuery
- Create a dbt cloud account here https://www.getdbt.com/signup/ 

- Create a service-account key with specific roles: `BigQuery Data Editor`, `BigQuery Job User`, `BigQuery User`, or `BigQuery Admin`.

- Create a dbt cloud project, then follow steps here: https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_4_analytics_engineering/dbt_cloud_setup.md

- Create a deploy key in our github repository

