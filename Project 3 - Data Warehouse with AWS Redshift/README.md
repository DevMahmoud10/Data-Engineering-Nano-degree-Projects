# Sparkify Data Warehouse ETL with Redshift
This project is part of Udacity Data Engineering Nanodegree

## Introduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

## Objective
Defining fact and dimension tables for a star schema for a particular analytic focus, and writing an ETL pipeline that load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.

## Project Structure
- **sql_queries.py** Contains the SQL queries for staging, schema definition and ETL.
- **create_tables.py** Drops and creates tables on AWS Redshift (Reset the tables).
- **etl.py** Stages and transforms the data from S3 buckets and loads them into tables.
- **dwh.cfg** configuration file for AWS services.
- **README.md** current file, provides discussion on my project.
- **workflow.ipynb** The main workflow script from IaC to verfication queries.

## Data Modeling
### Fact Table
```
Songplays:
    songplay_id INTEGER IDENTITY(0,1)   NOT NULL SORTKEY,
    start_time  TIMESTAMP               NOT NULL,
    user_id     VARCHAR(50)             NOT NULL DISTKEY,
    level       VARCHAR(10)             NOT NULL,
    song_id     VARCHAR(40)             NOT NULL,
    artist_id   VARCHAR(50)             NOT NULL,
    session_id  VARCHAR(50)             NOT NULL,
    location    VARCHAR(100)            NULL,
    user_agent  VARCHAR(255)            NULL
```
### Dimension Tables
```
Users:
    user_id     INTEGER                 NOT NULL SORTKEY,
    first_name  VARCHAR(50)             NULL,
    last_name   VARCHAR(80)             NULL,
    gender      VARCHAR(10)             NULL,
    level       VARCHAR(10)             NULL
```

```
Songs:
    song_id     VARCHAR(50)             NOT NULL SORTKEY,
    title       VARCHAR(500)           NOT NULL,
    artist_id   VARCHAR(50)             NOT NULL,
    year        INTEGER                 NOT NULL,
    duration    DECIMAL(9)              NOT NULL
```
```
Artists:
    artist_id   VARCHAR(50)             NOT NULL SORTKEY,
    name        VARCHAR(500)           NULL,
    location    VARCHAR(500)           NULL,
    latitude    DECIMAL(9)              NULL,
    longitude   DECIMAL(9)              NULL
```
```
Time:
    start_time  TIMESTAMP               NOT NULL SORTKEY,
    hour        SMALLINT                NULL,
    day         SMALLINT                NULL,
    week        SMALLINT                NULL,
    month       SMALLINT                NULL,
    year        SMALLINT                NULL,
    weekday     SMALLINT                NULL
```
## Prerequisites

These are the prerequisites to run the program.
```
- python 3.7
- PostgreSQL
- psycopg2
- AWS account
```
## How to run
Follow the steps to extract and load the data into the data model.

1. Set AWS services configuration in `dwh.cfg`
2. Run `workflow.ipynb` cell by cell to follow the pipeline.


