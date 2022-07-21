# Sparkify ETL with postgres
This project is part of Udacity Data Engineering Nanodegree

## Introduction
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

## Objective
Defining fact and dimension tables for a star schema for a particular analytic focus, and writing an ETL pipeline that transfers data from files in two local directories into these tables in Postgres using Python and SQL.

## Project Structure
- **data/** folder nested at the home of the project, where all needed jsons reside.
- **sql_queries.py** contains all your sql queries, and is imported into the files bellow.
- **create_tables.py** drops and creates tables. You run this file to reset your tables before each time you run your ETL scripts.
- **test.ipynb** displays the first few rows of each table to let you check your database.
- **etl.ipynb** reads and processes a single file from song data and log data and loads the data into your tables.
- **etl.py** reads and processes files from song data and log data and loads them into your tables.
- **README.md** current file, provides discussion on my project.

## Data Modeling
### Fact Table
```
Songplays:
    songplay_id PRIMARY KEY,
    start_time TIMESTAMP NOT NULL Foriegn key to time(start_time),
    user_id INT NOT NULL Foriegn key to users(user_id),
    level VARCHAR,
    song_id VARCHAR Foriegn key to songs(song_id),
    artist_id VARCHAR Foriegn key to artists(artist_id),
    session_id INT,
    location TEXT,
```
### Dimension Table
```
Users:
    user_id INT PRIMARY KEY,
    first_name VARCHAR NOT NULL,
    last_name VARCHAR NOT NULL,
    gender CHAR(1) NOT NULL CHECK (gender = 'F' OR gender='M'),
    level VARCHAR NOT NULL
```

```
Songs:
    song_id VARCHAR PRIMARY KEY,
    title VARCHAR NOT NULL,
    artist_id VARCHAR NOT NULL Foriegn key to artists(artist_id),
    year INT NOT NULL,
    duration FLOAT NOT NULL
```
```
Artists:
    artist_id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    location TEXT NOT NULL,
    latitude FLOAT NOT NULL,
    longitude FLOAT NOT NULL
```
```
Time:
    start_time TIMESTAMP PRIMARY KEY,
    hour INT NOT NULL,
    day INT NOT NULL,
    week INT NOT NULL,
    month INT NOT NULL,
    year INT NOT NULL,
    weekday VARCHAR NOT NULL
```
## Prerequisites

These are the prerequisites to run the program.
```
- python 3.7
- PostgreSQL
- psycopg2
```
## How to run
Follow the steps to extract and load the data into the data model.

2. Run `create_tables.py` to create/reset the tables by

   ```python
   python create_tables.py
   ```

3. Run ETL process and load data into database by 

   ```python
   python etl.py
   ```

4. Check whether the data has been loaded into database by executing queries in `test.ipynb`
