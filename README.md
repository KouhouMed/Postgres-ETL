# Postgres ETL Project

## Overview
This project implements an ETL (Extract, Transform, Load) pipeline using Python and PostgreSQL for a music streaming app, Sparkify. It processes song and user activity data, transforming it into a set of dimensional tables for easier querying and analysis.

## Project Structure
- `etl.py`: Contains the ETL processes to read JSON logs on user activity and JSON metadata on songs, and load the data into PostgreSQL tables.
- `create_tables.py`: Drops and creates the database and tables.
- `sql_queries.py`: Contains all the SQL queries used in the ETL process.

## Database Schema
The database uses a star schema optimized for queries on song play analysis. This includes the following tables:

### Fact Table
1. **songplays** - records in log data associated with song plays

### Dimension Tables
2. **users** - users in the app
3. **songs** - songs in music database
4. **artists** - artists in music database
5. **time** - timestamps of records in songplays broken down into specific units

## Requirements
- Python 3.x
- PostgreSQL
- psycopg2
- pandas

## Dataset
The project uses two datasets:
1. **Song Dataset**: JSON files containing metadata about songs and artists.
2. **Log Dataset**: JSON files containing user activity logs from the music streaming app.

## ETL Pipeline
1. Process song data to populate the songs and artists tables.
2. Process log data to populate the time and users tables.
3. Use data from both song and log datasets to populate the songplays fact table.
