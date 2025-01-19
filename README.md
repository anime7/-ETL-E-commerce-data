# E-commerce Data ETL and Analysis

This project performs ETL (Extract, Transform, Load) on a Kaggle e-commerce dataset and uses SQL queries to answer key business questions.

## Project Overview

This project cleans, transforms, and loads Brazilian e-commerce data into a PostgreSQL database for analysis. A Python script handles the ETL process, and SQL queries provide insights.

## Data Sources and Destination

* **Source:** [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
* **Destination:** PostgreSQL database

## Data Transformations

* **Handling Missing Values:** Missing values are filled or rows dropped strategically (see `complete_updated.ipynb` for details).
* **Removing Duplicates:** Duplicate rows are removed from `geolocation_df`.
* **Data Type Conversion:** Date/time columns are converted to the appropriate format.

## Business Questions

1. Analyze sales and revenue trends over time (daily, weekly, monthly).
2. Identify the best-selling products by revenue or quantity.
3. Analyze sales and revenue by customer location (city and state).
4. Calculate the average order value (overall and by location).
5. Determine the average customer rating (overall and by product/category).
6. Analyze seller performance (sales and ratings).


## Project Structure

* `data/`: Contains original and transformed data (transformed data might be excluded due to size).
* `original_data/`: Contains the original Kaggle CSV files.
* `Result_csv/`: Contains results of SQL queries (CSV format).
* `scripts/`: Contains the Python ETL script and SQL scripts.
    * `complete_updated.ipynb`: Jupyter Notebook for ETL.
    * `TABLE.sql`: SQL script for table creation and queries.


## Running the Project

1. **Clone the repository:**
   ```bash
   git clone https://github.com/anime7/-ETL-E-commerce-data.git
