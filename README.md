# E-commerce Data ETL and Analysis

This project performs ETL (Extract, Transform, Load) on a Kaggle e-commerce dataset and then uses SQL queries to answer key business questions.

## Project Overview

The project aims to clean, transform, and load e-commerce data into a PostgreSQL database, enabling efficient analysis and reporting.  A Python script handles the data extraction, cleaning, transformation, and loading processes. SQL queries are then used to extract insights and answer specific business questions.

## Data Sources and Destination

* **Source:** Kaggle e-commerce dataset (CSV files).  The dataset includes information on customers, orders, products, sellers, geolocation, payments, and reviews.  [Provide a link to the Kaggle dataset if it's publicly available].
* **Destination:** PostgreSQL database.  The following tables are created in the database:
    * `customer_df`
    * `geolocation_df`
    * `order_items_df`
    * `order_payments_df`
    * `order_reviews_df`
    * `order_orders_df`
    * `products_df`
    * `sellers_df`
    * `product_category_df`

## Data Transformations

The Python ETL script performs the following transformations:

* **Handling Missing Values:**
    * `order_reviews_df`: Missing values in `review_comment_title` and `review_comment_message` are filled with "No comment".
    * `order_orders_df`: Rows with missing values in `order_approved_at`, `order_delivered_carrier_date`, or `order_delivered_customer_date` are dropped.
    * `products_df`: Missing categorical values are filled with "unknown," and missing numerical values are filled with the mean of the respective column.
* **Removing Duplicates:** Duplicate rows are removed from `geolocation_df`.
* **Data Type Conversion:** Date and time columns are converted to the appropriate datetime format.

## Business Questions

[Please provide the specific business questions the project aims to answer.  Be as precise as possible. Examples:]

* What are the top 5 selling product categories by revenue?
* Which customer segment has the highest average order value?
* How does order volume vary by month and year?
* What is the average customer rating by product category?

## Project Structure

* `data/`: Contains the original Kaggle CSV files and potentially transformed data files (although these might not be committed to the repository due to size).
* `original_data/`: Contains the original Kaggle CSV data files.
* `Result_csv/`:  Contains the results of the SQL queries used to answer the business questions, saved as CSV files.
* `scripts/`: Contains the Python ETL script and the SQL script.
    * `complete_updated.ipynb`: The main Jupyter Notebook that performs the ETL process.
    * `TABLE.sql`: SQL script to create tables (and optionally contains the SQL queries to answer the business questions).  [If the business questions are answered in a separate .sql file or within the Jupyter Notebook, clarify this here].



## Running the Project

1. **Clone the repository:**
   ```bash
   git clone https://github.com/anime7/-ETL-E-commerce-data.git
