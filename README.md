# Commodity Trades - Big Data Project

This project analyzes a global commodity trade dataset containing transactions over the past 30 years. The dataset is used to explore key insights into international trade, such as transaction counts, commodity price averages, and flow types across different countries.

## Available Versions
- **Hadoop (Java)**: Distributed processing using Hadoop MapReduce.
- **Apache Spark (Python)**: Fast, in-memory processing using Apache Spark.
- **SparkSQL (Python & SQL)**: SQL-based querying using Apache Spark for efficient data analysis.

## Dataset Source
The dataset used in this project can be downloaded from the following link:
[Download the dataset](https://jpbarddal.github.io/assets/data/bigdata/transactions_amostra.csv.zip)

## Dataset Overview:
- Contains transactions over the last 30 years.
- Data includes commodity details, transaction flow types, quantities, values, and related information.

## Key Queries
The project addresses several important questions regarding the global commodity trade:

1. **Total Transactions Involving Brazil**  
   Count the total number of commodity trade transactions where Brazil is involved.
   
2. **Transactions by Flow Type and Year**  
   Group transactions by flow type and year to identify trends over time.
   
3. **Average Commodity Value Per Year**  
   Calculate the average commodity price for each year.
   
4. **Average Commodity Price per Unit Type, Year, and Category in Export Flow (Brazil)**  
   Calculate the average price of commodities per unit type, year, and category for export transactions involving Brazil.
   
5. **Max, Min, and Mean Transaction Prices Per Unit Type and Year**  
   Compute the maximum, minimum, and mean transaction price for each unit type by year.
   
6. **Country with Largest Average Commodity Price in Export Flow**  
   Identify the country with the highest average commodity price in the export flow.
   
7. **Most Commercialized Commodity in 2016 by Flow Type**  
   Identify the most traded commodity (by quantity) in 2016, broken down by flow type.

## Project Setup

### Prerequisites
Ensure that you have the following tools installed:
- Hadoop (Java) or Apache Spark with Python
- Java Development Kit (JDK) for Hadoop or Python for Spark
- Apache Maven for managing dependencies (if using Hadoop)
- PySpark for running the Spark queries in Python
- Jupyter Notebook for running SparkSQL queries in Python

### Clone the Repository
Clone the repository using the following command:
```bash
git clone https://github.com/ThomasFrentzel/Commodity_Trades
```

