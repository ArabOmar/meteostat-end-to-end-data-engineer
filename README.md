# End-to-End Data Engineering Project for Weather Data

### Introduction
This project demonstrates an end-to-end data engineering pipeline that fetches meteorological data using an external API. The pipeline leverages AWS technologies such as Lambda for serverless computing, CloudWatch for monitoring and logging, AWS Glue for data transformation, and Athena for querying the processed data. This solution provides automated data collection, storage, transformation, and querying capabilities, showcasing the power of cloud-native tools for building scalable and efficient data pipelines.


### Architecture
![Architecture Diagram](https://github.com/ArabOmar/meteostat-end-to-end-data-engineer/blob/main/meteostat-project.png)


### Services Used
1. **Amazon S3** is a highly scalable and secure cloud storage solution for storing and retrieving data like files, images, and backups. It offers durability, easy access from anywhere, data encryption, and seamless integration with other AWS services for efficient data management.

2. **AWS Lambda** is a serverless computing service that lets you run code in response to events without provisioning or managing servers. It automatically scales based on demand, providing a cost-effective and efficient way to execute applications in real time.

3.  **Amazon CloudWatch** is a monitoring and observability service for AWS resources and applications. It collects and tracks metrics, logs, and events, helping you monitor the health and performance of your systems, and set alarms for automatic responses to issues.

4. ** AWS Glue Crawler** automatically discovers and catalogs data from different sources in your AWS environment. It identifies the data's structure and schema, making it easier to query and analyze by organizing it into a centralized data catalog.

5. **AWS Glue Data Catalog** is a central repository that stores metadata about your data, such as its schema, location, and format. It enables seamless discovery, management, and access to datasets across AWS services like Athena, Redshift, and EMR. It helps streamline ETL processes and ensures consistent, organized data for analytics.

6. **Amazon Athena** is an interactive query service that lets you analyze data directly in Amazon S3 using SQL. It is serverless, so there’s no infrastructure to manage, and you pay only for the queries you run, making it a cost-effective option for querying large datasets.

For more details, visit the [AWS Documentation](https://aws.amazon.com/documentation/).


### Python Packages Used

1. **asyncio**: A library to write concurrent code using async/await syntax. It is used for writing asynchronous programs, especially in I/O-bound tasks.
  
2. **httpx**: A fast and async-compatible HTTP client for Python, designed for making HTTP requests. It supports synchronous and asynchronous operations, often used alongside `asyncio`.

3. **boto3**: The AWS SDK for Python. It allows Python developers to write software that makes use of Amazon services like S3, Lambda, and EC2.

4. **pandas**: A powerful data manipulation and analysis library. It provides data structures like DataFrame and Series, which are ideal for handling structured data.
  
5. **datetime**: A module that supplies classes for manipulating dates and times. It allows easy handling and formatting of date and time objects.

6. **json**: A module for working with JSON (JavaScript Object Notation) data. It supports parsing and generating JSON data in Python.

7. **StringIO**: A module from the `io` library that provides an in-memory file-like object. It allows you to read and write strings as if they were files, useful for handling text data in memory.


### About Dataset/API

The **Meteostat API** provides access to historical weather data, offering information like temperature, precipitation, wind speed, and more. You can retrieve data for specific locations and time periods, enabling you to integrate weather data into your applications for analysis or forecasting.

- **Data Access:** Historical weather data (temperature, precipitation, wind, etc.)
- **API Endpoints:** Get data by location (stations), date range, and specific weather parameters.
- **Formats:** Returns data in JSON format for easy integration.
- **Usage:** Great for weather analysis, climate research, or app development.

For more details, visit the [Meteostat API documentation](https://meteostat.net/en/docs).

### Project Execution Flow  

1. **AWS Lambda Layers for Dependencies**  
- Created **Lambda layers** for httpx (for API requests) and pandas (for data processing).  
- These layers ensure efficient dependency management and reduce package size in Lambda functions.  

2. **Data Extraction**
- Implement an extraction function using AWS Lambda:
  - Extract metadata of weather stations near Nîmes, France, and metric data from nearby stations using Meteostat API.  
  - Uses extract_api_1.py for station metadata and extract_api_2.py for metric data retrieval.  
  - Store extracted raw data in an **S3 bucket** under a dedicated folder for organization.
    
3. **Automated Data Collection with AWS Lambda & CloudWatch**  
- Set up **AWS Lambda** functions triggered by **Amazon CloudWatch**:  
  - **Metadata extraction:** Runs **monthly**.  
  - **Metric data extraction:** Runs **hourly**.  

4. **Data Transformation**  
- Implement a transformation function using AWS Lambda:  
  - Cleans, processes, and keeps only **essential** data.  
  - Uses transform_api_1.py for metadata and transform_api_2.py for metric data.
  - Load transformed data into a **processed folder** in S3.  
  - Follows a **/year/month/day** pattern for partitioning, enabling optimized querying.  

5. **Automating Transformation Trigger**  
- Set up an event-based trigger to **automatically execute the transformation function** when new raw data is added to S3.  

6. **Cleaning Up Old Raw Data**  
- Create a scheduled Lambda function to **delete unnecessary raw data** each month.  
- Prevents excessive storage consumption by removing outdated files.  

7. **Cataloging & Querying with AWS Glue & Athena**  
- Create an **AWS Glue Crawler** to scan and catalog processed data.  
- Use **Amazon Athena** for querying and analyzing the cleaned, structured data efficiently.  
