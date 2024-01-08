# Modernizing Retail Analytics for Enhanced Customer Insights

## Business Problem:
A retail organization, XYZ Retail, is facing challenges in deriving meaningful insights from their dispersed and unstructured data sources. The lack of a centralized analytics platform hinders their ability to understand customer behavior, optimize inventory, and personalize marketing efforts.

## Project Objective:
Implement a comprehensive data modernization and analytics transformation solution to streamline retail data processing, enabling XYZ Retail to gain actionable insights for strategic decision-making.

## Technical Solution:

### Data Ingestion and Storage:

- Ingest sales, inventory, and customer data into an S3 bucket in real-time using Boto3 Python scripts.
- Configure the S3 bucket to trigger events, initiating Snowpipe for seamless data transfer to Snowflake DBMS.

### Snowflake Schema Architecture:

- Utilize Snowflake's three-schema approach - Bronze, Silver, and Gold - for efficient data processing and analytics.
- The Bronze schema acts as a landing zone for raw data, with an external stage (SOURCE_DATA_CUSTOMER_STAGE) accepting CSV files from the S3 bucket.
### ETL Pipeline:

- Employ a Snowflake pipe (WORK_CUSTOMER_SRC_TO_SL_PIPE) to efficiently load raw data from the S3 stage to the WORK_CUSTOMER_COPY table in the Bronze schema.
- Implement an error integration to capture and report any issues during the ETL process via AWS SNS email notifications.
### Change Data Capture (CDC):

- Leverage a stream (WORK_CUSTOMER_SRC_TO_SL_STREAM) on the WORK_CUSTOMER_COPY table to track real-time changes in customer data.
### Transformation and Integration:

- Design a stored procedure (WORK_CUSTOMER_SRC_TO_SL_SP) to transform streaming data, consolidating it into a session-specific table (WORK_CUSTOMER_XFM) in the Silver Schema.
- Use UPSERT statements to merge the transformed data into the CUSTOMER table in the Silver Schema.
### Scheduled Task for Nightly Updates:

- Implement a nightly task (WORK_CUSTOMER_SRC_TO_SL_TASK) to automatically transform and load any incremental data captured in the stream into the Silver CUSTOMER table.
### DBT Integration for Advanced Analytics:

- Connect DBT to Snowflake (snowflake_dbt) for advanced analytics and reporting.
- Create two DBT models (silver and gold) to perform additional transformations and build views that provide insights into customer behavior and preferences.
### Business Benefits:

- Real-time Customer Insights:

The solution enables XYZ Retail to gain real-time insights into customer behavior, allowing for personalized marketing campaigns and targeted promotions.
- Optimized Inventory Management:

Advanced analytics on inventory data help XYZ Retail optimize stock levels, reducing excess inventory and minimizing stockouts.
- Improved Decision-Making:

Centralized and transformed data in the Silver and Gold schemas empower decision-makers with reliable and up-to-date information, facilitating strategic planning.
- Enhanced Customer Experience:

Personalized marketing efforts and product recommendations based on the analyzed data contribute to an improved overall customer experience.

This business use case highlights how the data modernization and analytics transformation project directly addresses the specific challenges faced by a retail organization, showcasing the impact on business outcomes.






# ETL SCD using Snowflake Tasks, Streams & Stored Procedure

<img width="596" alt="image" src="https://github.com/diyaliza/ETL-Snowflake-AWS/assets/120042912/8beb0dc8-3e8e-4497-91f1-c3dfaba98cc0">

- The project is an ETL demonstration of data that is loaded into S3, which will be transformed into 3 differnt stages of Snowflake DBMS. 
- The data is loaded into S3 using a python scipt using Boto3. 
- The S3 bucket has been configured with an event trigger, when objects are inserted into the bucket. Once the bucket triggers the event with the destination of the snowpipe that is configured in the Snowflake.
- The Snowflake has 3 different schemas - Bronze, Silver and Gold. The bronze schema has a stage - SOURCE_DATA_CUSTOMER_STAGE which accepts the csv files that gets added in the S3 bucket 
- There is a pipe - WORK_CUSTOMER_SRC_TO_SL_PIPE which will take files from the external stage (SOURCE_DATA_CUSTOMER_STAGE) into the table WORK_CUSTOMER_COPY in the Bronze schema. 
- There is also an error integration set up which will track any errors that pops out out of the ETL and sends the information to AWS SNS, where teh subscription is an email service to my personal email. 
- A stream - WORK_CUSTOMER_SRC_TO_SL_STREAM is set up on WORK_CUSTOMER_COPY that tracks when appending is done on the WORK_CUSTOMER_COPY table. 
- The stored procedure - WORK_CUSTOMER_SRC_TO_SL_SP - written in JS - collects data from WORK_CUSTOMER_SRC_TO_SL_STREAM and adds that data into the table (that is just active in the session ) WORK_CUSTOMER_XFM in the Silver Schema. Later, this table is merged into CUSTOMER table in the Silver Schema. It tries to do UPSERT statement. 
- A task - WORK_CUSTOMER_SRC_TO_SL_TASK - is written on the Bronze schema to run every midnight that will check if there is data in the WORK_CUSTOMER_SRC_TO_SL_STREAM. If there is data, it will call the WORK_CUSTOMER_SRC_TO_SL_SP stored procedure which will transform data from the stream to the Silver CUSTOMER table.
- The view on Gold schema - CUSTOMER_VIEW - created on top of Silver.CUSTOMER will give an ample understanding of the data.

# ETL Using Snowflake, DBT

![image](https://github.com/diyaliza/ETL-Snowflake-AWS/assets/120042912/d0dba287-e92b-46fd-aa75-7183b7a7a3e0)

<img width="912" alt="image" src="https://github.com/diyaliza/ETL-Snowflake-AWS/assets/120042912/d9365bed-6058-4517-a84a-2def9deb9db4">
- External connection with DBT in the same Snowflake database (snowflake_dbt).
- Two DBT models (silver and gold) are created.
- The silver model connects to the Bronze schema's staging source and performs transformations, populating the silver table.
- A snapshot file is prepared to capture data changes in the silver table.
- The gold model includes a view that extracts data from the silver table, providing insights for analytics.

