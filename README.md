<img width="596" alt="image" src="https://github.com/diyaliza/ETL-Snowflake-AWS/assets/120042912/8beb0dc8-3e8e-4497-91f1-c3dfaba98cc0">

The project is an ETL demonstration of data that is loaded into S3, which will be transformed into 3 differnt stages of Snowflake DBMS. 
The data is loaded into S3 using a python scipt using Boto3. 
The S3 bucket has been configured with an event trigger, when objects are inserted into the bucket. Once the bucket triggers the event with the destination of the snowpipe that is configured in the Snowflake.
The Snowflake has 3 different schemas - Bronze, Silver and Gold. The bronze schema has a stage - SOURCE_DATA_CUSTOMER_STAGE which accepts the csv files that gets added in the S3 bucket 
There is a pipe - WORK_CUSTOMER_SRC_TO_SL_PIPE which will take files from the external stage (SOURCE_DATA_CUSTOMER_STAGE) into the table WORK_CUSTOMER_COPY in the Bronze schema. 
There is also an error integration set up which will track any errors that pops out out of the ETL and sends the information to AWS SNS, where teh subscription is an email service to my personal email. 
A stream - WORK_CUSTOMER_SRC_TO_SL_STREAM is set up on WORK_CUSTOMER_COPY that tracks when appending is done on the WORK_CUSTOMER_COPY table. 
The stored procedure - WORK_CUSTOMER_SRC_TO_SL_SP - written in JS - collects data from WORK_CUSTOMER_SRC_TO_SL_STREAM and adds that data into the table (that is just active in the session ) WORK_CUSTOMER_XFM in the Silver Schema. Later, this table is merged into CUSTOMER table in the Silver Schema. It tries to do UPSERT statement. 
A task - WORK_CUSTOMER_SRC_TO_SL_TASK - is written on the Bronze schema to run every midnight that will check if there is data in the WORK_CUSTOMER_SRC_TO_SL_STREAM. If there is data, it will call the WORK_CUSTOMER_SRC_TO_SL_SP stored procedure which will transform data from the stream to the Silver CUSTOMER table.
The view on Gold schema - CUSTOMER_VIEW - created on top of Silver.CUSTOMER will give an ample understanding of the data.
