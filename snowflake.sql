USE SCHEMA SNOWLAKE_DB.BRONZE
CREATE OR REPLACE STORAGE INTEGRATION s3_bronze
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = 'S3'
ENABLED=TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::767397677613:role/snowflake-access'
STORAGE_ALLOWED_LOCATIONS= ('s3://snowflake-project-etl/raw_data/')

DESC INTEGRATION s3_bronze

CREATE OR REPLACE FILE FORMAT MY_CSV_FORMAT
TYPE=CSV
FIELD_DELIMITER = ','
FIELD_OPTIONALLY_ENCLOSED_BY='"'
SKIP_HEADER=1
NULL_IF = ('NULL','null')
EMPTY_FIELD_AS_NULL = true

GRANT CREATE STAGE ON SCHEMA BRONZE TO ROLE ACCOUNTADMIN

GRANT USAGE ON INTEGRATION s3_bronze TO ROLE ACCOUNTADMIN

CREATE OR REPLACE STAGE SOURCE_DATA_CUSTOMER_STAGE
STORAGE_INTEGRATION = s3_bronze
URL = 's3://snowflake-project-etl/raw_data/'
FILE_FORMAT =MY_CSV_FORMAT

ls @SOURCE_DATA_CUSTOMER_STAGE

CREATE NOTIFICATION INTEGRATION SNOWFLAKE_ERROR_INTEGRATION

  ENABLED = true

  TYPE = QUEUE

  NOTIFICATION_PROVIDER = AWS_SNS

  DIRECTION = OUTBOUND

  AWS_SNS_TOPIC_ARN = 'arn:aws:sns:us-east-1:767397677613:S3_SF_Error_Notification'

  AWS_SNS_ROLE_ARN = 'arn:aws:iam::767397677613:role/snowflake_error_notif_role';

DESC INTEGRATION SNOWFLAKE_ERROR_INTEGRATION

CREATE OR REPLACE TABLE

        SNOWLAKE_DB.SILVER.CUSTOMER

    (

       FIRST_NAME VARCHAR(50) NOT NULL COLLATE 'en-ci',

  LAST_NAME VARCHAR(50) NOT NULL COLLATE 'en-ci',

  COMPANYNAME VARCHAR(200) COLLATE 'en-ci',

  PHONE VARCHAR(200) COLLATE 'en-ci',

  ADDRESSLINE1 VARCHAR(200) NOT NULL COLLATE 'en-ci',

  ADDRESSLINE2 VARCHAR(200) COLLATE 'en-ci',

       CITY VARCHAR(100) COLLATE 'en-ci',

  STATE VARCHAR(50) NOT NULL COLLATE 'en-ci',

  COUNTRY VARCHAR(20) NOT NULL COLLATE 'en-ci',

  POSTALCODE VARCHAR(50) COLLATE 'en-ci',

  PROVINCE VARCHAR(20) COLLATE 'en-ci',

  ---- SNOWFLAKE METADATA COLUMNS ----

  TIME_ZONE VARCHAR(3) NOT NULL COLLATE 'en-ci',

  SOURCE_SYS_NAME VARCHAR(20) NOT NULL COLLATE 'en-ci',

  INSTNC_ST_NM VARCHAR(50) NOT NULL COLLATE 'en-ci',

  PROCESS_ID VARCHAR(100) NOT NULL COLLATE 'en-ci',

  PROCESS_NAME VARCHAR(200) NOT NULL COLLATE 'en-ci',

  INSERT_DTS TIMESTAMP_NTZ(6) NOT NULL,

  UPDATE_DTS TIMESTAMP_NTZ(6) NOT NULL,

  MD5_HASH VARCHAR(80) NOT NULL COLLATE 'en-ci',

       PRIMARY KEY (FIRST_NAME,LAST_NAME,ADDRESSLINE1,COUNTRY,STATE,MD5_HASH));

CREATE OR REPLACE TABLE

    SNOWLAKE_DB.BRONZE.WORK_CUSTOMER_COPY (

  COMPANYNAME VARCHAR(200) COLLATE 'en-ci',

  PHONE VARCHAR(200) COLLATE 'en-ci',

  ADDRESSLINE1 VARCHAR(200) COLLATE 'en-ci',

  ADDRESSLINE2 VARCHAR(200) COLLATE 'en-ci',

                    CITY VARCHAR(100) COLLATE 'en-ci',

  STATE VARCHAR(50) COLLATE 'en-ci',

  POSTALCODE VARCHAR(50) COLLATE 'en-ci',

  COUNTRY VARCHAR(20) COLLATE 'en-ci',

  PROVINCE VARCHAR(20) COLLATE 'en-ci',

  FIRST_NAME VARCHAR(50) COLLATE 'en-ci',

  LAST_NAME VARCHAR(50) COLLATE 'en-ci',

       ---- SNOWFLAKE METADATA COLUMNS ----

  INSERT_DTS TIMESTAMP_NTZ(6) NOT NULL,

  UPDATE_DTS TIMESTAMP_NTZ(6) NOT NULL,

                     SOURCE_FILE_NAME VARCHAR(500) NOT NULL,

                     SOURCE_FILE_ROW_NUMBER NUMBER(38,0) NOT NULL);
CREATE OR REPLACE STREAM SNOWLAKE_DB.BRONZE.WORK_CUSTOMER_SRC_TO_SL_STREAM

ON

TABLE SNOWLAKE_DB.BRONZE.WORK_CUSTOMER_COPY

APPEND_ONLY = TRUE;


CREATE OR REPLACE PIPE SNOWLAKE_DB.BRONZE.WORK_CUSTOMER_SRC_TO_SL_PIPE

AUTO_INGEST=TRUE

ERROR_INTEGRATION = SNOWFLAKE_ERROR_INTEGRATION

COMMENT = 'INGEST DATA FILES FROM SNOWFLAKE EXTERNAL STAGE LOCATION'

AS

    COPY INTO

SNOWLAKE_DB.BRONZE.WORK_CUSTOMER_COPY

FROM

    (     SELECT

          $1 AS COMPANYNAME,

  $2 AS PHONE,

  $3 AS ADDRESSLINE1,

  $4 AS ADDRESSLINE2,

                       $5 AS CITY,

  $6 AS STATE,

  $7 AS POSTALCODE,

  $8 AS COUNTRY,

  $9 AS PROVINCE,

  $10 AS FIRST_NAME,

  $11 AS LAST_NAME,

                       CURRENT_TIMESTAMP(6) AS INSERT_DTS,

                       CURRENT_TIMESTAMP(6) AS UPDATE_DTS,

                       METADATA$FILENAME AS SOURCE_FILE_NAME,

                       METADATA$FILE_ROW_NUMBER AS SOURCE_FILE_ROW_NUMBER

      FROM

             @SNOWLAKE_DB.BRONZE.SOURCE_DATA_CUSTOMER_STAGE/

    )

FILE_FORMAT = (FORMAT_NAME = MY_CSV_FORMAT);

CREATE PROCEDURE IF NOT EXISTS SNOWLAKE_DB.BRONZE.WORK_CUSTOMER_SRC_TO_SL_SP(

    DATABASE VARCHAR(50))

    RETURNS VARCHAR(50)

    LANGUAGE JAVASCRIPT

    EXECUTE AS CALLER

    AS

    $$

    try {

//Create statement BEGIN, Begins a transaction in the current session

        snowflake.execute({sqlText:`BEGIN TRANSACTION;`});

//load data from Snow Stream to Customer XFM in SILVER Layer

    snowflake.execute({sqlText:`

        CREATE OR REPLACE TABLE ${DATABASE}.SILVER.WORK_CUSTOMER_XFM

        AS

        SELECT

           FIRST_NAME,

      LAST_NAME,

      COMPANYNAME,

      PHONE,

      ADDRESSLINE1,

      ADDRESSLINE2,

           CITY,

      STATE,

      COUNTRY,

      POSTALCODE,

      PROVINCE,

        ---- snowflake metadata columns ----

           'EST' AS TIME_ZONE,

           'CUSTOMER' AS SOURCE_SYS_NAME,

           'STANDARD' AS INSTNC_ST_NM,

           CURRENT_SESSION() AS PROCESS_ID,

           'SNOWLAKE_DB.BRONZE.WORK_CUSTOMER_SRC_TO_SL_SP' AS PROCESS_NAME,

           INSERT_DTS,

           UPDATE_DTS,

           MD5(

              COALESCE(TO_VARCHAR(TRIM(COMPANYNAME)),'') ||

              COALESCE(TO_VARCHAR(TRIM(PHONE)),'')||

      COALESCE(TO_VARCHAR(TRIM(ADDRESSLINE1)),'')||

      COALESCE(TO_VARCHAR(TRIM(ADDRESSLINE2)),'')||

      COALESCE(TO_VARCHAR(TRIM(CITY)),'')||

      COALESCE(TO_VARCHAR(TRIM(STATE)),'')||

      COALESCE(TO_VARCHAR(TRIM(COUNTRY)),'')||

      COALESCE(TO_VARCHAR(TRIM(POSTALCODE)),'')||

      COALESCE(TO_VARCHAR(TRIM(PROVINCE)),'')

           ) AS MD5_HASH,

           SOURCE_FILE_NAME,

           SOURCE_FILE_ROW_NUMBER

        FROM 

            ${DATABASE}.BRONZE.WORK_CUSTOMER_SRC_TO_SL_STREAM

    WHERE

            FIRST_NAME is NOT NULL

    AND

    LAST_NAME IS NOT NULL;

    `});

    

    //Versioning Notes:

//1. The below SCD Type-1 versioning technique does a UPSERT of all the records whenever an UPDATE comes in for a PK.

//UPSERT the records in the TARGET table  - SILVER Layer

snowflake.execute({sqlText:`

        MERGE INTO

                ${DATABASE}.SILVER.CUSTOMER TGT

            USING

                ${DATABASE}.SILVER.WORK_CUSTOMER_XFM XFM

            ON

                TGT.FIRST_NAME = XFM.FIRST_NAME

            AND

                TGT.LAST_NAME = XFM.LAST_NAME

           WHEN MATCHED AND TGT.MD5_HASH = XFM.MD5_HASH THEN 

                UPDATE SET

                    TGT.UPDATE_DTS = XFM.UPDATE_DTS

            WHEN MATCHED AND TGT.MD5_HASH <> XFM.MD5_HASH THEN

                UPDATE SET    

                    TGT.COMPANYNAME = XFM.COMPANYNAME,

                    TGT.PHONE = XFM.PHONE,

                    TGT.ADDRESSLINE1 = XFM.ADDRESSLINE1,

TGT.ADDRESSLINE2 = XFM.ADDRESSLINE2,

TGT.CITY = XFM.CITY,

TGT.STATE = XFM.STATE,

TGT.COUNTRY = XFM.COUNTRY,

TGT.POSTALCODE = XFM.POSTALCODE,

TGT.PROVINCE = XFM.PROVINCE,

                    TGT.TIME_ZONE = XFM.TIME_ZONE,

                    TGT.SOURCE_SYS_NAME = XFM.SOURCE_SYS_NAME,

                    TGT.INSTNC_ST_NM = XFM.INSTNC_ST_NM,

                    TGT.PROCESS_ID = XFM.PROCESS_ID,

                    TGT.PROCESS_NAME = XFM.PROCESS_NAME,

                    TGT.UPDATE_DTS = XFM.UPDATE_DTS,

                    TGT.MD5_HASH = XFM.MD5_HASH

            WHEN NOT MATCHED THEN

                INSERT (

                    FIRST_NAME,

    LAST_NAME,

    COMPANYNAME,

    PHONE,

    ADDRESSLINE1,

    ADDRESSLINE2,

    CITY,

STATE,

COUNTRY,

POSTALCODE,

PROVINCE,

TIME_ZONE,

SOURCE_SYS_NAME,

INSTNC_ST_NM,

PROCESS_ID,

PROCESS_NAME,

INSERT_DTS,

UPDATE_DTS,

MD5_HASH

                )

                VALUES (

                    XFM.FIRST_NAME,

XFM.LAST_NAME,

XFM.COMPANYNAME,

XFM.PHONE,

XFM.ADDRESSLINE1,

XFM.ADDRESSLINE2,

XFM.CITY,

XFM.STATE,

XFM.COUNTRY,

XFM.POSTALCODE,

XFM.PROVINCE,

XFM.TIME_ZONE,

XFM.SOURCE_SYS_NAME,

XFM.INSTNC_ST_NM,

XFM.PROCESS_ID,

XFM.PROCESS_NAME,

XFM.INSERT_DTS,

XFM.UPDATE_DTS,

XFM.MD5_HASH

                );

            `});


//Create statement COMMIT, Commits an open transaction in the current session

snowflake.execute({sqlText:`COMMIT;`});

//Statement returned for info and debuging purposes

return "Store Procedure Executed Successfully";

}

    catch (err)

    {

        result = 'Error: ' + err;

        snowflake.execute({sqlText:`ROLLBACK;`});

        throw result;

    }

    $$;

CREATE OR REPLACE TASK SNOWLAKE_DB.BRONZE.WORK_CUSTOMER_SRC_TO_SL_TASK

    WAREHOUSE = COMPUTE_WH

    COMMENT = 'This task is used to load data from copy table to CUSTOMER silver layer table'

    SCHEDULE = 'USING CRON 0 0 * * * America/New_York'

    ERROR_INTEGRATION = SNOWFLAKE_ERROR_INTEGRATION

    WHEN SYSTEM$STREAM_HAS_DATA('SNOWLAKE_DB.BRONZE.WORK_CUSTOMER_SRC_TO_SL_STREAM')

    AS CALL SNOWLAKE_DB.BRONZE.WORK_CUSTOMER_SRC_TO_SL_SP('SNOWLAKE_DB');

ALTER TASK SNOWLAKE_DB.BRONZE.WORK_CUSTOMER_SRC_TO_SL_TASK RESUME;
CREATE VIEW 

    SNOWLAKE_DB.GOLD.CUSTOMER_VIEW

AS

SELECT

    FIRST_NAME,

LAST_NAME,

COMPANYNAME,

PHONE,

ADDRESSLINE1,

ADDRESSLINE2,

CITY,

STATE,

COUNTRY,

POSTALCODE,

PROVINCE,

TIME_ZONE,

SOURCE_SYS_NAME,

INSTNC_ST_NM,

PROCESS_ID,

PROCESS_NAME,

INSERT_DTS,

UPDATE_DTS,

MD5_HASH

FROM

    SNOWLAKE_DB.SILVER.CUSTOMER

;

SHOW PIPES;
EXECUTE TASK SNOWLAKE_DB.BRONZE.WORK_CUSTOMER_SRC_TO_SL_TASK;

SELECT

    *

FROM

    SNOWLAKE_DB.GOLD.CUSTOMER_VIEW

;