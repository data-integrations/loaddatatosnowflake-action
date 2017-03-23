# LoadToSnowflake Action

Description
-----------
LoadToSnowflake Action to load data from an Internal or External Amazon S3 Location into a Snowflake table. For more information, please see the documentation at [Snowflake.net](https://docs.snowflake.net/manuals/user-guide/data-load.html).

Properties
----------

**accountName:** The Snowflake Account name. (Macro-enabled)

**userName:** The username used to access the Snowflake Account. (Macro-enabled)

**password:** The password used to access the Snowflake Account. (Macro-enabled)

**warehouse:** The name of the Snowflake warehouse. (Macro-enabled)

**dbName:** The name of the database in the Snowflake warehouse. (Macro-enabled)

**schema:** The name of the schema, or namespace, of the table. (Macro-enabled)

**tableName:** The name of the Snowflake table name where the data will be loaded. (Macro-enabled)

**stageName:** The name of the staging location where the data from the data source will be loaded. (Macro-enabled)

**dataSource:** Source of the data to be loaded. Defaults to Internal. (Macro-enabled)

**path:** Path or AWS S3 URL of the data to be loaded. (Macro-enabled)

**fileFormatType:** File format to specify the type of file. Defaults to CSV. (Macro-enabled)

**s3AuthenticationMethod:** S3 Authentication method. Defaults to Access Credentials. For IAM authentication, 
cluster should be hosted on AWS servers. For more information on 
setting up S3 buckets, see [Configuring an S3 Bucket to Use as an External Location](https://docs.snowflake.net/manuals/user-guide/data-loading-s3-config.html) (Macro-enabled)

**accessKey:** Access key for AWS S3 to connect to. Required if dataSource is AWS S3 and authentication is Access Credentials. (Macro-enabled)

**secretAccessKey:** Secret access key for AWS S3 to connect to. Required if dataSource is AWS S3 and authentication is Access Credentials. (Macro-enabled)


Conditions
----------
Any invalid configuration for connection to Snowflake, will result into runtime failure.

Any invalid configurations for connecting to AWS S3 bucket, will result into the runtime failure.

Data Source needed must be specified correctly, External for AWS S3 and Internal for file system.

Table must exists in the Snowflake cluster, for loading the data. If not, then it will result into the runtime failure.


Example
-------
This example connects to a S3 instance using the 'accessKey and secretAccessKey', and to Snowflake instance using
' username and password'. Data from the S3 bucket provided through 'path' will be loaded
into the Snowflake table 'emp_test'.

    {
      "name": "LoadToSnowflake",
      "type": "action",
        "properties": {
          "accountName": "account",
          "userName": "user",
          "password": "password",
          "warehouse": "warehouse1",
          "dbName": "empDb",
          "schema": "schema",
          "tableName": "emp_test",
          "stageName": "my_csv_stage",
          "dataSource": "External Location",
          "path": "s3://caskdata-ustest/test/testfile.csv",
          "fileFormatType": "CSV",
          "s3AuthenticationMethod": "Access Credentials"
          "accessKey": "access-key",
          "secretAccessKey": "secret-access-key"
        }
    }
