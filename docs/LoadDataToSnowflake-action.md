# LoadDataToSnowflake Action

Description
-----------
LoadDataToSnowflake Action to load data from internal/external location(Amazon S3) into the Snowflake table.

Properties
----------

**accountName:** Snowflake account name. (Macro-enabled)

**userName:** Username. (Macro-enabled)

**password:** Password. (Macro-enabled)

**warehouse:** Warehouse. (Macro-enabled)

**dbName:** Database name. (Macro-enabled)

**schema:** Schema. (Macro-enabled)

**tableName:** The Snowflake table name where the data will be loaded. (Macro-enabled)

**stageName:** The staging location where the data from the data source will be loaded. (Macro-enabled)

**dataSource:** Source of the data to be loaded. Defaults to Internal. (Macro-enabled)

**path:** Path/URL(AWS S3) of the data to be loaded. (Macro-enabled)

**fileFormatType:** File format to specify the type of file. Defaults to CSV. (Macro-enabled)

**s3AuthenticationMethod:** S3 Authentication method. Defaults to Access Credentials. For IAM authentication, cluster should be hosted on AWS servers. (Macro-enabled)

**accessKey:** Access key for AWS S3 to connect to.Mandatory if dataSource is AWS S3. (Macro-enabled)

**secretAccessKey:** Secret access key for AWS S3 to connect to. Mandatory if dataSource is AWS S3. (Macro-enabled)


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
      "name": "LoadDataToSnowflake",
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
