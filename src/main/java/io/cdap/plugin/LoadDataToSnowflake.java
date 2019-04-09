/*
 * Copyright © 2017-2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.cdap.plugin;


import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.api.action.ActionContext;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import javax.annotation.Nullable;

/**
 * LoadDataToSnowflake Action Plugin - Loads the data from internal/external Location(Amazon S3)
 * into Snowflake.
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name("LoadToSnowflake")
@Description("LoadToSnowflake action to load data from internal/external location(Amazon S3)" +
  " into the Snowflake table.")
public class LoadDataToSnowflake extends Action {
  private static final String ACCESS_CREDENTIALS = "Access Credentials";
  private static final String EXTERNAL_LOCATION = "External Location";
  private static final String INTERNAL_LOCATION = "Internal Location";
  private static final String CSV = "CSV";
  private final LoadDataToSnowflakeConfig config;

  public LoadDataToSnowflake(LoadDataToSnowflakeConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    if (config.dataSource.equalsIgnoreCase(EXTERNAL_LOCATION)) {
      config.validate(config.s3AuthenticationMethod);
    }
  }

  @Override
  public void run(ActionContext actionContext) throws Exception {
    try (Connection connection = getConnection(); Statement statement = connection.createStatement()) {
      statement.executeUpdate(buildCreateStageCommand());
      if (config.dataSource.equalsIgnoreCase(INTERNAL_LOCATION)) {
        statement.execute(buildPutCommand());
      }
      statement.executeUpdate(buildCopyIntoCommand());
    } catch (Exception e) {
      throw new IllegalArgumentException("Error while loading data to Snowflake: " + e.getMessage());
    }
  }

  private String buildCreateStageCommand() {
    StringBuilder createStageCommand = new StringBuilder();
    createStageCommand.append("CREATE OR REPLACE STAGE ").append(config.stageName);
    if (config.dataSource.equalsIgnoreCase(EXTERNAL_LOCATION)) {
      createStageCommand.append(" URL = '").append(config.path).append("' ");
      if (config.s3AuthenticationMethod.equalsIgnoreCase(ACCESS_CREDENTIALS)) {
        createStageCommand.append("CREDENTIALS = (").append("AWS_KEY_ID='")
          .append(config.accessKey).append("' ").append("AWS_SECRET_KEY='")
          .append(config.secretAccessKey).append("')");
      }
    }
    createStageCommand.append("  FILE_FORMAT = (TYPE = ").append(config.fileFormatType).append(")");
    return createStageCommand.toString();
  }

  private String buildPutCommand() {
    StringBuilder putCommand = new StringBuilder();
    putCommand.append("PUT ").append(config.path);
    putCommand.append(" @").append(config.stageName);
    return putCommand.toString();
  }

  private String buildCopyIntoCommand() {
    StringBuilder copyIntocommand = new StringBuilder();
    copyIntocommand.append("COPY INTO ").append(config.tableName);
    copyIntocommand.append(" FROM @").append(config.stageName).append(" ON_ERROR = 'SKIP_FILE'");
    return copyIntocommand.toString();
  }

  private Connection getConnection() throws SQLException {
    try {
      Class.forName("net.snowflake.client.jdbc.SnowflakeDriver");
    } catch (ClassNotFoundException ex) {
      throw new IllegalArgumentException("SnowflakeDriver driver not found " + ex.getMessage(), ex);
    }
    Properties properties = new Properties();
    properties.put("user", config.userName);
    properties.put("password", config.password);
    properties.put("account", config.accountName);
    properties.put("warehouse", config.warehouse);
    properties.put("db", config.dbName);
    properties.put("schema", config.schema);
    StringBuilder connectStr = new StringBuilder();
    connectStr.append("jdbc:snowflake://").append(config.accountName).append(".snowflakecomputing.com");
    return DriverManager.getConnection(connectStr.toString(), properties);
  }

  /**
   * Config class for LoadDataToSnowflake
   */
  public static class LoadDataToSnowflakeConfig extends PluginConfig {

    @Description("Snowflake account name. (Macro-enabled)")
    @Macro
    private String accountName;

    @Description("Username of the Snowflake account. (Macro-enabled)")
    @Macro
    private String userName;

    @Description("Password of the Snowflake account. (Macro-enabled)")
    @Macro
    private String password;

    @Description("The name of the Snowflake Warehouse. (Macro-enabled)")
    @Macro
    private String warehouse;

    @Description("The Database name in the Snowflake Warehouse. (Macro-enabled)")
    @Macro
    private String dbName;

    @Description("The name of the schema for the table in the Snowflake Warehouse. (Macro-enabled)")
    @Macro
    private String schema;

    @Description("The Snowflake table name where the data will be loaded. (Macro-enabled)")
    @Macro
    private String tableName;

    @Description("The staging location where the data from the data source will be loaded. (Macro-enabled)")
    @Macro
    private String stageName;

    @Description("Source of the data to be loaded. Defaults to Internal. (Macro-enabled)")
    @Macro
    private String dataSource;

    @Description("Path/URL(AWS S3) of the data to be loaded. (Macro-enabled)")
    @Macro
    private String path;

    @Description("File format to specify the type of file. Defaults to CSV. (Macro-enabled)")
    @Macro
    private String fileFormatType;

    @Description("S3 Authentication method. Defaults to Access Credentials. (Macro-enabled)")
    @Macro
    private String s3AuthenticationMethod;

    @Description("Access key for AWS S3 to connect to. Mandatory if dataSource is AWS S3. (Macro-enabled)")
    @Nullable
    @Macro
    private String accessKey;

    @Description("Secret access key for AWS S3 to connect to. Mandatory if dataSource is AWS S3. (Macro-enabled)")
    @Nullable
    @Macro
    private String secretAccessKey;


    public LoadDataToSnowflakeConfig(String accountName, String userName, String password, String warehouse,
                                     String dbName, String schema, String tableName, String stageName,
                                     String dataSource, String path, String fileFormatType,
                                     @Nullable String s3AuthenticationMethod, @Nullable String accessKey,
                                     @Nullable String secretAccessKey) {
      this.accountName = accountName;
      this.userName = userName;
      this.password = password;
      this.warehouse = warehouse;
      this.dbName = dbName;
      this.schema = schema;
      this.tableName = tableName;
      this.stageName = stageName;
      this.dataSource = dataSource;
      this.path = path;
      this.fileFormatType = fileFormatType;
      this.s3AuthenticationMethod = s3AuthenticationMethod;
      this.accessKey = accessKey;
      this.secretAccessKey = secretAccessKey;
    }

    public LoadDataToSnowflakeConfig() {
      this.dataSource = INTERNAL_LOCATION;
      this.fileFormatType = CSV;
      this.s3AuthenticationMethod = ACCESS_CREDENTIALS;
    }

    public void validate(String authenticationMethod) {
      if (authenticationMethod.equalsIgnoreCase(ACCESS_CREDENTIALS)) {
        if (!containsMacro("accessKey") && (accessKey == null || accessKey.isEmpty())) {
          throw new IllegalArgumentException("The Access Key must be specified if " +
                                               "authentication method is Access Credentials.");
        }
        if (!containsMacro("secretAccessKey") && (secretAccessKey == null || secretAccessKey.isEmpty())) {
          throw new IllegalArgumentException("The Secret Access Key must be specified if " +
                                               "authentication method is Access Credentials.");
        }
      }
    }
  }
}
