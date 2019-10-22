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

import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.mock.common.MockPipelineConfigurer;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit Tests for LoadDataToSnowflakeConfig.
 */
public class LoadDataToSnowflakeConfigTest {
  private static final String ACCOUNT_NAME = "account";
  private static final String USER_NAME = "user123";
  private static final String PASSWORD = "password";
  private static final String WAREHOUSE_NAME = "warehouse";
  private static final String DB_NAME = "testdb1";
  private static final String SCHEMA = "PUBLIC";
  private static final String TABLE_NAME = "test_table";
  private static final String STAGING_LOCATION = "stage123";
  private static final String DATA_SOURCE = "External Location";
  private static final String PATH = "s3://caskdata/test/testfile.csv";
  private static final String FILE_FORMAT_TYPE = "CSV";
  private static final String S3_AUTHENTICATION_METHOD = "ACCESS CREDENTIALS";
  private static final String ACCESS_KEY_ID = "XXXXXXXX";
  private static final String SECRET_ACCESS_KEY = "XXXXXXXX";

  private static final String NAME_ACCESS_KEY = "accessKey";
  private static final String NAME_SECRET_ACCESS_KEY = "secretAccessKey";

  @Test
  public void testIfKeyIsNotPresent() {
    LoadDataToSnowflake.LoadDataToSnowflakeConfig config =
      new LoadDataToSnowflake.LoadDataToSnowflakeConfig(ACCOUNT_NAME, USER_NAME, PASSWORD, WAREHOUSE_NAME, DB_NAME,
                                                        SCHEMA, TABLE_NAME, STAGING_LOCATION, DATA_SOURCE, PATH,
                                                        FILE_FORMAT_TYPE, S3_AUTHENTICATION_METHOD, "", "");
    MockPipelineConfigurer configurer = new MockPipelineConfigurer(null);
    FailureCollector collector = configurer.getStageConfigurer().getFailureCollector();
    new LoadDataToSnowflake(config).configurePipeline(configurer);
    Assert.assertEquals(2, collector.getValidationFailures().size());
    Assert.assertEquals(NAME_ACCESS_KEY, collector.getValidationFailures().get(0).getCauses().get(0)
      .getAttribute(CauseAttributes.STAGE_CONFIG));
    Assert.assertEquals(NAME_SECRET_ACCESS_KEY, collector.getValidationFailures().get(1).getCauses().get(0)
      .getAttribute(CauseAttributes.STAGE_CONFIG));
  }

  @Test
  public void testIfRoleIsPresentAndPathStartWithS3a() {
    LoadDataToSnowflake.LoadDataToSnowflakeConfig config =
      new LoadDataToSnowflake.LoadDataToSnowflakeConfig(ACCOUNT_NAME, USER_NAME, PASSWORD, WAREHOUSE_NAME, DB_NAME,
                                                        SCHEMA, TABLE_NAME, STAGING_LOCATION, DATA_SOURCE, PATH,
                                                        FILE_FORMAT_TYPE, "IAM", "", "");
    MockPipelineConfigurer configurer = new MockPipelineConfigurer(null);
    try {
      new LoadDataToSnowflake(config).configurePipeline(configurer);
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Path must start with s3a for IAM based authentication.", e.getMessage());
    }
  }
}
