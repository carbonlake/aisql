/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.aisql;

import java.io.IOException;
import java.util.Arrays;

import org.apache.carbondata.common.exceptions.sql.NoSuchDataMapException;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.LiteralExpression;
import org.apache.carbondata.core.scan.expression.conditional.EqualToExpression;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.ObjectSerializationUtil;

import org.apache.spark.sql.aisql.intf.DataScan;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * unit test for leo model ddl commands
 */
public class TestModelTraining extends AISQLTestSession {

  @BeforeClass
  public static void setup() throws IOException {
    sql("drop database if exists db cascade");
    sql("create database db");
  }

  /**
   * test create and drop model
   */
  @Test
  public void testModel() throws IOException {
    sql("drop table if exists db.test");
    sql("create table db.test(c1 int, c2 int, c3 int)");
    sql("drop experiment if exists m1");
    // create model without options
    sql("CREATE EXPERIMENT m1  as select c1,c2 as a from db.test where c3>5 and c2=1");
    assert (FileFactory.isFileExist(
        CarbonProperties.getInstance().getSystemFolderLocation() + "/model/m1.dmschema"));
    sql("drop experiment if exists m1");
    assert (!FileFactory.isFileExist(
        CarbonProperties.getInstance().getSystemFolderLocation() + "/model/m1.dmschema"));
    sql("drop experiment if exists m2");
    // create model with options
    sql(
        "CREATE EXPERIMENT if not exists m2 OPTIONS('label_col'='c2', 'max_iteration'='100') "
            + "as select c1,c2 from db.test where c3>5");
    assert (FileFactory.isFileExist(
        CarbonProperties.getInstance().getSystemFolderLocation() + "/model/m2.dmschema"));
    sql("drop experiment if exists m2");
    sql("drop table if exists db.test");
  }

  /**
   * test query object
   */
  @Test
  public void testQueryObject()
      throws IOException, NoSuchDataMapException {
    CarbonProperties.getInstance().addProperty("leo.ma.username", "hwstaff_l00215684");
    CarbonProperties.getInstance().addProperty("leo.ma.password", "@Huawei123");
    sql("drop table if exists db.test");
    sql("create table db.test(c1 int, c2 int, c3 int)");
    sql("drop experiment if exists m1");
    sql(
        "CREATE experiment if not exists m1 OPTIONS('label_col'='c2', 'max_iteration'='100') "
            + "as select c1,c2 from db.test where c3=5");
    DataMapSchema m1 = ExperimentStoreManager.getInstance().getExperimentSchema("m1");
    assert(m1.getDataMapName().equalsIgnoreCase("m1"));
    assert(m1.getCtasQuery().equalsIgnoreCase(" select c1,c2 from db.test where c3=5"));
    // get Query Object
    String query = m1.getProperties().get(ExperimentStoreManager.QUERY_OBJECT());
    DataScan queryObject = (DataScan) ObjectSerializationUtil.convertStringToObject(query);
    String[] projects = new String[]{"c1", "c2"};
    // compare projection columns
    assert (Arrays.equals(queryObject.getProjectionColumns(), projects));
    // compare table name
    assert (queryObject.getTableName().equalsIgnoreCase("db_test"));
    // compare filter expression
    ColumnExpression columnExpression = new ColumnExpression("c3", DataTypes.INT);
    EqualToExpression equalToExpression = new EqualToExpression(columnExpression,
        new LiteralExpression("5", DataTypes.INT));
    assert (queryObject.getFilterExpression().getString().equals(equalToExpression.getString()));
  }

  @Test
  public void testModelWithModelArts() throws IOException {
    CarbonProperties.getInstance().addProperty("leo.ma.username", "hwstaff_l00215684");
    CarbonProperties.getInstance().addProperty("leo.ma.password", "@Huawei123");
    sql("drop database if exists a1 cascade");
    sql("create database a1");
    sql("drop table if exists a1.test");
    sql("create table a1.test(c1 int, c2 int, c3 int)");
    sql("drop experiment if exists m2");
    // create model with options
    sql(
        "CREATE experiment if not exists m2 OPTIONS('worker_server_num'='1', "
            + "'app_url'='/obs-5b79/train_mnist/', 'boot_file_url'='/obs-5b79/train_mnist/train_mnist.py', "
            + "'data_url'='/obs-5b79/dataset-mnist/','log_url'='/obs-5b79/train-log/','engine_id'='28','spec_id'='1') as select c1,c2 from a1.test where c3>5");
    assert (FileFactory.isFileExist(
        CarbonProperties.getInstance().getSystemFolderLocation() + "/model/m2.dmschema"));

    sql("create model job1 using experiment m2 OPTIONS('train_url'='/obs-5b79/mnist-model/','params'='num_epochs=1')").show();

    sql("drop model if exists job1 on experiment m2");

    sql("drop experiment if exists m2");
    sql("drop table if exists a1.test");
    sql("drop database if exists a1 cascade");
  }

  /**
   * test Model Metrics
   */
  @Test
  public void testExperimentMetrics() {
    CarbonProperties.getInstance().addProperty("leo.ma.username", "hwstaff_l00215684");
    CarbonProperties.getInstance().addProperty("leo.ma.password", "@Huawei123");
    sql("drop table if exists db.test");
    sql("create table db.test(c1 int, c2 int, c3 int)");
    sql("drop experiment if exists e11");
    sql(
        "CREATE experiment if not exists e11 OPTIONS('worker_server_num'='1', "
            + "'app_url'='/obs-5b79/train_mnist/', 'boot_file_url'='/obs-5b79/train_mnist/train_mnist.py', "
            + "'data_url'='/obs-5b79/dataset-mnist/','log_url'='/obs-5b79/train-log/','engine_id'='28','spec_id'='1') as select c1,c2 from db.test where c3>5");
    sql("create model job1 using experiment e11 OPTIONS('train_url'='/obs-5b79/mnist-model/','params'='num_epochs=1')").show();
    // get experiment info for experiment m
    sql("select * from experiment_info(e11)").show(false);
    sql("drop model if exists job1 on experiment e11").show(false);
    sql("drop experiment if exists e11");
    sql("drop table if exists db.test");
  }

  /**
   * test Job Metrics
   */
  @Test
  public void testJobMetrics() {
    CarbonProperties.getInstance().addProperty("leo.ma.username", "hwstaff_l00215684");
    CarbonProperties.getInstance().addProperty("leo.ma.password", "@Huawei123");
    sql("drop table if exists db.test");
    sql("create table db.test(c1 int, c2 int, c3 int)");
    sql("drop experiment if exists m1");
    sql(
        "CREATE experiment if not exists m1 OPTIONS('worker_server_num'='1', "
            + "'app_url'='/obs-5b79/train_mnist/', 'boot_file_url'='/obs-5b79/train_mnist/train_mnist.py', "
            + "'data_url'='/obs-5b79/dataset-mnist/','log_url'='/obs-5b79/train-log/','engine_id'='28','spec_id'='1') as select c1,c2 from db.test where c3>5");
    sql("drop model if exists j on experiment m1").show(false);
    sql("create model j using experiment m1 OPTIONS('train_url'='/obs-5b79/mnist-model/','params'='num_epochs=1')").show();
    // get job metrics for job j
    sql("select * from training_info(m1.j)").show(false);
    sql("drop model if exists j on experiment m1").show(false);
    sql("drop experiment if exists m1");
    sql("drop table if exists db.test");
  }

  @Test
  public void testModelWithModelArtsFlower() throws IOException {
    CarbonProperties.getInstance().addProperty("leo.ma.username", "hwstaff_l00215684");
    CarbonProperties.getInstance().addProperty("leo.ma.password", "@Huawei123");
    sql("drop database if exists a1 cascade");
    sql("create database a1");
    sql("drop table if exists a1.test");
    sql("create table a1.test(c1 int, c2 int, c3 int)");
    sql("drop experiment if exists flower_exp");
    // create model with options
    sql(
        "CREATE experiment if not exists flower_exp OPTIONS('worker_server_num'='1', "
            + "'model_id'='7', 'dataset_id'='7EkkgKp0hbbZH6wp3MU', 'dataset_name'='flower', 'dataset_version_name'='V002',"
            + "'dataset_version_id'='2liks7uf5BazuB4rWai','log_url'='/obs-5b79/train-log/','engine_id'='28','spec_id'='1') as select c1,c2 from a1.test where c3>5");
    assert (FileFactory.isFileExist(
        CarbonProperties.getInstance().getSystemFolderLocation() + "/model/flower_exp.dmschema"));
    sql("drop model if exists job1 on experiment flower_exp").show(false);

    sql("create model job1 using experiment flower_exp OPTIONS('train_url'='/obs-5b79/flower_model1/')").show();

    sql("select * from training_info(flower_exp.job1)").show(false);

    sql("REGISTER MODEL flower_exp.job1 AS flower_mod");
    sql("select * from model_info(flower_mod)").show(false);
    sql("drop table if exists a1.testIMG");
    sql("create table a1.testIMG(c1 string, c2 binary)");
    sql("LOAD DATA LOCAL INPATH '/home/root1/carbon/carbondata/fleet/router/src/test/resources/img.csv' INTO TABLE a1.testIMG OPTIONS('header'='false','DELIMITER'=',','binary_decoder'='baSe64')").show();
    sql("select flower_mod(c2) from a1.testIMG").show(false);
    sql("drop table if exists a1.testIMG");

    // delete model and service from modelArts
    sql("UNREGISTER MODEL flower_exp.job1");

    sql("drop model if exists job1 on experiment flower_exp");
    sql("drop experiment if exists flower_exp");
    sql("drop table if exists a1.test");
    sql("drop database if exists a1 cascade");
  }

  @Test
  public void testExperimentIfAlreadyExists() throws IOException {
    sql("drop table if exists db.test");
    sql("create table db.test(c1 int, c2 int, c3 int)");
    sql("drop experiment if exists m1");
    sql("CREATE EXPERIMENT m1  as select c1,c2 as a from db.test where c3>5 and c2=1");
    assert (FileFactory.isFileExist(
        CarbonProperties.getInstance().getSystemFolderLocation() + "/model/m1.dmschema"));
    // create experiment with same name
    try {
      sql("CREATE EXPERIMENT m1  as select c1,c2 as a from db.test where c3>5 and c2=1");
    } catch (Exception e) {
      assert(e.getMessage().contains("Experiment with name m1 already exists in storage;"));
    }
    sql("drop experiment if exists m1");
    sql("drop table if exists db.test");
  }

  @Test
  public void testExperimentWithInvalidOptions() {
    sql("drop table if exists db.test");
    sql("create table db.test(c1 int, c2 int, c3 int)");
    sql("drop experiment if exists m1");
    // create experiment with invalid options
    try {
      sql("CREATE experiment if not exists flower_exp OPTIONS('worker_server_num'='1', "
          + "'model_id'='7', 'dataset_id'='7EkkgKp0hbbZH6wp3MU', 'dataset_name'='flower', 'dataset_version_name'='V002',"
          + "'dataset_version_id'='2liks7uf5BazuB4rWai','log_url'='/obs-5b79/train-log/','engine_id'='28','spec_id'='1', 'user_name'='abc') "
          + "as select c1,c2 from db.test where c3>5");
    } catch (Exception e) {
      assert(e.getMessage().contains("Invalid Options: {user_name}"));
    }
    sql("drop experiment if exists m1");
    sql("drop table if exists db.test");
  }

  @Test
  public void testCreateMoreModelsOnSameExperiment() throws IOException {
    CarbonProperties.getInstance().addProperty("leo.ma.username", "hwstaff_l00215684");
    CarbonProperties.getInstance().addProperty("leo.ma.password", "@Huawei123");
    sql("drop database if exists a1 cascade");
    sql("create database a1");
    sql("drop table if exists a1.test");
    sql("create table a1.test(c1 int, c2 int, c3 int)");
    sql("drop experiment if exists exp");
    // create model with options
    sql(
        "CREATE experiment if not exists exp OPTIONS('worker_server_num'='1', "
            + "'app_url'='/obs-5b79/train_mnist/', 'boot_file_url'='/obs-5b79/train_mnist/train_mnist.py', "
            + "'data_url'='/obs-5b79/dataset-mnist/','log_url'='/obs-5b79/train-log/','engine_id'='28','spec_id'='1') as select c1,c2 from a1.test where c3>5");
    assert (FileFactory.isFileExist(
        CarbonProperties.getInstance().getSystemFolderLocation() + "/model/exp.dmschema"));

    sql("create model job_1 using experiment exp OPTIONS('train_url'='/obs-5b79/mnist-model/','params'='num_epochs=1')").show();
    sql("create model job_2 using experiment exp OPTIONS('train_url'='/obs-5b79/flower_model1/','params'='num_epochs=3')").show();
    assert (sql("select * from experiment_info(exp)").count() == 2);

    sql("drop model if exists job_1 on experiment exp");
    sql("drop model if exists job_2 on experiment exp");
    sql("drop experiment if exists exp");
    sql("drop table if exists a1.test");
    sql("drop database if exists a1 cascade");
  }

  @Test
  public void testCreateModelWithInvalidOptions() throws IOException {
    CarbonProperties.getInstance().addProperty("leo.ma.username", "hwstaff_l00215684");
    CarbonProperties.getInstance().addProperty("leo.ma.password", "@Huawei123");
    sql("drop database if exists a1 cascade");
    sql("create database a1");
    sql("drop table if exists a1.test");
    sql("create table a1.test(c1 int, c2 int, c3 int)");
    sql("drop experiment if exists exp");
    // create model with options
    sql("CREATE experiment if not exists exp OPTIONS('worker_server_num'='1', "
        + "'app_url'='/obs-5b79/train_mnist/', 'boot_file_url'='/obs-5b79/train_mnist/train_mnist.py', "
        + "'data_url'='/obs-5b79/dataset-mnist/','log_url'='/obs-5b79/train-log/','engine_id'='28','spec_id'='1') "
        + "as select c1,c2 from a1.test where c3>5");
    assert (FileFactory.isFileExist(
        CarbonProperties.getInstance().getSystemFolderLocation() + "/model/exp.dmschema"));
    try {
      sql("create model job_1 using experiment exp OPTIONS('train_url'='/obs-5b79/mnist-model/','params'='num_epochs=1', 'user_name'='abc')").show();
    } catch (Exception e) {
      assert (e.getMessage().contains("Invalid Options: {user_name}"));
    }
    sql("drop model if exists job_1 on experiment exp");
    sql("drop experiment if exists exp");
    sql("drop table if exists a1.test");
    sql("drop database if exists a1 cascade");
  }


  @AfterClass
  public static void tearDown() {
    sql("drop database if exists db cascade");
  }
}