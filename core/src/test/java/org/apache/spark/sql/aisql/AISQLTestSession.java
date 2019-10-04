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

import java.io.File;
import java.io.IOException;

import org.apache.carbondata.core.util.CarbonProperties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class AISQLTestSession {

  private static SparkSession session;

  private static void init() {
    CarbonProperties.getInstance().addProperty("carbon.system.folder.location", new File(".").getAbsolutePath());
    String warehouse = null;
    try {
      warehouse = new File("./warehouse").getCanonicalPath();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    SparkSession.Builder builder = SparkSession.builder()
        .master("local")
        .config("spark.driver.host", "localhost")
        .config("spark.sql.warehouse.dir", warehouse)
        .withExtensions(new AISQLExtension());
    session = builder.getOrCreate();
    session.sparkContext().setLogLevel("ERROR");
  }

  static Dataset<Row> sql(String sqlString) {
    if (session == null) {
      init();
    }
    return session.sql(sqlString);
  }

  static SparkSession getSession() {
    if (session == null) {
      init();
    }
    return session;
  }
}
