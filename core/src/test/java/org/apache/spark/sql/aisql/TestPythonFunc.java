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

import java.util.List;

import org.apache.carbondata.core.datastore.impl.FileFactory;

import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestPythonFunc extends AISQLTestSession {

  @BeforeClass
  public static void setup() {
    sql("drop database if exists db1 cascade");
    sql("create database db1");
  }

  @Test
  public void testWebSearch() {
    sql("create table db1.t1 (name string, age int)");
    sql("select url, title from WebSearch('key_word'='特朗普', 'page_num'='3')").show(100, false);
    sql("drop table db1.t1");
  }

  @Test
  public void testDownload() {
    sql("create table db1.t1 (image binary)");
    sql("insert into db1.t1 select download('https://www.baidu.com/img/bd_logo1.png?qua=high&where=super')");
    sql("select * from db1.t1").show();
    sql("drop table db1.t1");
  }

  @Test
  public void testRunScript() {
    String s = FileFactory.getCarbonFile("./").getAbsolutePath();
    String scriptPath = s + "/src/test/python/test.py";
    List<Row> rows = sql("RUN SCRIPT \"" + scriptPath + "\" PYFUNC \"add\" " +
        "WITH PARAMS (\"x\"=\"4\", \"y\"=\"2\") OUTPUT (v long)").collectAsList();
    Assert.assertEquals(rows.get(0).get(0), 6l);

    rows = sql("RUN SCRIPT \"" + scriptPath + "\" PYFUNC \"sub\" " +
        "WITH PARAMS (\"x\"=\"100\", \"y\"=\"5\") OUTPUT (v long)").collectAsList();
    Assert.assertEquals(rows.get(0).get(0), 95l);

    rows = sql("RUN SCRIPT \"" + scriptPath + "\" PYFUNC \"numpyTest\" " +
        "OUTPUT (s string)").collectAsList();
    Assert.assertEquals(rows.get(0).get(0), "(2, 3)");
  }
}
