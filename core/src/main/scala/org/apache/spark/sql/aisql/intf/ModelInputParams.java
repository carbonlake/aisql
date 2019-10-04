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

package org.apache.spark.sql.aisql.intf;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.metadata.datatype.DataType;

/**
 *Model input Query params to get the predictions.
 */
public class ModelInputParams implements Serializable {

  private static final long serialVersionUID = 7882280289101749788L;

  private List<ModelInputParam> params = new ArrayList<>();

  public List<ModelInputParam> getParams() {
    return params;
  }

  public void setParams(List<ModelInputParam> params) {
    this.params = params;
  }

  public static class ModelInputParam implements Serializable {

    private static final long serialVersionUID = -5561314869876095857L;

    private String paramName;

    private Object paramValue;

    private DataType dataType;

    public ModelInputParam(String paramName, Object paramValue, DataType dataType) {
      this.paramName = paramName;
      this.paramValue = paramValue;
      this.dataType = dataType;
    }

    public String getParamName() {
      return paramName;
    }

    public Object getParamValue() {
      return paramValue;
    }

    public void setParamValue(Object paramValue) {
      this.paramValue = paramValue;
    }

    public DataType getDataType() {
      return dataType;
    }
  }
}


