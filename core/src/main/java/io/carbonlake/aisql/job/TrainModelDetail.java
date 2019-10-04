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
package io.carbonlake.aisql.job;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.carbondata.common.annotations.InterfaceAudience;

/**
 * Status of each datamap
 */
@InterfaceAudience.Internal
public class TrainModelDetail implements Serializable {

  private static final long serialVersionUID = -1208096889633270490L;

  private String jobName;

  private Map<String, String> properties = new HashMap<>();

  private Status status = Status.CREATED;

  public TrainModelDetail() {
  }

  public TrainModelDetail(String jobName, Map<String, String> properties) {
    this.jobName = jobName;
    this.properties = properties;
    this.status = Status.CREATED;
  }

  public String getJobName() {
    return jobName;
  }

  public void setJobName(String jobName) {
    this.jobName = jobName;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public void setProperties(Map<String, String> properties) {
    this.properties = properties;
  }

  public Status getStatus() {
    return status;
  }

  public void setStatus(Status status) {
    this.status = status;
  }

  public static enum Status {
    CREATED,DROPPED
  }
}
