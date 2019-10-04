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

import java.util.Map;

public interface ModelAPI {

  long startTrainingJob(Map<String, String> options, String modelName, DataScan dataScan)
      throws ModelAPIException;

  void stopTrainingJob(long jobId) throws ModelAPIException;

  Map<String, String> getTrainingJobInfo(long jobId) throws ModelAPIException;

  String importModel(Map<String, String> options, String udfName) throws ModelAPIException;

  Map<String, String> getModelInfo(String modelId) throws ModelAPIException;

  String deployModel(Map<String, String> options, String modelName) throws ModelAPIException;

  String queryService(ModelInputParams params, String url) throws ModelAPIException;

  Map<String, Object> getModelServiceDetails(String serviceId) throws ModelAPIException;

  Map<String, Object> getModelDetails(String modelId) throws ModelAPIException;

  ModelInputParams parseConfig(Map<String, String> options) throws ModelAPIException;

  void deleteModel(String modelId) throws ModelAPIException;

  void deleteModelService(String serviceId) throws ModelAPIException;

}
