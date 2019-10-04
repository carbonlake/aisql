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

import java.io.IOException;

/**
 * It updates the datamap status to the storage. It will have 2 implementations one will be disk
 * based and another would be DB based
 *
 * @version 1.4
 */
public interface TrainJobStorageProvider {

  /**
   * It reads and returns all enabled train model details from storage.
   *
   * @return TrainModelDetail[] all trainjob details
   */
  TrainModelDetail[] getAllTrainModels(String experimentName) throws IOException;


  /**
   * It reads and returns train model details from storage.
   *
   */
  TrainModelDetail getTrainModel(String experimentName, String jobName) throws IOException;

  /**
   * Save the model detail of the given experiment.
   *
   */
  void saveTrainModel(String experimentName, TrainModelDetail jobDetail) throws IOException;

  /**
   * Updates the jobDetail of the given experiment.
   *
   */
  void updateTrainModel(String experimentName, TrainModelDetail jobDetail) throws IOException;

  /**
   * Drops the train model
   */
  void dropTrainModel(String experimentName, String trainModelName) throws IOException;

  /**
   * Drops the model
   */
  void dropModel(String modelName) throws IOException;
}
