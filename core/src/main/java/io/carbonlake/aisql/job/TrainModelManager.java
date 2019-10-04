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
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.CarbonProperties;

/**
 * Maintains the status of each datamap. As per the status query will decide whether to hit
 * datamap or not.
 */
public class TrainModelManager {

  // Create private constructor to not allow create instance of it
  private TrainModelManager() {

  }

  /**
   * TODO Use factory when we have more storage providers
   */
  private static TrainJobStorageProvider storageProvider =
      new DiskBasedTrainJobProvider(CarbonProperties.getInstance().getSystemFolderLocation()
          + CarbonCommonConstants.FILE_SEPARATOR + "model");


  /**
   * Get all non dropped trained models of the given experiment.
   */
  public static TrainModelDetail[] getAllEnabledTrainedModels(String experimentName)
      throws IOException {
    TrainModelDetail[] trainModelDetails = storageProvider.getAllTrainModels(experimentName);
    List<TrainModelDetail> statusDetailList = new ArrayList<>();
    for (TrainModelDetail statusDetail : trainModelDetails) {
      if (statusDetail.getStatus() == TrainModelDetail.Status.CREATED) {
        statusDetailList.add(statusDetail);
      }
    }
    return statusDetailList.toArray(new TrainModelDetail[statusDetailList.size()]);
  }

  /**
   * Get all trained models of given experiment, including dropped models.
   */
  public static TrainModelDetail[] getAllTrainedModels(String experimentName) throws IOException {
    return storageProvider.getAllTrainModels(experimentName);
  }

  /**
   * Get Train Model detail from storage.
   */
  public static TrainModelDetail getTrainModel(
      String experimentName, String modelName) throws IOException {
    return storageProvider.getTrainModel(experimentName, modelName);
  }

  /**
   * Drop the train model from storage.
   */
  public static void dropTrainModel(String experimentName, String modelName) throws IOException {
    storageProvider.dropTrainModel(experimentName, modelName);
  }

  /**
   * Drop the trained model
   */
  public static void dropModel(String experimentName) throws IOException {
    storageProvider.dropModel(experimentName);
  }

  /**
   * Save the trained model to storage
   */
  public static void saveTrainModel(String experimentName, TrainModelDetail modelDetail)
      throws IOException {
    storageProvider.saveTrainModel(experimentName, modelDetail);
  }

  /**
   * Update the train model
   */
  public static void updateTrainModel(String experimentName, TrainModelDetail modelDetail)
      throws IOException {
    storageProvider.updateTrainModel(experimentName, modelDetail);
  }

}
