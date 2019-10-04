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
package com.huawei.cloud.modelarts;

import java.util.HashMap;
import java.util.Map;

public class ModelArtsStatusCodes {

  private static final Map<String, String> trainingStatusCodes = new HashMap<>();

  static {

    trainingStatusCodes.put("0", "JOBSTAT_UNKNOWN: Unknown status.");
    trainingStatusCodes.put("1", "JOBSTAT_INIT: The job is being initialized.");
    trainingStatusCodes.put("2", "JOBSTAT_IMAGE_CREATING: The job image is being created.");
    trainingStatusCodes.put("3", "JOBSTAT_IMAGE_FAILED: Failed to create the job image.");
    trainingStatusCodes.put("4", "JOBSTAT_SUBMIT_TRYING: The job is being submitted.");
    trainingStatusCodes.put("5", "JOBSTAT_SUBMIT_FAILED: Failed to submit the job.");
    trainingStatusCodes.put("6", "JOBSTAT_DELETE_FAILED: Failed to delete the job.");
    trainingStatusCodes.put("7", "JOBSTAT_DEPLOYING: The job is being deployed.");
    trainingStatusCodes.put("8", "JOBSTAT_RUNNING: The job is running.");
    trainingStatusCodes.put("9", "JOBSTAT_KILLING: The job is being deleted.");
    trainingStatusCodes.put("10", "JOBSTAT_COMPLETED: The job has been completed.");
    trainingStatusCodes.put("11", "JOBSTAT_FAILED: Failed to run the job.");
    trainingStatusCodes.put("12", "JOBSTAT_KILLED: Job canceled successfully.");
    trainingStatusCodes.put("13", "JOBSTAT_CANCELED: Job canceled.");
    trainingStatusCodes.put("14", "JOBSTAT_LOST: Job lost.");
    trainingStatusCodes.put("15", "JOBSTAT_SCALING: The job is being scaled.");
    trainingStatusCodes.put("18", "JOBSTAT_CHECK_INIT: The job review is being initialized.");
    trainingStatusCodes.put("19", "JOBSTAT_RUNNING: The job is being reviewed.");
    trainingStatusCodes.put("21", "JOBSTAT_CHECK_FAILED: Failed to review the job.");
  }

  public static String getStatus(String code) {
    return trainingStatusCodes.get(code);
  }

}
