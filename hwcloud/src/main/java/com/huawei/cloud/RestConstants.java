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

package com.huawei.cloud;

/**
 * It contains Cloud end points and rest url constants.
 */
public interface RestConstants {

  String HUAWEI_ENDPOINT = "cn-north-1.myhuaweicloud.com";

  String HUAWEI_CLOUD_AUTH_ENDPOINT = "https://iam." + HUAWEI_ENDPOINT + "/v3/auth/tokens";

  String HUAWEI_CLOUD_SECURITYTOKEN_ENDPOINT =
      "https://iam." + HUAWEI_ENDPOINT + "/v3.0/OS-CREDENTIAL/securitytokens";

  String OBS_ENDPOINT =
      "https://obs." + HUAWEI_ENDPOINT;

  String AUTH_TOKEN_HEADER = "X-Subject-Token";

  String MODELARTS_CN_NORTH_V1_ENDPOINT = "https://modelarts." + HUAWEI_ENDPOINT + "/v1/";

  String OBS_URL_SUFFIX = "obs.myhwclouds.com";

  String MODELARTS_TRAINING_REST = "training-jobs";

  String MODELARTS_MODEL = "models";

  String MODELARTS_SERVICES = "services";

  String SEPARATOR = "/";

  String MODELARTS_TRAINING_VERSIONS = "versions";

}
