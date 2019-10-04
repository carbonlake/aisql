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

package com.huawei.cloud.ocr

import com.huawei.cloud.CloudConstants
import org.apache.spark.sql.{CloudUtils, SparkSession, UDFRegistration}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.util.CarbonProperties

/**
 * register cloud UDF into spark
 */
object CloudUdfRegister {

  val LOGGER = LogServiceFactory.getLogService(CloudUdfRegister.getClass.getCanonicalName)

  def register(sparkSession: SparkSession): Unit = {
    LOGGER.info("starting the registration of cloud udf")
    // ocr
    OCRUdf.register(sparkSession)
    LOGGER.info("finished the registration of cloud udf")
  }

  /**
   *  get the configuration of the cloud
   */
  def getCloudConf(sparkSession: SparkSession): (String, String, String, String) = {
    val properties = CarbonProperties.getInstance()
    val endPoint = properties.getProperty(CloudConstants.CLOUD_ENDPOINT)
    val region = properties.getProperty(CloudConstants.CLOUD_REGION)
    val hadoopConf = CloudUtils.sessionState(sparkSession).newHadoopConf()
    val ak = properties.getProperty(
      CloudConstants.CLOUD_ACCESS_KEY,
      hadoopConf.get("fs.s3a.access.key"))
    val sk = properties.getProperty(
      CloudConstants.CLOUD_SECRET_KEY,
      hadoopConf.get("fs.s3a.secret.key"))
    (endPoint, region, ak, sk)
  }
}
