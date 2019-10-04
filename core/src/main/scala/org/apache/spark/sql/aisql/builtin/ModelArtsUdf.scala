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

package org.apache.spark.sql.aisql.builtin

import io.carbonlake.aisql.job.TrainModelDetail
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.aisql.AISQLExtension
import org.apache.spark.sql.expressions.UserDefinedFunction
/**
 * Util of Model Arts Udf
 */
object ModelArtsUdf {

  /**
   * register ModelArts UDF
   */
  def register(
      sparkSession: SparkSession,
      modelDetail: TrainModelDetail,
      udfName: String): UserDefinedFunction = {
    val modelAPI = AISQLExtension.modelTraingAPI
    val params = modelAPI.parseConfig(modelDetail.getProperties)
    val url = modelDetail.getProperties.get("access_address")
    if (params.getParams.size() == 1) {
      sparkSession.udf.register(udfName, (in1: AnyRef) => {
        params.getParams.get(0).setParamValue(in1)
        modelAPI.queryService(params, url)
      })
    } else if (params.getParams.size() == 2) {
      sparkSession.udf.register(udfName, (in1: AnyRef, in2: AnyRef) => {
        params.getParams.get(0).setParamValue(in1)
        params.getParams.get(1).setParamValue(in2)
        modelAPI.queryService(params, url)
      })
    } else if (params.getParams.size() == 3) {
      sparkSession.udf.register(udfName, (in1: AnyRef, in2: AnyRef, in3: AnyRef) => {
        params.getParams.get(0).setParamValue(in1)
        params.getParams.get(1).setParamValue(in2)
        params.getParams.get(2).setParamValue(in3)
        modelAPI.queryService(params, url)
      })
    } else if (params.getParams.size() == 4) {
      sparkSession.udf.register(udfName, (in1: AnyRef, in2: AnyRef, in3: AnyRef, in4: AnyRef) => {
        params.getParams.get(0).setParamValue(in1)
        params.getParams.get(1).setParamValue(in2)
        params.getParams.get(2).setParamValue(in3)
        params.getParams.get(3).setParamValue(in4)
        modelAPI.queryService(params, url)
      })
    } else {
      throw new UnsupportedOperationException("Udf with more than 4 parameters are not supported")
    }
  }

}


