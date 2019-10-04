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

package org.apache.spark.sql.aisql.command

import scala.collection.JavaConverters._

import io.carbonlake.aisql.job.TrainModelManager
import org.apache.spark.sql.aisql.{AISQLExtension, ExperimentStoreManager}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}

case class UnregisterModelCommand(
    experimentName: String,
    modelName: String)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    // get experiment and model details
    val modelSchemas = ExperimentStoreManager.getInstance.getAllExperimentSchemas
    val model = modelSchemas.asScala
      .find(model => model.getDataMapName.equalsIgnoreCase(experimentName))
    val schema = model.getOrElse(
      throw new AnalysisException(
        "Experiment with name " + experimentName + " doesn't exists in storage"))
    val details = TrainModelManager.getAllTrainedModels(experimentName)
    val jobDetail = details.find(_.getJobName.equalsIgnoreCase(modelName)).
      getOrElse(throw new AnalysisException(
        "Model with name " + modelName + " doesn't exists on experiment " + experimentName))
    var modelId = ""
    var serviceId = ""
    try {
      modelId = jobDetail.getProperties.get("model_id")
      serviceId = jobDetail.getProperties.get("service_id")
    } catch {
      case e: Exception =>
        throw new AnalysisException(
          "Error while getting Model_Id/Service_Id")
    }
    if (!modelId.isEmpty && !serviceId.isEmpty) {
      // unregister udf function from spark
      sparkSession.sessionState.catalog
        .dropTempFunction(jobDetail.getProperties.get("udfName"), false)
      // delete a model service
      AISQLExtension.modelTraingAPI.deleteModelService(serviceId)
      // delete the model from modelArts
      AISQLExtension.modelTraingAPI.deleteModel(modelId)
    }
    Seq.empty
  }
}
