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

import io.carbonlake.aisql.job.{TrainModelDetail, TrainModelManager}
import org.apache.spark.sql.aisql.{AISQLExtension, ExperimentStoreManager}
import org.apache.spark.sql.aisql.intf.DataScan
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.ObjectSerializationUtil

case class CreateModelCommand(
    modelName: String,
    experimentName: String,
    options: Map[String, String],
    ifNotExists: Boolean)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    // check if model with modelName already exists
    val experimentSchemas = ExperimentStoreManager.getInstance.getAllExperimentSchemas
    val experiment = experimentSchemas.asScala
      .find(model => model.getDataMapName.equalsIgnoreCase(experimentName))
    val schema = experiment.getOrElse(
      throw new AnalysisException(
        "Experiment with name " + experimentName + " does not exist"))

    ModelUtil.validateOptions(options)
    val optionsMap = new java.util.HashMap[String, String]()
    optionsMap.putAll(options.asJava)
    val details = TrainModelManager.getAllTrainedModels(experimentName)
    if (details.exists(_.getJobName.equalsIgnoreCase(modelName))) {
      if (!ifNotExists) {
        throw new AnalysisException(
          "Model with name " + modelName + " already exists on Experiment " + experimentName)
      } else {
        Seq.empty
      }
    }
    val optionsMapFinal = new java.util.HashMap[String, String]()
    optionsMapFinal.putAll(optionsMap)
    optionsMapFinal.putAll(schema.getProperties)
    val str = schema.getProperties.get(ExperimentStoreManager.QUERY_OBJECT)
    val queryObject =
      ObjectSerializationUtil.convertStringToObject(str).asInstanceOf[DataScan]
    // It starts creating the training job and generates the model in cloud.
    val jobId =
      AISQLExtension.modelTraingAPI.startTrainingJob(
        optionsMapFinal,
        experimentName + CarbonCommonConstants.UNDERSCORE + modelName,
        queryObject)
    optionsMap.put("job_id", jobId.toString)
    val detail = new TrainModelDetail(modelName, optionsMap)
    try {
      // store experiment schema
      TrainModelManager.saveTrainModel(experimentName, detail)
    } catch {
      case e: Exception =>
        AISQLExtension.modelTraingAPI.stopTrainingJob(jobId)
        throw e
    }

    Seq.empty
  }
}
