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

import io.carbonlake.aisql.job.{NoSuchExperimentException, TrainModelManager}
import org.apache.spark.sql.aisql.{AISQLExtension, ExperimentStoreManager}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.{Row, SparkSession}


case class DropExperimentCommand(
    experimentName: String,
    ifExists: Boolean)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val experimentSchemas = ExperimentStoreManager.getInstance.getAllExperimentSchemas
    val ifExperimentExists = experimentSchemas.asScala
      .exists(experiment => experiment.getDataMapName.equalsIgnoreCase(experimentName))
    if (ifExperimentExists) {
      val details = TrainModelManager.getAllEnabledTrainedModels(experimentName)
      details.foreach { d =>
        val jobId = d.getProperties.get("job_id")
        val modelId = d.getProperties.getOrDefault("model_id", "")
        val serviceId = d.getProperties.getOrDefault("service_id", "")
        val udf = d.getProperties.getOrDefault("udfName", "")
        if (!udf.isEmpty && !serviceId.isEmpty && !modelId.isEmpty) {
          sparkSession.sessionState.catalog.dropTempFunction(udf, true)
          AISQLExtension.modelTraingAPI.deleteModelService(serviceId)
          AISQLExtension.modelTraingAPI.deleteModel(modelId)
        }
        AISQLExtension.modelTraingAPI.stopTrainingJob(jobId.toLong)
      }
      TrainModelManager.dropModel(experimentName)
      ExperimentStoreManager.getInstance.dropExperimentSchema(experimentName)
    } else {
      if (!ifExists) {
        throw new NoSuchExperimentException(experimentName)
      }
    }
    Seq.empty
  }
}
