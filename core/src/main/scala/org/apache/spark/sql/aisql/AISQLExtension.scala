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

package org.apache.spark.sql.aisql

import scala.collection.JavaConverters._

import io.carbonlake.aisql.job.TrainModelManager
import org.apache.spark.sql.aisql.builtin.DownloadUdf
import org.apache.spark.sql.aisql.builtin.ModelArtsUdf.register
import org.apache.spark.sql.aisql.intf.ModelAPI
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}

import org.apache.carbondata.common.logging.LogServiceFactory

class AISQLExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectParser(AISQLExtension.createParser)
  }
}

object AISQLExtension {
  private var init = false
  private val parser = new AISQLParser

  def initIfRequired(session: SparkSession): Unit = {
    if (!init) {
      init = true
      registerExistingModelArtsServing(session)
      registerBuiltinUDF(session)
    }
  }

  def createParser(session: SparkSession, parserInterface: ParserInterface): ParserInterface = {
    initIfRequired(session)
    new ParserInterface {
      override def parsePlan(sqlText: String): LogicalPlan =
        try {
          parser.parse(sqlText)
        } catch {
          case t: Throwable => parserInterface.parsePlan(sqlText)
        }

      override def parseExpression(sqlText: String): Expression =
        parserInterface.parseExpression(sqlText)

      override def parseTableIdentifier(sqlText: String): TableIdentifier =
        parserInterface.parseTableIdentifier(sqlText)

      override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier =
        parserInterface.parseFunctionIdentifier(sqlText)

      override def parseTableSchema(sqlText: String): StructType =
        parserInterface.parseTableSchema(sqlText)

      override def parseDataType(sqlText: String): DataType =
        parserInterface.parseDataType(sqlText)
    }
  }

  private def registerBuiltinUDF(sesssion: SparkSession): SparkSession = {
    val download: String => Array[Byte] = DownloadUdf.download
    sesssion.udf.register("download", download)
    sesssion
  }

  private lazy val modelTrainingAPIInstance = {
    val loader = this.getClass.getClassLoader
    loader.loadClass("com.huawei.cloud.modelarts.ModelArtsModelAPI")
      .asInstanceOf[ModelAPI]
  }

  def modelTraingAPI: ModelAPI = modelTrainingAPIInstance

  /**
   * register ModelArts UDF to spark
   */
  private def registerExistingModelArtsServing(session: SparkSession): SparkSession = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
    val experimentSchemas = ExperimentStoreManager.getInstance.getAllExperimentSchemas
    LOGGER.info("Started registering all ModelArts UDF to spark")
    experimentSchemas.asScala.foreach {
      experimentSchema =>
        val trainingJobDetails =
          TrainModelManager.getAllTrainedModels(experimentSchema.getDataMapName)
        trainingJobDetails.foreach {
          jobDetail =>
            val udfName = jobDetail.getProperties.getOrDefault("udfName", "")
            if (!udfName.isEmpty) {
              register(session, jobDetail, udfName)
            }
        }
    }
    LOGGER.info("Finished registering all ModelArts UDF to spark")
    session
  }
}