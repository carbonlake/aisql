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

import scala.collection.JavaConverters._
import scala.util.control.Breaks._

import io.carbonlake.aisql.job.{TrainModelDetail, TrainModelManager}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.aisql.{AISQLExtension, ExperimentStoreManager}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{MultiInstanceRelation, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, GenericInternalRow, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Logical plan for ModelInfo TVF
 */
case class ModelInfo(output: Seq[Attribute], param: ModelInfoParams)
  extends LeafNode with MultiInstanceRelation {
  override def newInstance(): ModelInfo = copy(output = output.map(_.newInstance()))
}

class ModelInfoParams(exprs: Seq[Expression]) {
  val udfName: String = exprs.map(_.asInstanceOf[UnresolvedAttribute]).head.name

  def toSeq: Seq[String] = Seq(udfName)
}

/**
 * Physical plan for ModelInfo TVF
 */
case class ModelInfoExec(
    session: SparkSession,
    udfInfo: ModelInfo) extends LeafExecNode {

  override def output: Seq[Attribute] = udfInfo.output

  override protected def doExecute(): RDD[InternalRow] = {
    val projection = UnsafeProjection.create(output.map(_.dataType).toArray)
    val udfName = udfInfo.param.udfName
    val experimentSchemas = ExperimentStoreManager.getInstance.getAllExperimentSchemas
    var jobDetail: TrainModelDetail = null
    breakable {
      experimentSchemas.asScala.foreach(experiment => {
        val jobDetails = TrainModelManager.getAllTrainedModels(experiment.getDataMapName)
        val detail = jobDetails.find(_.getProperties.containsKey("udfName"))
        if (detail.isDefined) {
          if (detail.get.getProperties.get("udfName").equalsIgnoreCase(udfName)) {
            jobDetail = detail.get
            break()
          }
        }
      })
    }
    if (null != jobDetail) {
      val modelId: String = jobDetail.getProperties.get("model_id")
      val modelDetail = AISQLExtension.modelTraingAPI.getModelInfo(modelId)
      val metrics = Array(UTF8String.fromString(modelId),
        UTF8String.fromString(modelDetail.get("modelName")),
        UTF8String.fromString(modelDetail.get("modelType")),
        UTF8String.fromString(modelDetail.get("modelSize")),
        UTF8String.fromString(modelDetail.get("modelStatus")),
        UTF8String.fromString(modelDetail.get("modelVersion")))
      val rows = projection(new GenericInternalRow(metrics.asInstanceOf[Array[Any]]))
      session.sparkContext.makeRDD(Array(rows))
    } else {
      throw new AnalysisException("Experiment or Model not found for udfName: " + udfName)
    }
  }
}
