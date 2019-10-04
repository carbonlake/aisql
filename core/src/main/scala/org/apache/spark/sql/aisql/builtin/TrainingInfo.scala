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

import io.carbonlake.aisql.job.{NoSuchExperimentException, TrainModelManager}
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
 * Logical plan for JobMetrics TVF
 */
case class TrainingInfo(output: Seq[Attribute], param: TrainingInfoParams)
  extends LeafNode with MultiInstanceRelation {
  override def newInstance(): TrainingInfo = copy(output = output.map(_.newInstance()))
}

class TrainingInfoParams(expressions: Seq[Expression]) {
  val jobName: String = expressions.map(_.asInstanceOf[UnresolvedAttribute]).head.name

  def toSeq: Seq[String] = Seq(jobName)
}

/**
 * Physical plan for JobMetrics TVF
 */
case class JobMetricsExec(
    session: SparkSession,
    jobMetrics: TrainingInfo) extends LeafExecNode {

  override protected def doExecute(): RDD[InternalRow] = {
    val projection = UnsafeProjection.create(output.map(_.dataType).toArray)
    val job = jobMetrics.param.jobName
    if (!job.contains(".")) {
      throw new AnalysisException("Experiment name with model name must be provided")
    }
    val experimentName = job.substring(0, job.indexOf("."))
    val modelName = job.substring(job.indexOf(".") + 1, job.length)

    val ifExperimentExists = ExperimentStoreManager.getInstance.getAllExperimentSchemas.asScala
      .exists(m => m.getDataMapName.equalsIgnoreCase(experimentName))
    if (ifExperimentExists) {
      val trainJob = TrainModelManager.getTrainModel(experimentName, modelName)
      if (null != trainJob) {
        val trainingInfo = AISQLExtension.modelTraingAPI
          .getTrainingJobInfo(java.lang.Long.parseLong(trainJob.getProperties.get("job_id")))
        val metrics = Array(UTF8String.fromString(trainJob.getProperties.get("job_id")),
          UTF8String.fromString(trainJob.getJobName),
          UTF8String.fromString(trainingInfo.get("status")),
          UTF8String.fromString(trainingInfo.get("duration")))
        val rows = projection(new GenericInternalRow(metrics.asInstanceOf[Array[Any]]))
        session.sparkContext.makeRDD(Array(rows))
      } else {
        throw new AnalysisException(
          "Model with name " + modelName + " does not exist on Experiment: " + experimentName)
      }
    } else {
      throw new NoSuchExperimentException(experimentName)
    }
  }

  override def output: Seq[Attribute] = jobMetrics.output
}

