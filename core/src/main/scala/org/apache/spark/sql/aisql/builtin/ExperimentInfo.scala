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

import java.util

import scala.collection.JavaConverters._

import io.carbonlake.aisql.job.{NoSuchExperimentException, TrainModelManager}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.aisql.ExperimentStoreManager
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{MultiInstanceRelation, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, GenericInternalRow, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.unsafe.types.UTF8String

import org.apache.carbondata.core.constants.CarbonCommonConstants

/**
 * Logical plan for ExperimentInfo TVF
 */
case class ExperimentInfo(output: Seq[Attribute], param: ExperimentInfoParams)
  extends LeafNode with MultiInstanceRelation {
  override def newInstance(): ExperimentInfo = copy(output = output.map(_.newInstance()))
}

class ExperimentInfoParams(exprs: Seq[Expression]) {
  val experimentName: String = exprs.map(_.asInstanceOf[UnresolvedAttribute]).head.name

  def toSeq: Seq[String] = Seq(experimentName)
}

/**
 * Physical plan for ExperimentInfo TVF
 */
case class ExperimentInfoExec(
    session: SparkSession,
    experimentInfo: ExperimentInfo) extends LeafExecNode {

  override protected def doExecute(): RDD[InternalRow] = {
    val projection = UnsafeProjection.create(output.map(_.dataType).toArray)
    val experimentName = experimentInfo.param.experimentName
    val ifExperimentExists = ExperimentStoreManager.getInstance.getAllExperimentSchemas.asScala
      .exists(m => m.getDataMapName.equalsIgnoreCase(experimentName))
    if (ifExperimentExists) {
      // get all training jobs started on experiment
      val jobs = TrainModelManager.getAllEnabledTrainedModels(experimentName)
      val metrics = new util.ArrayList[String]()
      jobs.foreach{ job =>
        val currentJob = Array(job.getJobName,
          job.getProperties.asScala.toSeq.sorted.mkString(","), job.getStatus).mkString("|")
        metrics.add(currentJob)
      }
      val rows = metrics.asScala.map { line =>
        val splits = line.split('|').map(UTF8String.fromString)
        projection(new GenericInternalRow(splits.asInstanceOf[Array[Any]])).copy()
      }
      session.sparkContext.makeRDD(rows)
    } else {
     throw new NoSuchExperimentException(experimentName)
    }
  }

  override def output: Seq[Attribute] = experimentInfo.output
}
