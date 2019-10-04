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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.aisql.PythonExecUtil
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions.{Attribute, EqualTo, Expression, GenericInternalRow, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.unsafe.types.UTF8String

/**
 * Logical plan for WebSearch TVF
 */
case class WebSearch(output: Seq[Attribute], param: WebSearchParams)
  extends LeafNode with MultiInstanceRelation {
  override def newInstance(): WebSearch = copy(output = output.map(_.newInstance()))
}

class WebSearchParams(exprs: Seq[Expression]) {
  val params = exprs.map(_.asInstanceOf[EqualTo])
  val keyWord = params.find(_.left.simpleString.equals("key_word")).map(_.right.simpleString)
    .getOrElse(throw new AnalysisException("missing 'key_word' parameter for WebSearch"))
  val pageNum = params.find(_.left.simpleString.equals("page_num")).map(_.right.simpleString)
    .getOrElse(throw new AnalysisException("missing 'page_num' parameter for WebSearch"))

  def toSeq: Seq[String] = Seq(keyWord, pageNum)
}

/**
 * Physical plan for WebSearch TVF
 */
case class WebSearchExec(
    session: SparkSession,
    webSearch: WebSearch) extends LeafExecNode {
  private val script = "/Users/jacky/code/carbondata/fleet/router/src/main/python/web_search.py"

  override protected def doExecute(): RDD[InternalRow] = {
    // build a process to execute the web_search.py
    val projection = UnsafeProjection.create(output.map(_.dataType).toArray)
    val inBinary =
      PythonExecUtil.runPythonScript(session, Seq.empty.toArray, script, webSearch.param.toSeq)
    val lines = new String(inBinary).split("\n")
    val rows = lines.map { line =>
      val splitted = line.split('|').map(UTF8String.fromString)
      projection(new GenericInternalRow(splitted.asInstanceOf[Array[Any]])).copy()
    }
    session.sparkContext.makeRDD(rows)
  }

  override def output: Seq[Attribute] = webSearch.output
}
