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

import scala.util.matching.Regex

import org.apache.spark.sql.aisql.command.{CreateExperimentCommand, CreateModelCommand, DropExperimentCommand, DropModelCommand, RegisterModelCommand, RunScriptCommand, UnregisterModelCommand}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.management.{CarbonAlterTableCompactionCommand, CarbonLoadDataCommand}
import org.apache.spark.sql.parser.CarbonSpark2SqlParser
import org.apache.spark.sql.util.CarbonException

import org.apache.carbondata.spark.util.CarbonScalaUtil

class AISQLParser extends CarbonSpark2SqlParser {

  protected val MODEL: Regex = aisqlKeyWord("MODEL")
  protected val EXPERIMENT: Regex = aisqlKeyWord("EXPERIMENT")
  protected val REGISTER: Regex = aisqlKeyWord("REGISTER")
  protected val UNREGISTER: Regex = aisqlKeyWord("UNREGISTER")
  protected val RUN: Regex = aisqlKeyWord("RUN")
  protected val SCRIPT: Regex = aisqlKeyWord("SCRIPT")
  protected val PYFUNC: Regex = aisqlKeyWord("PYFUNC")
  protected val PARAMS: Regex = aisqlKeyWord("PARAMS")
  protected val OUTPUT: Regex = aisqlKeyWord("OUTPUT")
  protected val PYTHON: Regex = aisqlKeyWord("PYTHON")
  protected val ENV: Regex = aisqlKeyWord("ENV")

  /**
   * This will convert key word to regular expression.
   */
  private def aisqlKeyWord(keys: String): Regex = {
    ("(?i)" + keys).r
  }

  override def parse(input: String): LogicalPlan = {
    synchronized {
      // Initialize the Keywords.
      initLexical
      phrase(start)(new lexical.Scanner(input)) match {
        case Success(plan, _) =>
          CarbonScalaUtil.cleanParserThreadLocals()
          plan match {
            case x: CarbonLoadDataCommand =>
              x.inputSqlString = input
              x
            case x: CarbonAlterTableCompactionCommand =>
              x.alterTableModel.alterSql = input
              x
            case logicalPlan => logicalPlan
          }
        case failureOrError =>
          CarbonScalaUtil.cleanParserThreadLocals()
          CarbonException.analysisException(failureOrError.toString)
      }
    }
  }

  override protected lazy val start: Parser[LogicalPlan] = explainPlan | startCommand |
                                                           modelManagement | serviceManagement |
                                                           scriptManagement

  protected lazy val modelManagement: Parser[LogicalPlan] = createModel | dropModel |
                                                            createExperiment | dropExperiment
  protected lazy val serviceManagement: Parser[LogicalPlan] = registerModel | unregisterModel

  protected lazy val scriptManagement: Parser[LogicalPlan] = runScript

  /**
   * CREATE EXPERIMENT [IF NOT EXISTS] experimentName
   * OPTIONS (...)
   * AS select_query
   */
  protected lazy val createExperiment: Parser[LogicalPlan] =
    CREATE ~> EXPERIMENT ~> opt(IF ~> NOT ~> EXISTS) ~ ident ~
    (OPTIONS ~> "(" ~> repsep(keyValueOptions, ",") <~ ")").? ~
    (AS ~> restInput) <~ opt(";") ^^ {
      case ifNotExists ~ experimentName ~ options ~ query =>
        val optionMap = options.getOrElse(List[(String, String)]()).toMap[String, String]
        CreateExperimentCommand(experimentName, optionMap, ifNotExists.isDefined, query)
    }

  /**
   * DROP EXPERIMENT [IF EXISTS] experimentName
   */
  protected lazy val dropExperiment: Parser[LogicalPlan] =
    DROP ~> EXPERIMENT ~> opt(IF ~> EXISTS) ~ ident <~ opt(";") ^^ {
      case ifExists ~ experimentName =>
        DropExperimentCommand(experimentName, ifExists.isDefined)
    }

  /**
   * CREATE MODEL [IF NOT EXISTS] modelName
   * USING EXPERIMENT experimentName
   * OPTIONS (...)
   */
  protected lazy val createModel: Parser[LogicalPlan] =
    CREATE ~> MODEL ~> opt(IF ~> NOT ~> EXISTS) ~ ident ~
    (USING ~ EXPERIMENT ~> ident) ~
    (OPTIONS ~> "(" ~> repsep(keyValueOptions, ",") <~ ")").? <~ opt(";") ^^ {
      case ifNotExists ~ modelName ~ experimentName ~ options =>
        val optionMap = options.getOrElse(List[(String, String)]()).toMap[String, String]
        CreateModelCommand(modelName, experimentName, optionMap, ifNotExists.isDefined)
    }

  protected lazy val keyValueOptions: Parser[(String, String)] =
    (stringLit <~ "=") ~ stringLit ^^ {
      case opt ~ optvalue => (opt.trim.toLowerCase(), optvalue)
      case _ => ("", "")
    }

  protected lazy val outputList: Parser[(String, String)] =
    (ident ~ ident) ^^ {
      case fieldName ~ fieldType => (fieldName.trim.toLowerCase(), fieldType.trim.toLowerCase())
      case _ => ("", "")
    }

  /**
   * DROP MODEL [IF EXISTS] modelName on EXPERIMENT experimentName
   */
  protected lazy val dropModel: Parser[LogicalPlan] =
    DROP ~> MODEL ~> opt(IF ~> EXISTS) ~ ident ~ (ON ~ EXPERIMENT ~> ident) <~ opt(";") ^^ {
      case ifExists ~ modelName ~ experimentName =>
        DropModelCommand(modelName, experimentName, ifExists.isDefined)
    }

  /**
   * REGISTER MODEL experimentName.modelName AS udfName
   */
  protected lazy val registerModel: Parser[LogicalPlan] =
    REGISTER ~> MODEL ~> (ident <~ ".") ~ ident ~ (AS ~> ident) <~ opt(";") ^^ {
      case experimentName ~ modelName ~ udfName =>
        RegisterModelCommand(experimentName, modelName, udfName)
    }

  /**
   * UNREGISTER MODEL experimentName.modelName
   */
  protected lazy val unregisterModel: Parser[LogicalPlan] =
    UNREGISTER ~> MODEL ~> (ident <~ ".") ~ ident <~ opt(";") ^^ {
      case experimentName ~ modelName =>
        UnregisterModelCommand(experimentName, modelName)
    }

  /**
   * RUN SCRIPT path PYFUNC func
   * [WITH PARAMS ("param1"="value1", ...)]
   * [OUTPUT (fieldName fieldType, ...)]
   *
   * By default, output schema is (value string)
   */
  protected lazy val runScript: Parser[LogicalPlan] =
    RUN ~> (SCRIPT ~> stringLit) ~ (PYFUNC ~> stringLit) ~
    (WITH ~> PARAMS ~> "(" ~> repsep(keyValueOptions, ",") <~ ")").? ~
    (OUTPUT ~> "(" ~> repsep(outputList, ",") <~ ")").? <~ opt(";") ^^ {
      case scriptPath ~ funcName ~ params ~ output =>
        val paramMap = params.getOrElse(List[(String, String)]()).toMap[String, String]
        val outputMap = output.getOrElse(List[(String, String)]()).toMap[String, String]
        RunScriptCommand(scriptPath, funcName, paramMap, outputMap)
    }
}
