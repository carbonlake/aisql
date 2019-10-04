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

import java.io.{ByteArrayOutputStream, File, InputStream}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.api.python.PythonUtils
import org.apache.spark.sql.SparkSession

/**
 * Utility to run python script
 */
object PythonExecUtil {

  def runPythonScript(
      spark: SparkSession,
      libraryIncludes: Array[String],
      scriptPath: String,
      scriptArgs: Seq[String] = Seq.empty): Array[Byte] = {
    val pathElements = new ArrayBuffer[String]
    pathElements += PythonUtils.sparkPythonPath
    pathElements += sys.env.getOrElse("PYTHONPATH", "")
    pathElements += Seq(sys.env("SPARK_HOME"), "python").mkString(File.separator)
    pathElements ++= libraryIncludes
    val pythonPath = PythonUtils.mergePythonPaths(pathElements: _*)

    val pythonExec = spark.sparkContext.getConf.get("spark.python.exec", "python")

    val pb = new ProcessBuilder((Seq(pythonExec, scriptPath) ++ scriptArgs).asJava)
    val workerEnv = pb.environment()
    workerEnv.put("PYTHONPATH", pythonPath)
    // This is equivalent to setting the -u flag; we use it because ipython doesn't support -u:
    workerEnv.put("PYTHONUNBUFFERED", "YES")
    val worker = pb.start()

    val stream = worker.getInputStream
    val errorStream = worker.getErrorStream
    worker.waitFor()
    val inBinary = getBinary(stream)
    val errBinary = getBinary(errorStream)

    if (errBinary.length  > 0) {
      throw new Exception(new String(errBinary))
    }
    worker.destroy()
    inBinary
  }

  private def getBinary(stream: InputStream): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    while (stream.available() > 0) {
      out.write(stream.read())
    }
    stream.close()
    out.toByteArray
  }

}
