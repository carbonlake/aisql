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

package org.apache.spark.sql.aisql.image

import org.apache.spark.sql.SparkSession
import Utils.{fromBytes, toBytes}


object VisionSparkUDFs {

  /**
   * Utility method to register all Vision-related UDFs at once
   *
   * For more fine grained control over the parameters, consider registering UDFs manually
   * instead of calling this method
   *
   * @param sparkSession
   */
  def registerAll(sparkSession: SparkSession): Unit = {
    registerCrop(sparkSession)
    registerResize(sparkSession)
    registerRotate(sparkSession)
    registerRotateWithBgColor(sparkSession)
    registerAddNoise(sparkSession)
  }


  /**
   * Image crop
   *
   * @param sparkSession
   */
  def registerCrop(sparkSession: SparkSession): Unit = {
    sparkSession.udf.register("crop",
      (bytes: Array[Byte], x: Int, y: Int, w: Int, h: Int) => {
        val img = fromBytes(bytes)
        val croppedImg = BasicTransformations.crop(img, x, y, w, h)
        toBytes(croppedImg)
      })
  }


  /**
   * Image resize
   *
   * @param sparkSession
   */
  def registerResize(sparkSession: SparkSession): Unit = {
    sparkSession.udf.register("resize",
      (bytes: Array[Byte], w: Int, h: Int) => {
        val img = fromBytes(bytes)
        val resizedImg = BasicTransformations.resize(img, w, h)
        toBytes(resizedImg)
      })
  }


  /**
   * Image rotate (with white background color)
   *
   * @param sparkSession
   */
  def registerRotate(sparkSession: SparkSession): Unit = {
    sparkSession.udf.register("rotate",
      (bytes: Array[Byte], angle: Int) => {
        val img = fromBytes(bytes)
        val rotatedImg = GeometricTransformations.rotate(img, angle)
        toBytes(rotatedImg)
      })
  }


  /**
   * Image rotate with custom background color
   *
   * @param sparkSession
   */
  def registerRotateWithBgColor(sparkSession: SparkSession): Unit = {
    sparkSession.udf.register("rotateWithBgColor",
      (bytes: Array[Byte], angle: Int, bgColor: String) => {
        val img = fromBytes(bytes)
        val rotatedImg = GeometricTransformations.rotate(img, angle, bgColor)
        toBytes(rotatedImg)
      })
  }


  /**
   * Add random noise to image
   *
   * @param sparkSession
   */
  def registerAddNoise(sparkSession: SparkSession): Unit = {
    sparkSession.udf.register("addRandomNoise",
      (bytes: Array[Byte]) => {
        val img = fromBytes(bytes)
        val noisyImg = DataAugmentor.addRandomNoise(img, 1, 0.5)
        toBytes(noisyImg)
      })
  }

}
