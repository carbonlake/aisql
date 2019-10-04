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

import scala.util.Random

import org.apache.spark.sql.SQLContext
import org.opencv.core.{Mat, MatOfByte, MatOfInt}
import org.opencv.imgcodecs.Imgcodecs


/**
 * Utility methods
 */
object Utils {

  /**
   * Convert byte-array to image
   *
   * @param bytes Byte-Array of image
   * @return Image Matrix (OpenCV Mat)
   */
  def fromBytes(bytes: Array[Byte]): Mat = {
    Imgcodecs.imdecode(new MatOfByte(bytes: _*), Imgcodecs.CV_LOAD_IMAGE_UNCHANGED)
  }


  /**
   * Convert Image matrix (OpenCV Mat) to byte array
   *
   * @param img Image
   * @return Byte-Array
   */
  def toBytes(img: Mat): Array[Byte] = {
    // Output quality
    val imageQuality = new MatOfInt(Imgcodecs.CV_IMWRITE_JPEG_QUALITY, 95)

    // Encode image for sending back
    val matOfByte = new MatOfByte
    Imgcodecs.imencode(".jpg", img, matOfByte, imageQuality)
    matOfByte.toArray
  }


  /**
   * Generate true/false based of the given parameter prob
   *
   * @param prob Probability of generating a true
   * @return true/false
   */
  def lucky(prob: Double): Boolean = {
    Random.nextDouble() < prob
  }


  /**
   * Method to clip (limit) a value between a range.
   *
   * Given an interval, values outside the interval are clipped to the interval edges.
   * For example, if an interval of [0, 1] is specified, values smaller than 0 become 0,
   * and values larger than 1 become 1.
   *
   * @param value Value to be restricted
   * @param min Left edge of the interval
   * @param max Right edge of the interval
   * @return Clipped value
   */
  def clip[T](value: T, min: T, max: T)(implicit numeric: Numeric[T]): T = {
    if (numeric.lt(value, min)) {
      return min
    }
    if (numeric.gt(value, max)) {
      return max
    }
    value
  }

}
