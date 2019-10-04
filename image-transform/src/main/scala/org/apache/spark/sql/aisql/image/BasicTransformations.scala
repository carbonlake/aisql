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

import org.opencv.core.{Mat, Rect, Size}
import org.opencv.imgproc.Imgproc


object BasicTransformations extends Serializable {

  /**
   * Image resize
   *
   * @param img image matrix (OpenCV Mat)
   * @param w width of output image
   * @param h height of output image
   * @return image with size (w, h)
   */
  def resize(img: Mat, w: Int, h: Int): Mat = {
    val resizedImg = new Mat
    val newSize = new Size(w, h)
    Imgproc.resize(img, resizedImg, newSize)
    resizedImg
  }

  /**
   * Image crop
   *
   * @param img image matrix (OpenCV Mat)
   * @param x left edge of cropped image
   * @param y top edge of cropped image
   * @param w width of cropped image
   * @param h height of cropped image
   * @return cropped image
   */
  def crop(img: Mat, x: Int, y: Int, w: Int, h: Int): Mat = {
    val cropRect = new Rect(x, y, w, h)
    val croppedImg = new Mat(img, cropRect)
    croppedImg
  }

}
