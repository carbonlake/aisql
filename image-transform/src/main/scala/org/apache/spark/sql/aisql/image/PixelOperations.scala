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

import java.util

import org.opencv.core.{Core, CvType, Mat, Scalar, Size}
import org.opencv.imgproc.{CLAHE, Imgproc}


object PixelOperations {

  def enhanceContrast(img: Mat, severity: Double, clipLimit_CLAHE: Double = 20.0): Unit = {
    // Convert from RGB to LAB image space
    val imgLAB = new Mat(img.size(), CvType.CV_8UC3)
    Imgproc.cvtColor(img, imgLAB, Imgproc.COLOR_BGR2Lab)

    // Split channels in LAB image
    val lab: java.util.List[Mat] = new util.ArrayList[Mat]()
    Core.split(imgLAB, lab)
    val l = lab.get(0)

    // Apply contrast enhancement
    val clahe: CLAHE = Imgproc.createCLAHE(clipLimit_CLAHE, new Size(8, 8))
    clahe.apply(l, l)

    // Rebuild final LAB image
    lab.set(0, l)
    val convertedImgLAB = new Mat(img.size(), CvType.CV_8UC3)
    Core.merge(lab, convertedImgLAB)

    // Convert back from LAB to BGR image space
    val convertedImgRGB = new Mat(img.size(), CvType.CV_8UC3)
    Imgproc.cvtColor(convertedImgLAB, convertedImgRGB, Imgproc.COLOR_Lab2BGR)

    // Take weighted average of original image and sharpened image according to severity
    Core.addWeighted(convertedImgRGB, severity, img, 1 - severity, 0, img)
  }


  def enhanceBrightness(img: Mat, delta: Double): Unit = {
    // Convert from RGB to HSV image space
    val imgHSV = new Mat(img.size(), CvType.CV_8UC3)
    Imgproc.cvtColor(img, imgHSV, Imgproc.COLOR_BGR2HSV)

    // Split channels from HSV image
    val hsv: java.util.List[Mat] = new util.ArrayList[Mat]()
    Core.split(imgHSV, hsv)
    val v = hsv.get(2)

    // Increase brightness
    Core.add(v, new Scalar(delta), v)

    // Rebuild final HSV image
    val convertedImgHSV = new Mat(img.size(), CvType.CV_8UC3)
    Core.merge(hsv, convertedImgHSV)

    // Convert back from HSV to BGR image space
    Imgproc.cvtColor(convertedImgHSV, img, Imgproc.COLOR_HSV2BGR)
  }


  def enhanceSaturation(img: Mat, delta: Double): Unit = {
    // Convert from RGB to HSV image space
    val imgHSV = new Mat(img.size(), CvType.CV_8UC3)
    Imgproc.cvtColor(img, imgHSV, Imgproc.COLOR_BGR2HSV)

    // Split channels from HSV image
    val hsv: java.util.List[Mat] = new util.ArrayList[Mat]()
    Core.split(imgHSV, hsv)
    val s = hsv.get(1)

    // Increase saturation
    Core.add(s, new Scalar(delta), s)

    // Rebuild final HSV image
    val convertedImgHSV = new Mat(img.size(), CvType.CV_8UC3)
    Core.merge(hsv, convertedImgHSV)

    // Convert back from HSV to BGR image space
    Imgproc.cvtColor(convertedImgHSV, img, Imgproc.COLOR_HSV2BGR)
  }

}
