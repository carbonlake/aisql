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

import org.opencv.core.{Core, CvType, Mat, Point, Scalar, Size}
import Utils.lucky
import org.opencv.imgproc.Imgproc


/**
 * Augment image with random noise and transformations
 */
object DataAugmentor {

  def drawRandomLines(img: Mat, numLines: Int, thickness: Int): Unit = {
    val (width, height) = (img.size().width, img.size().height)
    (1 to numLines).foreach { _ =>
      val pt1 = new Point(width * Random.nextDouble(), height * Random.nextDouble())
      val pt2 = new Point(width * Random.nextDouble(), height * Random.nextDouble())
      val color = new Scalar(Random.nextDouble() * 255,
        Random.nextDouble() * 255,
        Random.nextDouble() * 255)
      Imgproc.line(img, pt1, pt2, color, thickness)
    }
  }


  def shiftColor(img: Mat, delta: Double): Unit = {
    val deltaScalar = new Scalar(delta, delta, delta)
    Core.add(img, deltaScalar, img)
  }


  def addBlurNoise(img: Mat, shrinkScale: Double): Unit = {
    val shrinkedImg = new Mat()

    // Shrink image
    Imgproc.resize(img, shrinkedImg, new Size(), shrinkScale, shrinkScale, Imgproc.INTER_AREA)

    // Expand shrinked image
    Imgproc.resize(shrinkedImg, img, img.size(), 0, 0, Imgproc.INTER_CUBIC)
  }


  def addGaussianBlur(img: Mat, kernelSize: Int): Unit = {
    assert(kernelSize % 2 == 1, "kernelSize must be odd")
    Imgproc.GaussianBlur(img, img, new Size(kernelSize, kernelSize), 0, 0)
  }


  def addGaussianNoise(img: Mat, std: Double): Unit = {
    // Generate noise from Normal distribution
    val noise = new Mat(img.size(), CvType.CV_32FC3)
    Core.randn(noise, 0.0, std)

    // Add noise to image
    Core.add(img, noise, img, new Mat(), CvType.CV_8UC3)
  }


  def addSaltPepperNoise(img: Mat, ratio: Double): Unit = {
    val numPixelsToChange: Int = (img.total() * ratio).asInstanceOf[Int]
    (1 to numPixelsToChange).foreach { _ =>
      val x = Random.nextInt(img.size().width.asInstanceOf[Int])
      val y = Random.nextInt(img.size().height.asInstanceOf[Int])
      val noiseVal = Random.nextInt(256).asInstanceOf[Byte]
      img.put(y, x, Array(noiseVal, noiseVal, noiseVal))
    }
  }


  def enhanceSharpness(img: Mat, kernelSize: Int, beta: Double = 0.5): Unit = {
    assert(beta >= 0)

    // Generate blurred image
    val blurImg = img.clone()
    addGaussianBlur(blurImg, kernelSize)

    // Subtract blur image from original image to create sharpened image
    Core.addWeighted(img, 1 + beta, blurImg, -beta, 0, img)
  }


  def enhanceContrast(img: Mat, severity: Double): Unit = {
    PixelOperations.enhanceContrast(img, severity)
  }


  def enhanceBrightness(img: Mat, delta: Double): Unit = {
    PixelOperations.enhanceBrightness(img, delta)
  }


  def enhanceSaturation(img: Mat, delta: Double): Unit = {
    PixelOperations.enhanceSaturation(img, delta)
  }


  /**
   * Convenience method to add random noise in the image
   *
   * For more fine-grained control over noise, manually call each of the methods individually
   *
   * @param img input image
   * @param prob probability of adding a particular
   * @param severity extent of image alteration per operation
   * @param addBlur boolean representing whether consider blur as noise
   * @return Noisy image
   */
  def addRandomNoise(
      img: Mat,
      prob: Double = 0.25,
      severity: Double = 0.25,
      addBlur: Boolean = false): Mat = {

    require(severity >= 0.0 && severity <= 1.0,
      s"severity expected between [0.0, 1.0], found: $severity")

    require(prob >= 0.0 && prob <= 1.0,
      s"prob expected between [0.0, 1.0], found: $prob")

    require(img.`type`() == CvType.CV_8UC3, "Only BGR images supported")

    val noisyImg = img.clone()

    if (lucky(prob)) {
      val numLines = (severity * 10).asInstanceOf[Int]
      val thickness = if (lucky(severity)) 2 else 1
      drawRandomLines(noisyImg, numLines, thickness)
    }

    if (lucky(prob)) {
      val delta = Utils.clip(if (lucky(0.5)) {
        -(severity * 10)
      } else {
        severity * 10
      }, -5, 5)
      shiftColor(noisyImg, delta)
    }

    if (addBlur && lucky(prob)) {
      val scale = 1.0 - 0.5 * severity
      addBlurNoise(noisyImg, shrinkScale = scale)
    }

    if (addBlur && lucky(prob)) {
      var kernel: Int = (11 * severity).asInstanceOf[Int]
      if(kernel % 2 == 0) {
        kernel = kernel + 1
      }
      addGaussianBlur(noisyImg, kernelSize = kernel)
    }

    if (lucky(prob)) {
      val std = severity * 20.0
      addGaussianNoise(noisyImg, std = std)
    }

    if (lucky(prob)) {
      val ratioOfNoisyPixels = 0.1 * severity
      addSaltPepperNoise(noisyImg, ratioOfNoisyPixels)
    }

    if (lucky(prob)) {
      var kernel: Int = (11 * severity).asInstanceOf[Int]
      if(kernel % 2 == 0) {
        kernel = kernel + 1
      }
      enhanceSharpness(noisyImg, kernel)
    }

    if (lucky(prob)) {
      enhanceContrast(noisyImg, severity)
    }

    if (lucky(prob)) {
      val delta = severity * 50
      enhanceBrightness(noisyImg, delta)
    }

    if (lucky(prob)) {
      val delta = severity * 50
      enhanceSaturation(noisyImg, delta)
    }

    noisyImg
  }

}
