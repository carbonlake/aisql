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

import scala.collection.JavaConverters._

import org.opencv.core.{Core, CvType, Mat, MatOfPoint, MatOfPoint2f, Point, Rect, RotatedRect, Scalar, Size}
import org.opencv.imgproc.Imgproc

object GeometricTransformations extends Serializable {

  /**
   * Image rotation
   *
   * @param img input image
   * @param angle angle of rotation in anti-clockwise direction in degrees
   * @param bgColor background color in string (default white)
   * @return rotated image
   */
  def rotate(img: Mat, angle: Int, bgColor: String = "white"): Mat = {
    // Get 2x3 affine image transformation matrix
    val rotationMatrix: Mat = computeRotationMatrix(img, angle)

    // Get background color
    val color = ColorMap.getBGRColor(bgColor)

    // Find final image size
    val center: Point = new Point(img.size().width / 2, img.size().height / 2)
    val finalImgSize: Size = new RotatedRect(center, img.size(), angle).boundingRect().size()

    // Process image rotation
    val rotatedImg: Mat = new Mat(finalImgSize, img.`type`())
    Imgproc.warpAffine(img, rotatedImg, rotationMatrix, finalImgSize,
      Imgproc.INTER_CUBIC, Core.BORDER_CONSTANT,
      new Scalar(color._1, color._2, color._3))
    rotatedImg
  }

  /**
   * Compute 2x3 affine transformation matrix for 2D rotation
   *
   * @param img   input image
   * @param angle angle of rotation in degrees
   * @return 2x3 affine transformation matrix
   */
  def computeRotationMatrix(img: Mat, angle: Int): Mat = {
    // Find image center and final image size
    val center: Point = new Point(img.size().width / 2, img.size().height / 2)
    val bbox: Rect = new RotatedRect(center, img.size(), angle).boundingRect()

    // Generate 2x3 affine image transformation matrix
    val rotationMatrix: Mat = Imgproc.getRotationMatrix2D(center, angle, 1)
    val cx: Array[Double] = rotationMatrix.get(0, 2)
    val cy: Array[Double] = rotationMatrix.get(1, 2)
    cx(0) += (bbox.width / 2) - center.x
    cy(0) += (bbox.height / 2) - center.y
    rotationMatrix.put(0, 2, cx: _*)
    rotationMatrix.put(1, 2, cy: _*)

    rotationMatrix
  }

  /**
   * Rotate Bounding-Box by using rotation matrix
   *
   * @param bBoxXY (N x 4) array representing (x, y) coordinates of 2 opposite corners of BBox
   * @param rotationMatrix (3 x 2) affine transformation matrix
   * @return rotated BBox in same format as bBoxXY
   */
  def rotateBBoxXY(bBoxXY: Array[Array[Int]], rotationMatrix: Mat): Array[Array[Int]] = {
    if (bBoxXY.length == 0) {
      return bBoxXY
    }

    if (bBoxXY(0).length != 4) {
      throw new IllegalArgumentException(
        "xy must has (N x 4) size, N representing number of bounding boxes and 4 for (x,y) " +
        "coordinates of two opposite corners of bounding box")
    }

    // Convert XYXY into homogeneous coordinates
    val numBoxes = bBoxXY.length
    val src_xy = Mat.ones(numBoxes * 4, 3, CvType.CV_32FC1) // columns are (x, y, 1)
    for(i <- 0 until numBoxes) {
      assert(bBoxXY(i)(0) <= bBoxXY(i)(2),
        "Top-Left coordinates must be smaller than Bottom-Right coordinates")
      assert(bBoxXY(i)(1) <= bBoxXY(i)(3),
        "Top-Left coordinates must be smaller than Bottom-Right coordinates")

      src_xy.put(i, 0, Array(bBoxXY(i)(0)).map(_.asInstanceOf[Float])) // TL-x
      src_xy.put(i, 1, Array(bBoxXY(i)(1)).map(_.asInstanceOf[Float])) // TL-y
      src_xy.put(i + numBoxes, 0, Array(bBoxXY(i)(2)).map(_.asInstanceOf[Float])) // BR-x
      src_xy.put(i + numBoxes, 1, Array(bBoxXY(i)(3)).map(_.asInstanceOf[Float])) // BR-y
      src_xy.put(i + numBoxes * 2, 0, Array(bBoxXY(i)(2)).map(_.asInstanceOf[Float])) // TR-x
      src_xy.put(i + numBoxes * 2, 1, Array(bBoxXY(i)(1)).map(_.asInstanceOf[Float])) // TR-y
      src_xy.put(i + numBoxes * 3, 0, Array(bBoxXY(i)(0)).map(_.asInstanceOf[Float])) // BL-x
      src_xy.put(i + numBoxes * 3, 1, Array(bBoxXY(i)(3)).map(_.asInstanceOf[Float])) // BL-y
    }
    src_xy.convertTo(src_xy, CvType.CV_64FC1)

    // Perform rotation
    val dst_xy = new Mat(numBoxes, 2, CvType.CV_64FC1)
    Core.gemm(src_xy, rotationMatrix.t(), 1, new Mat(), 0, dst_xy)

    // Convert back to XYXY
    val rotatedBBoxXY = (0 until numBoxes).map{
      i =>
        val x1 = dst_xy.get(i, 0)(0) // Rotated TL-x
        val y1 = dst_xy.get(i, 1)(0) // Rotated TL-y
        val x2 = dst_xy.get(i + numBoxes, 0)(0) // Rotated BR-x
        val y2 = dst_xy.get(i + numBoxes, 1)(0) // Rotated BR-y
        val x3 = dst_xy.get(i + numBoxes * 2, 0)(0) // Rotated TR-x
        val y3 = dst_xy.get(i + numBoxes * 2, 1)(0) // Rotated TR-y
        val x4 = dst_xy.get(i + numBoxes * 3, 0)(0) // Rotated BL-x
        val y4 = dst_xy.get(i + numBoxes * 3, 1)(0) // Rotated BL-y
        Array(
          Array(x1, x2, x3, x4).min, // smaller of x1 ... x4
          Array(y1, y2, y3, y4).min, // smaller of y1 .. y4
          Array(x1, x2, x3, x4).max, // larger of x1 ... x4
          Array(y1, y2, y3, y4).max  // larger of y1 ... y4
        ).map(_.asInstanceOf[Int])
    }.toArray
    rotatedBBoxXY
  }

  /**
   * Rotate Bounding-Box by using rotation matrix
   *
   * @param bBoxWH (N x 4) array representing (x, y, w, h)
   *               (x, y) as top-left corner and (w, h) as width & height of BBox
   * @param rotationMatrix (3 x 2) affine transformation matrix
   * @return rotated BBox in same format as bBoxWh
   */
  def rotateBBoxWH(bBoxWH: Array[Rect], rotationMatrix: Mat): Array[Rect] = {
    // Convert XY-WH bounding boxes to XY-XY bounding boxes
    val bBoxXY = bBoxWH.map {
      rect =>
        Array(rect.x, rect.y, rect.x + rect.width, rect.y + rect.height)
    }

    // Do transformation
    val rotatedBBoxXY = rotateBBoxXY(bBoxXY, rotationMatrix)

    // Convert back from XY-XY bounding boxes to XY-WH bounding box
    val rotatedBBoxWH = rotatedBBoxXY.map {
      rect =>
        new Rect(rect(0), rect(1), rect(2) - rect(0), rect(3) - rect(1))
    }
    rotatedBBoxWH
  }


  def modeInRange(elements: Seq[Int], rangeMin: Int, rangeMax: Int): Int = {
    val elementsInRange = elements.collect {
      case element if element > rangeMin && element < rangeMax =>
        element
    }
    val grouped = elementsInRange.groupBy(i => i).map(kv => (kv._1, kv._2.size))
    grouped.maxBy(_._2)._1
  }


  def computeRotationAngle(img: Mat): Int = {
    // Convert BGR image to Gray image
    val imgGray = new Mat(img.size(), CvType.CV_8UC1)
    Imgproc.cvtColor(img, imgGray, Imgproc.COLOR_BGR2GRAY)

    // Convert Gray image to Binary Image
    val imgBinary = new Mat()
    Imgproc.adaptiveThreshold(imgGray, imgBinary, 255, Imgproc.ADAPTIVE_THRESH_GAUSSIAN_C,
      Imgproc.THRESH_BINARY, 1001, 0)
    val contours = new java.util.ArrayList[MatOfPoint]()

    // Find all contours
    Imgproc.findContours(imgBinary, contours, new Mat(), Imgproc.RETR_TREE,
      Imgproc.CHAIN_APPROX_SIMPLE)

    // Filter out non-important contours, compute closest fitting rectangle and find rotation angle
    val minArea = img.size().width * img.size().height / 1000
    val angles = contours.asScala.collect {
      case contour: MatOfPoint if Imgproc.contourArea(contour) > minArea =>
        val points = new MatOfPoint2f()
        contour.convertTo(points, CvType.CV_32F)
        Imgproc.minAreaRect(points).angle.asInstanceOf[Int]
    }.toSeq

    // Find the angle with the highest number of occurrence
    val modeAngle = modeInRange(angles, -30, 30)
    modeAngle
  }

}
