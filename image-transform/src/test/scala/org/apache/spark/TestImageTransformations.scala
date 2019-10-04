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

package org.apache.spark

import org.apache.spark.sql.test.util.QueryTest
import org.opencv.core.{Core, MatOfByte, Size}
import org.opencv.imgcodecs.Imgcodecs
import org.scalatest.BeforeAndAfterAll

class TestImageTransformations extends QueryTest with BeforeAndAfterAll {

  val dirPath: String = resourcesPath +
    "../../../../../../../../../resources/image"
  val imageSourceTable: String = "imageSourceTable"
  val imageTransTable: String = "imageTransTable"

  override protected def beforeAll(): Unit = {
    nu.pattern.OpenCV.loadShared()
    System.loadLibrary(Core.NATIVE_LIBRARY_NAME)

    VisionSparkUDFs.registerAll(sqlContext.sparkSession)

    sql(s"DROP TABLE IF EXISTS $imageSourceTable")
    sql(s"DROP TABLE IF EXISTS $imageTransTable")

    sql(s"CREATE TEMPORARY TABLE $imageSourceTable USING binaryfile OPTIONS(path='$dirPath')")

    sql(
      s"""
         | CREATE TABLE $imageTransTable (
         |   path STRING,
         |   modificationTime TIMESTAMP,
         |   length LONG,
         |   content BINARY
         | )
         | stored as carbondata
         """.stripMargin)
  }

  test("Image Resize UDF") {
    sql(
      s"""
        | INSERT INTO $imageTransTable
        | SELECT path, modificationTime, length, resize(content, 256, 256)
        | from $imageSourceTable
        | """.stripMargin)
    sql(s"select path, length from $imageTransTable").show(false)
    sql(s"SELECT content from $imageTransTable").collect().foreach {
      row =>
        val content = row.get(0).asInstanceOf[Array[Byte]]
        val img = Imgcodecs.imdecode(new MatOfByte(content: _*), Imgcodecs.CV_LOAD_IMAGE_UNCHANGED)
        assertResult(new Size(256, 256), "image is not resized to (256 x 256)")(img.size())
    }
  }


  test("Image Crop UDF") {
    sql(
      s"""INSERT COLUMNS(
         |   newcontent
         | ) INTO TABLE $imageTransTable
         | SELECT crop(content, 0, 0, 100, 100)
         | FROM $imageTransTable""".stripMargin
    )

    sql(s"SELECT newcontent from $imageTransTable").collect().foreach {
      row =>
        val content = row.get(0).asInstanceOf[Array[Byte]]
        val img = Imgcodecs.imdecode(new MatOfByte(content: _*), Imgcodecs.CV_LOAD_IMAGE_UNCHANGED)
        assert(img.size().equals(new Size(100, 100)),
          "image is not cropped to (100 x 100)")
    }
  }


  test("Image Rotate UDF") {
    sql(s"SELECT rotate(newcontent, 45) from $imageTransTable").collect().foreach {
      row =>
        val content = row.get(0).asInstanceOf[Array[Byte]]
        val img = Imgcodecs.imdecode(new MatOfByte(content: _*), Imgcodecs.CV_LOAD_IMAGE_UNCHANGED)
        assert(img.size().equals(new Size(143, 143)))

        val cornerPixel = img.get(0, 0)
        assert(cornerPixel(0) == 255.0 && cornerPixel(1) == 255.0 && cornerPixel(2) == 255.0,
          "corner pixel of rotated image is not white")
    }

    sql(s"SELECT rotateWithBgColor(newcontent, 45, 'blue') from $imageTransTable").collect().foreach {
      row =>
        val content = row.get(0).asInstanceOf[Array[Byte]]
        val img = Imgcodecs.imdecode(new MatOfByte(content: _*), Imgcodecs.CV_LOAD_IMAGE_UNCHANGED)
        assert(img.size().equals(new Size(143, 143)))

        val firstPixel = img.get(0, 0)
        assert(firstPixel(0) >= 254 && firstPixel(1) < 1 && firstPixel(2) < 1,
          "corner pixel of rotated image is not blue")
    }
  }


  test("Add Random Noise UDF") {
    sql(s"SELECT addRandomNoise(newcontent) from $imageTransTable").collect().foreach {
      row =>
        val content = row.get(0).asInstanceOf[Array[Byte]]
        val img = Imgcodecs.imdecode(new MatOfByte(content: _*), Imgcodecs.CV_LOAD_IMAGE_UNCHANGED)

        // It is difficult to check if the image is noisy, so we will just check for size
        assert(img.size().equals(new Size(100, 100)),
          "image is not cropped to (100 x 100)")
    }
  }


  override protected def afterAll(): Unit = {
    sql(s"DROP TABLE IF EXISTS $imageTransTable")
    sql(s"DROP TABLE IF EXISTS $imageSourceTable")
  }
}
