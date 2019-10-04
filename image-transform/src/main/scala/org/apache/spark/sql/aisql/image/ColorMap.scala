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

object ColorMap {

  // Colors are in RGB format
  private val colorMap_RGB: Map[String, (Int, Int, Int)] = Map(
    "white" -> (255, 255, 255),
    "black" -> (0, 0, 0),
    "gray" -> (127, 127, 127),
    "red" -> (255, 0, 0),
    "green" -> (0, 255, 0),
    "blue" -> (0, 0, 255),
    "yellow" -> (255, 255, 0),
    "magenta" -> (255, 0, 255),
    "cyan" -> (0, 255, 255)
  )


  private def getRGBColor(colorName: String): (Int, Int, Int) = {
    colorMap_RGB.getOrElse(colorName,
      throw new IllegalArgumentException(
        s"color $colorName is not a valid color. Valid colors are ${validColors()}"))
  }


  def getBGRColor(colorName: String): (Int, Int, Int) = {
    // Get RGB color from colorMap
    val colorRGB = getRGBColor(colorName)

    // Convert RGB to BGR
    val colorBGR = (colorRGB._3, colorRGB._2, colorRGB._1)
    colorBGR
  }


  def validColors(): Array[String] = {
    colorMap_RGB.keySet.toArray
  }
}
