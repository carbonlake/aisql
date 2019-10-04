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

import java.io.IOException
import java.util

import org.apache.carbondata.common.annotations.InterfaceAudience
import org.apache.carbondata.common.exceptions.sql.NoSuchDataMapException
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, DataMapSchema, DiskBasedDMSchemaStorageProvider}
import org.apache.carbondata.core.util.CarbonProperties


/**
 * It maintains all the Model's in it.
 */
@InterfaceAudience.Internal
object ExperimentStoreManager {
  private val instance = new ExperimentStoreManager

  val QUERY_OBJECT = "query_object"

  /**
   * Returns the singleton instance
   *
   * @return
   */
  def getInstance: ExperimentStoreManager = instance
}

@InterfaceAudience.Internal
final class ExperimentStoreManager {
  private val provider = new DiskBasedDMSchemaStorageProvider(
    CarbonProperties.getInstance.getSystemFolderLocation + "/model")

  /**
   * It gives all experiment schemas of a given table.
   *
   */
  @throws[IOException]
  def getDataMapSchemasOfTable(carbonTable: CarbonTable): util.List[DataMapSchema] = provider.retrieveSchemas(carbonTable)

  /**
   * It gives all experiment schemas from store.
   */
  @throws[IOException]
  def getAllExperimentSchemas: util.List[DataMapSchema] = {
    val schemas = provider.retrieveAllSchemas
    schemas
  }

  @throws[NoSuchDataMapException]
  @throws[IOException]
  def getExperimentSchema(experimentName: String): DataMapSchema = provider.retrieveSchema(experimentName)

  /**
   * Saves the experiment schema to storage
   *
   * @param experimentSchema
   */
  @throws[IOException]
  def saveExperimentSchema(experimentSchema: DataMapSchema): Unit = {
    provider.saveSchema(experimentSchema)
  }

  /**
   * Drops the experiment schema from storage
   *
   * @param experimentName
   */
  @throws[IOException]
  def dropExperimentSchema(experimentName: String): Unit = {
    provider.dropSchema(experimentName)
  }
}
