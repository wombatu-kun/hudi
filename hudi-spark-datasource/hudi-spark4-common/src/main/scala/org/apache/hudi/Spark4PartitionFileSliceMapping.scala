/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi

import org.apache.hudi.common.model.FileSlice

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.unsafe.types.VariantVal

/**
 * Spark 4.x concrete [[PartitionFileSliceMapping]]. Spark 4 added an abstract `getVariant` to
 * [[org.apache.spark.sql.catalyst.expressions.SpecializedGetters]] ([[VariantVal]] does not exist in
 * Spark 3.5), so it is implemented here, delegating to the wrapped partition-values row.
 */
class Spark4PartitionFileSliceMapping(internalRow: InternalRow,
                                      slices: Map[String, FileSlice])
  extends PartitionFileSliceMapping(internalRow, slices) {

  override def getVariant(ordinal: Int): VariantVal = getInternalRow.getVariant(ordinal)
}
