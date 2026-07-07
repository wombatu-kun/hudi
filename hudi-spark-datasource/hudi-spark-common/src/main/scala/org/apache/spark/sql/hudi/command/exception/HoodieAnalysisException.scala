/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hudi.command.exception

import org.apache.spark.sql.AnalysisException

/**
 * Hudi-owned [[AnalysisException]] subclass exposing a stable plain-message constructor across Spark
 * versions. Spark 4 removed the public `AnalysisException(message: String)` constructor in favor of the
 * error-class framework; since this class lives in a sub-package of `org.apache.spark.sql` it can still
 * reach the `protected[sql]` primary constructor (which defaults the errorClass/messageParameters).
 */
class HoodieAnalysisException (override val message: String,
                               override val line: Option[Int] = None,
                               override val startPosition: Option[Int] = None) extends AnalysisException(message) {

}
