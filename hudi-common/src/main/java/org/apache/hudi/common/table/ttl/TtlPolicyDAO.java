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

package org.apache.hudi.common.table.ttl;

import org.apache.hudi.common.table.ttl.model.TtlPolicy;

import java.io.IOException;
import java.util.List;

/**
 * DAO for working with TTL policies
 */
public interface TtlPolicyDAO {

  // Returns nam of the file where TTL policies are stored
  String getPoliciesFileName();

  // Returns all policies in natural order
  List<TtlPolicy> getAll() throws IOException;

  // Upserts given policy represented by json-string
  void save(String jsonStringPolicy) throws IOException;

  // Upserts given TTL policy
  void save(TtlPolicy policy) throws IOException;

  // Deletes policy by its specification as a key
  void deleteBySpec(String spec) throws IOException;

  // Deletes all policies from hudi table
  void deleteAll();

}
