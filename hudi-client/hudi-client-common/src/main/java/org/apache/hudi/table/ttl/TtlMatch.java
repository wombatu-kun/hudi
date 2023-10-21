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

package org.apache.hudi.table.ttl;

import org.apache.hudi.common.table.ttl.model.TtlPolicyLevel;

import java.time.temporal.ChronoUnit;

/**
 * Class for representation of TTL-processing results
 */
public class TtlMatch {

  // partition that was recognized as expired
  private final String source;

  // when this partition was updated last time
  private final String lastUpdate;

  // TTL policy and its settings by which that partition was recognized as expired
  private final String spec;
  // TTL policy level
  private final TtlPolicyLevel level;
  private final int value;
  private final ChronoUnit units;

  public TtlMatch(String source, String lastUpdate, String spec, TtlPolicyLevel level, int value, ChronoUnit units) {
    this.source = source;
    this.lastUpdate = lastUpdate;
    this.spec = spec;
    this.level = level;
    this.value = value;
    this.units = units;
  }

  public String getSource() {
    return source;
  }

  public String getLastUpdate() {
    return lastUpdate;
  }

  public String getSpec() {
    return spec;
  }

  public TtlPolicyLevel getLevel() {
    return level;
  }

  public int getValue() {
    return value;
  }

  public ChronoUnit getUnits() {
    return units;
  }

  @Override
  public String toString() {
    return "{\"source=\"" + source
        + "\", \"lastUpdate\"=\"" + lastUpdate
        + "\", \"spec\"=\"" + spec
        + "\", \"level\"=" + level
        + "\", \"value\"=\"" + value
        + ", \"units\"=\"" + units + "\"}";
  }
}
