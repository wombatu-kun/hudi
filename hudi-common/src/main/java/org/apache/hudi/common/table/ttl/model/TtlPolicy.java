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

package org.apache.hudi.common.table.ttl.model;

import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.exception.InvalidTtlPolicyException;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TtlPolicy {
  private static final List<ChronoUnit> SUPPORTED_UNITS = Arrays.asList(ChronoUnit.YEARS, ChronoUnit.MONTHS, ChronoUnit.WEEKS, ChronoUnit.DAYS);

  // Specification: defines what data should be affected by this policy.
  // It is a string with concrete partition or some template with wildcards (*, ?).
  @JsonProperty("spec")
  @NotEmpty
  private String spec;

  // Partition or Record level.
  @JsonProperty("level")
  @NotNull
  private TtlPolicyLevel level;

  // Defines the number of units for TTL of data defined by spec.
  @JsonProperty("value")
  @NotNull
  private Integer value;

  // Defines chrono unit for TTL of data defined by spec.
  // Only YEARS, MONTHS, WEEKS and DAYS are supported.
  @JsonProperty("units")
  @NotNull
  private ChronoUnit units;

  // Timestamp when this policy was saved last time (just for information).
  @JsonProperty("useSince")
  private Date useSince;

  public TtlPolicy() {
  }

  public TtlPolicy(String spec, TtlPolicyLevel level, int value, ChronoUnit units) {
    this.spec = spec;
    this.level = level;
    this.value = value;
    this.units = units;
  }

  public List<String> validate() {
    List<String> constraintViolations = new ArrayList<>();
    if (StringUtils.isNullOrEmpty(spec)) {
      constraintViolations.add("'spec' must not be empty;");
    } else {
      if (level == TtlPolicyLevel.PARTITION) {
        // just path with wildcards
        if (spec.replaceAll("/", "").isEmpty()) {
          constraintViolations.add("'spec' must not consist of /;");
        } else {
          // cut starting and ending slashes silently
          if (spec.startsWith("/")) {
            spec = spec.substring(1);
          }
          if (spec.endsWith("/")) {
            spec = spec.substring(0, spec.length() - 1);
          }
        }
      }
    }
    if (level == null) {
      constraintViolations.add("'level' must not be null;");
    }
    if (value == null || value <= 0) {
      constraintViolations.add("'value' must be integer greater than 0;");
    }
    if (units == null) {
      constraintViolations.add("'units' must not be null;");
      if (!SUPPORTED_UNITS.contains(units)) {
        constraintViolations.add("'units' must be one of types: YEARS, MONTHS, WEEKS, DAYS;");
      }
    }
    return constraintViolations;
  }

  // All supported Chrono units always converts to DAYS.
  public int convertTtlToDays() {
    switch (units) {
      case YEARS: return 365 * value;
      case MONTHS: return 30 * value;
      case WEEKS: return 7 * value;
      case DAYS: return value;
      default:
        throw new InvalidTtlPolicyException("Chrono units <" + units + "> not supported");
    }
  }

  public String getSpec() {
    return spec;
  }

  public void setSpec(String spec) {
    this.spec = spec;
  }

  public TtlPolicyLevel getLevel() {
    return level;
  }

  public void setLevel(TtlPolicyLevel level) {
    this.level = level;
  }

  public int getValue() {
    return value;
  }

  public void setValue(int value) {
    this.value = value;
  }

  public ChronoUnit getUnits() {
    return units;
  }

  public void setUnits(ChronoUnit units) {
    this.units = units;
  }

  public Date getUseSince() {
    return useSince;
  }

  public void setUseSince(Date useSince) {
    this.useSince = useSince;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TtlPolicy ttlPolicy = (TtlPolicy) o;

    if (!spec.equals(ttlPolicy.spec)) {
      return false;
    }
    if (level != ttlPolicy.level) {
      return false;
    }
    if (!value.equals(ttlPolicy.value)) {
      return false;
    }
    if (units != ttlPolicy.units) {
      return false;
    }
    return Objects.equals(useSince, ttlPolicy.useSince);
  }

  @Override
  public int hashCode() {
    int result = spec.hashCode();
    result = 31 * result + level.hashCode();
    result = 31 * result + value.hashCode();
    result = 31 * result + units.hashCode();
    result = 31 * result + (useSince != null ? useSince.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "{\"spec=\"" + spec + "\", "
        + "\"level\"=\"" + level + "\", "
        + "\"value\"=\"" + value + "\", "
        + "\"units\"=\"" + units + "\", "
        + "\"useSince\"=\"" + useSince + "\"}";
  }
}
