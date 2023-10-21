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

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Class with common util methods used in TTL processing
 */
public class TtlPolicyUtils {

  private TtlPolicyUtils() {
  }

  public static boolean timeIsUp(Date event, int termInDays, Date now) {
    long diffInMillis = Math.abs(now.getTime() - event.getTime());
    long diff = TimeUnit.DAYS.convert(diffInMillis, TimeUnit.MILLISECONDS);
    return diff >= termInDays;
  }

  public static Map<String, Pattern> compilePartitionPatterns(List<TtlPolicy> policies) {
    return policies.stream().collect(Collectors.toMap(TtlPolicy::getSpec, p -> Pattern.compile(specToRegex(p.getSpec()))));
  }

  public static boolean doesPartitionMatchPattern(String partition, Pattern pattern) {
    return pattern.matcher(partition).matches();
  }

  public static String specToRegex(String spec) {
    StringBuilder s = new StringBuilder("^");
    for (char c: spec.toCharArray()) {
      switch (c) {
        case '*':
          s.append(".*");
          break;
        case '?':
          s.append(".");
          break;
        // escape special regexp-characters
        case '(': case ')': case '[': case ']': case '$':
        case '^': case '.': case '{': case '}': case '|':
        case '+': case '\\':
          s.append("\\");
          s.append(c);
          break;
        default:
          s.append(c);
      }
    }
    return (s.append('$').toString());
  }
}
