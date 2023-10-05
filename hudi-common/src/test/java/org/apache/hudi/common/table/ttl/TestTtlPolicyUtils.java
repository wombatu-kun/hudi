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

import org.junit.jupiter.api.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link TtlPolicyUtils}.
 */
public class TestTtlPolicyUtils {

  @Test
  public void testSpecToRegex() {
    // no special symbols
    assertEquals("^abc/2023/par=val$", TtlPolicyUtils.specToRegex("abc/2023/par=val"));
    // supported wild cards *?
    assertEquals("^.*/20../par=.*$", TtlPolicyUtils.specToRegex("*/20??/par=*"));
    // escaping special regexp-characters ()[]^.{}|+\ at start, end, middle, and both sides of specs
    assertEquals("^1\\(/\\)2/3\\[3/\\]4\\]$", TtlPolicyUtils.specToRegex("1(/)2/3[3/]4]"));
    assertEquals("^ \\^/\\. / \\{ /\\} \\}$", TtlPolicyUtils.specToRegex(" ^/. / { /} }"));
    assertEquals("^a\\|\\|/\\+\\+b/c\\\\c$", TtlPolicyUtils.specToRegex("a||/++b/c\\c"));
    // unicode characters
    assertEquals("^龥且/Õö±•$", TtlPolicyUtils.specToRegex("龥且/Õö±•"));
    // empty string
    assertEquals("^$", TtlPolicyUtils.specToRegex(""));
  }

  @Test
  public void testPartitionMatchPattern() {
    // slashes at the start and the end of partition paths are removed
    // hive style, first level
    assertTrue(TtlPolicyUtils.doesPartitionMatchPattern("year=2023/model=LS+/ver=1.2", Pattern.compile(TtlPolicyUtils.specToRegex("*"))));
    assertTrue(TtlPolicyUtils.doesPartitionMatchPattern("year=2023/model=LS+/ver=1.2", Pattern.compile(TtlPolicyUtils.specToRegex("year=*"))));
    assertFalse(TtlPolicyUtils.doesPartitionMatchPattern("2023/LS+/1.2", Pattern.compile(TtlPolicyUtils.specToRegex("year=*"))));

    assertTrue(TtlPolicyUtils.doesPartitionMatchPattern("year=2023/model=LS+/ver=1.2", Pattern.compile(TtlPolicyUtils.specToRegex("year=202*"))));
    assertTrue(TtlPolicyUtils.doesPartitionMatchPattern("year=2023/model=LS+/ver=1.2", Pattern.compile(TtlPolicyUtils.specToRegex("year=202?*"))));
    assertTrue(TtlPolicyUtils.doesPartitionMatchPattern("year=2023/model=LS+/ver=1.2", Pattern.compile(TtlPolicyUtils.specToRegex("year=202?/*"))));
    assertFalse(TtlPolicyUtils.doesPartitionMatchPattern("year=2023/model=LS+/ver=1.2", Pattern.compile(TtlPolicyUtils.specToRegex("year=202?"))));

    assertTrue(TtlPolicyUtils.doesPartitionMatchPattern("year=2023/model=LS+/ver=1.2", Pattern.compile(TtlPolicyUtils.specToRegex("year=202?/model=LS+/ver=1.2"))));
    assertFalse(TtlPolicyUtils.doesPartitionMatchPattern("year=2013/model=LS+/ver=1.2", Pattern.compile(TtlPolicyUtils.specToRegex("year=202?/model=LS+/ver=1.2"))));

    // hive style, second level
    assertTrue(TtlPolicyUtils.doesPartitionMatchPattern("year=2023/model=LS+/ver=1.2", Pattern.compile(TtlPolicyUtils.specToRegex("*/*"))));
    assertTrue(TtlPolicyUtils.doesPartitionMatchPattern("year=2023/model=LS+/ver=1.2", Pattern.compile(TtlPolicyUtils.specToRegex("*/model=*"))));
    assertTrue(TtlPolicyUtils.doesPartitionMatchPattern("year=2023/model=LS+/ver=1.2", Pattern.compile(TtlPolicyUtils.specToRegex("*/model=*+*"))));
    assertTrue(TtlPolicyUtils.doesPartitionMatchPattern("year=2023/model=LS+/ver=1.2", Pattern.compile(TtlPolicyUtils.specToRegex("*/model=*+/*"))));
    assertFalse(TtlPolicyUtils.doesPartitionMatchPattern("year=2023/model=LS+/ver=1.2", Pattern.compile(TtlPolicyUtils.specToRegex("*/model=*+"))));
    assertFalse(TtlPolicyUtils.doesPartitionMatchPattern("year=2023/model=LS/ver=1.2", Pattern.compile(TtlPolicyUtils.specToRegex("*/model=*+"))));

    assertTrue(TtlPolicyUtils.doesPartitionMatchPattern("year=2023/model=LS+/ver=1.2", Pattern.compile(TtlPolicyUtils.specToRegex("year=2023/model=LS?*"))));
    assertTrue(TtlPolicyUtils.doesPartitionMatchPattern("year=2023/model=LS+/ver=1.2", Pattern.compile(TtlPolicyUtils.specToRegex("year=2023/model=LS?/*"))));
    assertTrue(TtlPolicyUtils.doesPartitionMatchPattern("year=2023/model=LS+/ver=1.2", Pattern.compile(TtlPolicyUtils.specToRegex("year=2023/model=???/*"))));
    assertFalse(TtlPolicyUtils.doesPartitionMatchPattern("year=2023/model=LS+/ver=1.2", Pattern.compile(TtlPolicyUtils.specToRegex("year=2023/model=???"))));
    assertFalse(TtlPolicyUtils.doesPartitionMatchPattern("year=2023/model=LS+/ver=1.2", Pattern.compile(TtlPolicyUtils.specToRegex("year=2023/model=ls?/*"))));

    // third level
    assertTrue(TtlPolicyUtils.doesPartitionMatchPattern("2023/LS+/1.2", Pattern.compile(TtlPolicyUtils.specToRegex("*/*/*"))));
    assertTrue(TtlPolicyUtils.doesPartitionMatchPattern("2023/LS+/1.2", Pattern.compile(TtlPolicyUtils.specToRegex("2023/LS+/*"))));
    assertFalse(TtlPolicyUtils.doesPartitionMatchPattern("2023/LS+/1.2", Pattern.compile(TtlPolicyUtils.specToRegex("2023/LS/*"))));
    assertFalse(TtlPolicyUtils.doesPartitionMatchPattern("2022/LS+/1.2", Pattern.compile(TtlPolicyUtils.specToRegex("2023/LS+/*"))));

    assertTrue(TtlPolicyUtils.doesPartitionMatchPattern("2023/LS+/1.2", Pattern.compile(TtlPolicyUtils.specToRegex("*/*/1.*"))));
    assertTrue(TtlPolicyUtils.doesPartitionMatchPattern("2023/LS+/1.2", Pattern.compile(TtlPolicyUtils.specToRegex("*/*/1.?"))));
    assertTrue(TtlPolicyUtils.doesPartitionMatchPattern("2023/LS+/1.2", Pattern.compile(TtlPolicyUtils.specToRegex("*/*/1?2"))));
    assertFalse(TtlPolicyUtils.doesPartitionMatchPattern("2023/LS+/1,2", Pattern.compile(TtlPolicyUtils.specToRegex("*/*/1.*"))));
    assertFalse(TtlPolicyUtils.doesPartitionMatchPattern("2023/LS+/1.2.3", Pattern.compile(TtlPolicyUtils.specToRegex("*/*/1.?"))));
    assertFalse(TtlPolicyUtils.doesPartitionMatchPattern("2023/LS+/12", Pattern.compile(TtlPolicyUtils.specToRegex("*/*/1?2"))));

    // unicode characters
    assertTrue(TtlPolicyUtils.doesPartitionMatchPattern("2023/亷亸亹/1.2", Pattern.compile(TtlPolicyUtils.specToRegex("2023/亷*/1.2"))));
    assertTrue(TtlPolicyUtils.doesPartitionMatchPattern("2023/亷亸亹/1.2", Pattern.compile(TtlPolicyUtils.specToRegex("2023/亷?亹/1.2"))));
    assertTrue(TtlPolicyUtils.doesPartitionMatchPattern("2023/亷亸亹/1.2", Pattern.compile(TtlPolicyUtils.specToRegex("2023/???/1.2"))));
    assertFalse(TtlPolicyUtils.doesPartitionMatchPattern("2023/亷人亹/1.2", Pattern.compile(TtlPolicyUtils.specToRegex("2023/亷亸亹/1.2"))));
  }

  @Test
  public void testTimeIsUp() throws ParseException {
    SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy hh:mm:ss");

    // termInDays could be only > 0
    assertTrue(TtlPolicyUtils.timeIsUp(dateFormat.parse("28-09-2023 00:00:00"), 1, dateFormat.parse("29-09-2023 00:00:00")));
    assertFalse(TtlPolicyUtils.timeIsUp(dateFormat.parse("28-09-2023 00:00:00"), 1, dateFormat.parse("28-09-2023 23:59:59")));

    // month with 30 days
    assertTrue(TtlPolicyUtils.timeIsUp(dateFormat.parse("28-09-2023 00:00:00"), 30, dateFormat.parse("28-10-2023 00:00:00")));
    assertFalse(TtlPolicyUtils.timeIsUp(dateFormat.parse("28-09-2023 00:00:00"), 30, dateFormat.parse("27-10-2023 00:00:00")));
    // month with 31 days
    assertTrue(TtlPolicyUtils.timeIsUp(dateFormat.parse("28-10-2023 00:00:00"), 30, dateFormat.parse("27-11-2023 00:00:00")));

    // no Feb from a leap year in the interval
    assertTrue(TtlPolicyUtils.timeIsUp(dateFormat.parse("28-09-2022 00:00:00"), 365, dateFormat.parse("28-09-2023 00:00:00")));
    assertFalse(TtlPolicyUtils.timeIsUp(dateFormat.parse("28-09-2022 00:00:00"), 365, dateFormat.parse("27-09-2023 00:00:00")));
    // Feb from a leap year in the interval
    assertTrue(TtlPolicyUtils.timeIsUp(dateFormat.parse("28-09-2023 00:00:00"), 365, dateFormat.parse("27-09-2024 00:00:00")));

    // value bounds
    Date maxDate = new Date(Long.MAX_VALUE);
    Date minDate = new Date(Long.MIN_VALUE);
    assertFalse(TtlPolicyUtils.timeIsUp(dateFormat.parse("28-09-2023 00:00:00"), Integer.MAX_VALUE, dateFormat.parse("28-09-3023 00:00:00")));
    assertTrue(TtlPolicyUtils.timeIsUp(dateFormat.parse("28-09-2023 00:00:00"), 1, maxDate));
    assertTrue(TtlPolicyUtils.timeIsUp(minDate, 1, dateFormat.parse("28-09-3023 00:00:00")));
    assertFalse(TtlPolicyUtils.timeIsUp(maxDate, 1, maxDate));
    assertFalse(TtlPolicyUtils.timeIsUp(minDate, 1, minDate));
  }
}
