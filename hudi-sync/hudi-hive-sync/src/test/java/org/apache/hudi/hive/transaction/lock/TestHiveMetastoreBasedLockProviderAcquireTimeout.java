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

package org.apache.hudi.hive.transaction.lock;

import org.apache.hudi.exception.HoodieLockException;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for what {@link HiveMetastoreBasedLockProvider} reports when the metastore does not
 * answer a lock request in time, with a mocked {@link IMetaStoreClient} and no live metastore or
 * ZooKeeper.
 */
class TestHiveMetastoreBasedLockProviderAcquireTimeout extends HiveMetastoreBasedLockProviderTestBase {

  private static final long LOCK_ID = 42L;
  private static final long ACQUIRE_TIMEOUT_MS = 200L;

  @Test
  void acquireTimeoutIsReportedAsATimeout() throws Exception {
    IMetaStoreClient client = mock(IMetaStoreClient.class);
    CountDownLatch metastoreAnswers = new CountDownLatch(1);
    when(client.lock(any())).thenAnswer(invocation -> {
      metastoreAnswers.await();
      return acquiredLock(LOCK_ID);
    });

    HiveMetastoreBasedLockProvider provider = new HiveMetastoreBasedLockProvider(lockConfiguration, client);
    try {
      HoodieLockException thrown = assertThrows(HoodieLockException.class,
          () -> provider.tryLock(ACQUIRE_TIMEOUT_MS, TimeUnit.MILLISECONDS));

      // The metastore never answered, and that is what the writer has to be told. Looking the lock
      // up afterwards cannot help: the request that timed out never returned a lock id.
      assertInstanceOf(TimeoutException.class, thrown.getCause());
      verify(client, never()).checkLock(anyLong());
      assertNull(provider.getLock());
    } finally {
      metastoreAnswers.countDown();
      provider.close();
    }
  }
}
