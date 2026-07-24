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

import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.exception.HoodieLockException;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockLevel;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.NoSuchLockException;
import org.apache.thrift.TException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.apache.hudi.common.config.LockConfiguration.HIVE_DATABASE_NAME_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.HIVE_TABLE_NAME_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_HEARTBEAT_INTERVAL_MS_KEY;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for how {@link HiveMetastoreBasedLockProvider} reacts to the metastore reporting its
 * lock as gone, with a mocked {@link IMetaStoreClient} and no live metastore or ZooKeeper.
 */
class TestHiveMetastoreBasedLockProviderLockLoss {

  private static final String DB = "testdb";
  private static final String TABLE = "testtable";
  private static final long LOCK_ID = 42L;
  private static final long OTHER_LOCK_ID = 43L;
  private static final long HEARTBEAT_INTERVAL_MS = 100L;
  private static final long AWAIT_TIMEOUT_MS = 30_000L;

  private LockConfiguration lockConfiguration;
  private LockComponent lockComponent;

  @BeforeEach
  void setUp() {
    TypedProperties props = new TypedProperties();
    props.setProperty(HIVE_DATABASE_NAME_PROP_KEY, DB);
    props.setProperty(HIVE_TABLE_NAME_PROP_KEY, TABLE);
    // Keep the ticks short so the scheduled heartbeat fires within the test.
    props.setProperty(LOCK_HEARTBEAT_INTERVAL_MS_KEY, String.valueOf(HEARTBEAT_INTERVAL_MS));
    lockConfiguration = new LockConfiguration(props);
    lockComponent = new LockComponent(LockType.EXCLUSIVE, LockLevel.TABLE, DB);
    lockComponent.setTablename(TABLE);
  }

  @Test
  void terminalHeartbeatFailureStopsRenewalAndDropsTheLock() throws Exception {
    IMetaStoreClient client = mock(IMetaStoreClient.class);
    when(client.lock(any())).thenReturn(acquiredLock(LOCK_ID));
    doThrow(new NoSuchLockException("lock " + LOCK_ID + " does not exist"))
        .when(client).heartbeat(anyLong(), anyLong());

    HiveMetastoreBasedLockProvider provider = new HiveMetastoreBasedLockProvider(lockConfiguration, client);
    try {
      assertTrue(provider.acquireLock(1000L, TimeUnit.MILLISECONDS, lockComponent));

      // The metastore has expired the lock, so the provider must stop claiming to hold it.
      awaitUntil(() -> provider.getLock() == null, "the lost lock must be dropped by the heartbeat");

      // The heartbeat task latches the failure on its own, so the count below would stay at one
      // even if the schedule were left running. Assert the cancellation itself as well.
      assertTrue(heartbeatFutureOf(provider).isCancelled(), "the heartbeat schedule must be cancelled");

      // Give the scheduler several more intervals: no further heartbeat may be attempted, since
      // both the schedule and the heartbeat task itself are stopped after a terminal failure.
      Thread.sleep(HEARTBEAT_INTERVAL_MS * 5);
      verify(client, times(1)).heartbeat(0L, LOCK_ID);

      // The writer must learn that it no longer holds exclusivity, and the provider must not send
      // a doomed unlock for a lock the metastore has already dropped.
      assertThrows(HoodieLockException.class, provider::unlock);
      verify(client, never()).unlock(anyLong());
    } finally {
      provider.close();
    }
  }

  @Test
  void transientHeartbeatFailureKeepsRenewingTheLock() throws Exception {
    IMetaStoreClient client = mock(IMetaStoreClient.class);
    when(client.lock(any())).thenReturn(acquiredLock(LOCK_ID));
    CountDownLatch heartbeats = new CountDownLatch(2);
    doAnswer(invocation -> {
      heartbeats.countDown();
      throw new TException("transient failure");
    }).when(client).heartbeat(anyLong(), anyLong());

    HiveMetastoreBasedLockProvider provider = new HiveMetastoreBasedLockProvider(lockConfiguration, client);
    try {
      assertTrue(provider.acquireLock(1000L, TimeUnit.MILLISECONDS, lockComponent));

      assertTrue(heartbeats.await(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS),
          "a transient failure must not stop the heartbeat schedule");
      assertNotNull(provider.getLock(), "a transient failure must not drop the lock");

      provider.unlock();
      verify(client).unlock(LOCK_ID);
      assertNull(provider.getLock());
    } finally {
      provider.close();
    }
  }

  @Test
  void lockCanBeAcquiredAgainAfterItWasLost() throws Exception {
    IMetaStoreClient client = mock(IMetaStoreClient.class);
    when(client.lock(any())).thenReturn(acquiredLock(LOCK_ID), acquiredLock(OTHER_LOCK_ID));
    // Only the first lock is expired by the metastore; heartbeating the second one succeeds.
    doThrow(new NoSuchLockException("lock " + LOCK_ID + " does not exist"))
        .doNothing()
        .when(client).heartbeat(anyLong(), anyLong());

    HiveMetastoreBasedLockProvider provider = new HiveMetastoreBasedLockProvider(lockConfiguration, client);
    try {
      assertTrue(provider.acquireLock(1000L, TimeUnit.MILLISECONDS, lockComponent));
      awaitUntil(() -> provider.getLock() == null, "the lost lock must be dropped by the heartbeat");

      // Acquiring again must clear the lost-lock state, otherwise the provider would keep failing
      // to release locks it holds perfectly well.
      assertTrue(provider.acquireLock(1000L, TimeUnit.MILLISECONDS, lockComponent));
      provider.unlock();

      verify(client).unlock(OTHER_LOCK_ID);
      assertNull(provider.getLock());
    } finally {
      provider.close();
    }
  }

  @Test
  void lostLockStateDoesNotLeakIntoTheNextAcquire() throws Exception {
    IMetaStoreClient client = mock(IMetaStoreClient.class);
    when(client.lock(any())).thenReturn(acquiredLock(LOCK_ID), waitingLock(OTHER_LOCK_ID));
    doThrow(new NoSuchLockException("lock " + LOCK_ID + " does not exist"))
        .doNothing()
        .when(client).heartbeat(anyLong(), anyLong());

    HiveMetastoreBasedLockProvider provider = new HiveMetastoreBasedLockProvider(lockConfiguration, client);
    try {
      assertTrue(provider.acquireLock(1000L, TimeUnit.MILLISECONDS, lockComponent));
      awaitUntil(() -> provider.getLock() == null, "the lost lock must be dropped by the heartbeat");

      // The second attempt only got queued, so it leaves no lock behind and nothing was expired
      // by the metastore this time round.
      assertFalse(provider.acquireLock(1000L, TimeUnit.MILLISECONDS, lockComponent));

      // Releasing must not report the loss that belonged to the previous lock.
      assertDoesNotThrow(provider::unlock);
    } finally {
      provider.close();
    }
  }

  @Test
  void unlockStaysSilentWhenNoLockWasEverHeld() {
    IMetaStoreClient client = mock(IMetaStoreClient.class);

    HiveMetastoreBasedLockProvider provider = new HiveMetastoreBasedLockProvider(lockConfiguration, client);
    try {
      // Releasing a lock that was never acquired is still a no-op: only a lock the metastore took
      // away is reported as a failure.
      assertDoesNotThrow(provider::unlock);
    } finally {
      provider.close();
    }
  }

  private static ScheduledFuture<?> heartbeatFutureOf(HiveMetastoreBasedLockProvider provider) throws Exception {
    Field field = HiveMetastoreBasedLockProvider.class.getDeclaredField("future");
    field.setAccessible(true);
    return (ScheduledFuture<?>) field.get(provider);
  }

  private static void awaitUntil(BooleanSupplier condition, String message) throws InterruptedException {
    long deadline = System.currentTimeMillis() + AWAIT_TIMEOUT_MS;
    while (System.currentTimeMillis() < deadline) {
      if (condition.getAsBoolean()) {
        return;
      }
      Thread.sleep(20L);
    }
    fail(message);
  }

  private static LockResponse acquiredLock(long lockId) {
    return lockResponse(lockId, LockState.ACQUIRED);
  }

  private static LockResponse waitingLock(long lockId) {
    return lockResponse(lockId, LockState.WAITING);
  }

  private static LockResponse lockResponse(long lockId, LockState state) {
    LockResponse response = new LockResponse();
    response.setLockid(lockId);
    response.setState(state);
    return response;
  }
}
