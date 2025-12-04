/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.fs;

import static org.apache.hadoop.hdds.fs.MockSpaceUsageCheckParams.newBuilder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.fs.MockSpaceUsageCheckParams.Builder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.stubbing.Answer;

/**
 * Tests for {@link CachingSpaceUsageSource}.
 */
class TestCachingSpaceUsageSource {

  @TempDir
  private static File dir;

  @Test
  void providesInitialValueUntilStarted() {
    final long initialValue = validInitialValue();
    SpaceUsageCheckParams params = paramsBuilder(new AtomicLong(initialValue))
        .withRefresh(Duration.ZERO)
        .build();

    SpaceUsageSource subject = new CachingSpaceUsageSource(params);

    assertEquals(initialValue, subject.getUsedSpace());
    assertAvailableWasUpdated(params.getSource(), subject);
  }

  @Test
  void ignoresMissingInitialValue() {
    SpaceUsageCheckParams params = paramsBuilder()
        .withRefresh(Duration.ZERO)
        .build();

    SpaceUsageSource subject = new CachingSpaceUsageSource(params);

    assertEquals(0, subject.getUsedSpace());
    assertAvailableWasUpdated(params.getSource(), subject);
  }

  @Test
  void updatesValueFromSourceUponStartIfPeriodicRefreshNotConfigured() {
    AtomicLong savedValue = new AtomicLong(validInitialValue());
    SpaceUsageCheckParams params = paramsBuilder(savedValue)
        .withRefresh(Duration.ZERO).build();

    CachingSpaceUsageSource subject = new CachingSpaceUsageSource(params);
    subject.start();

    assertSubjectWasRefreshed(params.getSource(), subject);
  }

  @Test
  void schedulesRefreshWithDelayIfConfigured() {
    long initialValue = validInitialValue();
    AtomicLong savedValue = new AtomicLong(initialValue);
    SpaceUsageCheckParams params = paramsBuilder(savedValue)
        .build();
    Duration refresh = params.getRefresh();
    ScheduledExecutorService executor = sameThreadExecutorWithoutDelay();

    CachingSpaceUsageSource subject =
        new CachingSpaceUsageSource(params, executor);
    subject.start();

    verifyRefreshWasScheduled(executor, refresh.toMillis(), refresh);
    assertSubjectWasRefreshed(params.getSource(), subject);
    assertEquals(initialValue, savedValue.get(),
        "value should not have been saved to file yet");
  }

  @Test
  void schedulesImmediateRefreshIfInitialValueMissing() {
    final long initialValue = missingInitialValue();
    AtomicLong savedValue = new AtomicLong(initialValue);
    SpaceUsageCheckParams params = paramsBuilder(savedValue).build();
    ScheduledExecutorService executor = sameThreadExecutorWithoutDelay();

    CachingSpaceUsageSource subject =
        new CachingSpaceUsageSource(params, executor);
    subject.start();

    verifyRefreshWasScheduled(executor, 0L, params.getRefresh());
    assertSubjectWasRefreshed(params.getSource(), subject);
    assertEquals(initialValue, savedValue.get(),
        "value should not have been saved to file yet");
  }

  @Test
  void savesValueOnShutdown() {
    AtomicLong savedValue = new AtomicLong(validInitialValue());
    SpaceUsageSource source = mock(SpaceUsageSource.class);
    final long usedSpace = 4L;
    when(source.getUsedSpace()).thenReturn(usedSpace, 5L, 6L);
    SpaceUsageCheckParams params = paramsBuilder(savedValue).withSource(source)
        .build();
    ScheduledFuture<?> future = mock(ScheduledFuture.class);
    ScheduledExecutorService executor = sameThreadExecutorWithoutDelay(future);

    CachingSpaceUsageSource subject =
        new CachingSpaceUsageSource(params, executor);
    subject.start();
    subject.shutdown();

    assertEquals(usedSpace, savedValue.get(),
        "value should have been saved to file");
    assertEquals(usedSpace, subject.getUsedSpace(),
        "no further updates from source expected");
    verify(future, times(2)).cancel(true);
    verify(executor).shutdown();
  }

  @Test
  void decrementUsedSpaceMoreThanCurrent() {
    SpaceUsageCheckParams params = paramsBuilder(new AtomicLong(50))
        .withRefresh(Duration.ZERO)
        .build();
    CachingSpaceUsageSource subject = new CachingSpaceUsageSource(params);
    SpaceUsageSource original = subject.snapshot();

    // Try to decrement more than the current value
    final long change = original.getUsedSpace() * 2;
    subject.decrementUsedSpace(change);

    // should not drop below 0
    assertEquals(0, subject.getUsedSpace());
    // available and used change by same amount (in opposite directions)
    assertEquals(original.getAvailable() + original.getUsedSpace(), subject.getAvailable());
    assertSnapshotIsUpToDate(subject);
  }

  @Test
  void decrementAvailableSpaceMoreThanCurrent() {
    SpaceUsageCheckParams params = paramsBuilder(new AtomicLong(50))
        .withRefresh(Duration.ZERO)
        .build();
    CachingSpaceUsageSource subject = new CachingSpaceUsageSource(params);
    SpaceUsageSource original = subject.snapshot();

    // Try to decrement more than the current value
    final long change = original.getAvailable() * 2;
    subject.incrementUsedSpace(change);

    // should not drop below 0
    assertEquals(0, subject.getAvailable());
    // available and used change by same amount (in opposite directions)
    assertEquals(original.getUsedSpace() + original.getAvailable(), subject.getUsedSpace());
    assertSnapshotIsUpToDate(subject);
  }

  private static void assertSnapshotIsUpToDate(SpaceUsageSource subject) {
    SpaceUsageSource snapshot = subject.snapshot();
    assertEquals(subject.getCapacity(), snapshot.getCapacity());
    assertEquals(subject.getAvailable(), snapshot.getAvailable());
    assertEquals(subject.getUsedSpace(), snapshot.getUsedSpace());
  }

  private static long missingInitialValue() {
    return 0L;
  }

  private static long validInitialValue() {
    return RandomUtils.secure().randomLong(1, 100);
  }

  private static Builder paramsBuilder(AtomicLong savedValue) {
    return paramsBuilder()
        .withPersistence(MockSpaceUsagePersistence.inMemory(savedValue));
  }

  private static Builder paramsBuilder() {
    return newBuilder(dir)
        .withSource(MockSpaceUsageSource.fixed(10000, 1000))
        .withRefresh(Duration.ofMinutes(5));
  }

  private static ScheduledExecutorService sameThreadExecutorWithoutDelay() {
    return sameThreadExecutorWithoutDelay(mock(ScheduledFuture.class));
  }

  private static ScheduledExecutorService sameThreadExecutorWithoutDelay(
      ScheduledFuture<?> result) {

    ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
    when(executor.scheduleWithFixedDelay(any(), anyLong(),
        anyLong(), any()))
        .thenAnswer((Answer<ScheduledFuture<?>>) invocation -> {
          Runnable task = invocation.getArgument(0);
          task.run();
          return result;
        });
    return executor;
  }

  private static void verifyRefreshWasScheduled(
      ScheduledExecutorService executor, long expectedInitialDelay,
      Duration refresh) {

    // refresh usedSpace
    verify(executor).scheduleWithFixedDelay(any(), eq(expectedInitialDelay),
        eq(refresh.toMillis()), eq(TimeUnit.MILLISECONDS));

    // update available/capacity
    final long oneMinute = Duration.ofMinutes(1).toMillis();
    final long delay = Math.min(refresh.toMillis(), oneMinute);
    verify(executor).scheduleWithFixedDelay(any(), eq(delay),
        eq(delay), eq(TimeUnit.MILLISECONDS));
  }

  private static void assertAvailableWasUpdated(SpaceUsageSource source,
      SpaceUsageSource subject) {

    assertEquals(source.getCapacity(), subject.getCapacity());
    assertEquals(source.getAvailable(), subject.getAvailable());
    assertSnapshotIsUpToDate(subject);
  }

  private static void assertSubjectWasRefreshed(SpaceUsageSource source,
      SpaceUsageSource subject) {

    assertAvailableWasUpdated(source, subject);
    assertEquals(source.getUsedSpace(), subject.getUsedSpace(),
        "subject should have been refreshed");
  }

}
