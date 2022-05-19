/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.fs;

import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.fs.MockSpaceUsageCheckParams.Builder;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.hadoop.hdds.fs.MockSpaceUsageCheckParams.newBuilder;
import static org.apache.ozone.test.GenericTestUtils.getTestDir;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link CachingSpaceUsageSource}.
 */
public class TestCachingSpaceUsageSource {

  private static final File DIR =
      getTestDir(TestCachingSpaceUsageSource.class.getSimpleName());

  @Test
  public void providesInitialValueUntilStarted() {
    final long initialValue = validInitialValue();
    SpaceUsageCheckParams params = paramsBuilder(new AtomicLong(initialValue))
        .withRefresh(Duration.ZERO)
        .build();

    SpaceUsageSource subject = new CachingSpaceUsageSource(params);

    assertEquals(initialValue, subject.getUsedSpace());
  }

  @Test
  public void ignoresMissingInitialValue() {
    SpaceUsageCheckParams params = paramsBuilder()
        .withRefresh(Duration.ZERO)
        .build();

    SpaceUsageSource subject = new CachingSpaceUsageSource(params);

    assertEquals(0, subject.getUsedSpace());
  }

  @Test
  public void updatesValueFromSourceUponStartIfPeriodicRefreshNotConfigured() {
    AtomicLong savedValue = new AtomicLong(validInitialValue());
    SpaceUsageCheckParams params = paramsBuilder(savedValue)
        .withRefresh(Duration.ZERO).build();

    CachingSpaceUsageSource subject = new CachingSpaceUsageSource(params);
    subject.start();

    assertSubjectWasRefreshed(params.getSource().getUsedSpace(), subject);
  }

  @Test
  public void schedulesRefreshWithDelayIfConfigured() {
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
    assertSubjectWasRefreshed(params.getSource().getUsedSpace(), subject);
    assertEquals(initialValue, savedValue.get(),
        "value should not have been saved to file yet");
  }

  @Test
  public void schedulesImmediateRefreshIfInitialValueMissing() {
    final long initialValue = missingInitialValue();
    AtomicLong savedValue = new AtomicLong(initialValue);
    SpaceUsageCheckParams params = paramsBuilder(savedValue).build();
    ScheduledExecutorService executor = sameThreadExecutorWithoutDelay();

    CachingSpaceUsageSource subject =
        new CachingSpaceUsageSource(params, executor);
    subject.start();

    verifyRefreshWasScheduled(executor, 0L, params.getRefresh());
    assertSubjectWasRefreshed(params.getSource().getUsedSpace(), subject);
    assertEquals(initialValue, savedValue.get(),
        "value should not have been saved to file yet");
  }

  @Test
  public void savesValueOnShutdown() {
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
    verify(future).cancel(true);
    verify(executor).shutdown();
  }

  private static long missingInitialValue() {
    return 0L;
  }

  private static long validInitialValue() {
    return RandomUtils.nextLong(1, 100);
  }

  private static Builder paramsBuilder(AtomicLong savedValue) {
    return paramsBuilder()
        .withPersistence(MockSpaceUsagePersistence.inMemory(savedValue));
  }

  private static Builder paramsBuilder() {
    return newBuilder(DIR)
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

    verify(executor).scheduleWithFixedDelay(any(), eq(expectedInitialDelay),
        eq(refresh.toMillis()), eq(TimeUnit.MILLISECONDS));
  }

  private static void assertSubjectWasRefreshed(long expected,
      SpaceUsageSource subject) {

    assertEquals(expected, subject.getUsedSpace(),
        "subject should have been refreshed");
  }

}
