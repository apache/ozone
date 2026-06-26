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

package org.apache.hadoop.io_.retry;

import static org.apache.hadoop.io_.retry.RetryPolicies.RETRY_FOREVER;
import static org.apache.hadoop.io_.retry.RetryPolicies.TRY_ONCE_THEN_FAIL;
import static org.apache.hadoop.io_.retry.RetryPolicies.exponentialBackoffRetry;
import static org.apache.hadoop.io_.retry.RetryPolicies.retryForeverWithFixedSleep;
import static org.apache.hadoop.io_.retry.RetryPolicies.retryUpToMaximumCountWithFixedSleep;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.security.sasl.SaslException;
import org.apache.hadoop.io.retry.Idempotent;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryPolicy.RetryAction;
import org.apache.hadoop.io.retry.RetryPolicy.RetryAction.RetryDecision;
import org.apache.hadoop.io_.retry.UnreliableInterface.UnreliableException;
import org.apache.hadoop.ipc_.ProtocolTranslator;
import org.apache.hadoop.security.AccessControlException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * TestRetryProxy tests the behaviour of the {@link RetryPolicy} class using
 * a certain method of {@link UnreliableInterface} implemented by
 * {@link UnreliableImplementation}.
 *
 * Some methods may be sensitive to the {@link Idempotent} annotation
 * (annotated in {@link UnreliableInterface}).
 */
public class TestRetryProxy {
  
  private UnreliableImplementation unreliableImpl;
  private RetryAction caughtRetryAction = null;
  
  @BeforeEach
  public void setUp() throws Exception {
    unreliableImpl = new UnreliableImplementation();
  }

  // answer mockPolicy's method with realPolicy, caught method's return value
  private void setupMockPolicy(RetryPolicy mockPolicy,
      final RetryPolicy realPolicy) throws Exception {
    when(mockPolicy.shouldRetry(any(Exception.class), anyInt(), anyInt(), anyBoolean()))
        .thenAnswer(invocation -> {
          Object[] args = invocation.getArguments();
          Exception e = (Exception) args[0];
          int retries = (int) args[1];
          int failovers = (int) args[2];
          boolean isIdempotentOrAtMostOnce = (boolean) args[3];
          caughtRetryAction = realPolicy.shouldRetry(e, retries, failovers,
              isIdempotentOrAtMostOnce);
          return caughtRetryAction;
        });
  }

  @Test
  public void testTryOnceThenFail() throws Exception {
    RetryPolicy policy = mock(RetryPolicies.TryOnceThenFail.class);
    RetryPolicy realPolicy = TRY_ONCE_THEN_FAIL;
    setupMockPolicy(policy, realPolicy);

    UnreliableInterface unreliable = (UnreliableInterface)
        RetryProxy.create(UnreliableInterface.class, unreliableImpl, policy);
    unreliable.alwaysSucceeds();
    try {
      unreliable.failsOnceThenSucceeds();
      fail("Should fail");
    } catch (UnreliableException e) {
      // expected
      verify(policy, times(1)).shouldRetry(any(Exception.class), anyInt(),
          anyInt(), anyBoolean());
      assertEquals(RetryDecision.FAIL, caughtRetryAction.action);
      assertEquals("try once and fail.", caughtRetryAction.reason);
    } catch (Exception e) {
      fail("Other exception other than UnreliableException should also get " +
          "failed.");
    }
  }

  /**
   * Test for {@link RetryInvocationHandler#isRpcInvocation(Object)}.
   */
  @Test
  public void testRpcInvocation() throws Exception {
    // For a proxy method should return true
    final UnreliableInterface unreliable = (UnreliableInterface)
        RetryProxy.create(UnreliableInterface.class, unreliableImpl, RETRY_FOREVER);
    assertTrue(RetryInvocationHandler.isRpcInvocation(unreliable));

    final AtomicInteger count = new AtomicInteger();
    // Embed the proxy in ProtocolTranslator
    ProtocolTranslator xlator = new ProtocolTranslator() {
      @Override
      public Object getUnderlyingProxyObject() {
        count.getAndIncrement();
        return unreliable;
      }
    };
    
    // For a proxy wrapped in ProtocolTranslator method should return true
    assertTrue(RetryInvocationHandler.isRpcInvocation(xlator));
    // Ensure underlying proxy was looked at
    assertEquals(1, count.get());
    
    // For non-proxy the method must return false
    assertFalse(RetryInvocationHandler.isRpcInvocation(new Object()));
  }
  
  @Test
  public void testRetryForever() throws UnreliableException {
    UnreliableInterface unreliable = (UnreliableInterface)
        RetryProxy.create(UnreliableInterface.class, unreliableImpl, RETRY_FOREVER);
    unreliable.alwaysSucceeds();
    unreliable.failsOnceThenSucceeds();
    unreliable.failsTenTimesThenSucceeds();
  }

  @Test
  public void testRetryForeverWithFixedSleep() throws UnreliableException {
    UnreliableInterface unreliable = (UnreliableInterface) RetryProxy.create(
        UnreliableInterface.class, unreliableImpl,
        retryForeverWithFixedSleep(1, TimeUnit.MILLISECONDS));
    unreliable.alwaysSucceeds();
    unreliable.failsOnceThenSucceeds();
    unreliable.failsTenTimesThenSucceeds();
  }

  @Test
  public void testRetryUpToMaximumCountWithFixedSleep() throws
      Exception {

    RetryPolicy policy = mock(RetryPolicies.RetryUpToMaximumCountWithFixedSleep.class);
    int maxRetries = 8;
    RetryPolicy realPolicy = retryUpToMaximumCountWithFixedSleep(maxRetries, 1, TimeUnit.NANOSECONDS);
    setupMockPolicy(policy, realPolicy);

    UnreliableInterface unreliable = (UnreliableInterface)
        RetryProxy.create(UnreliableInterface.class, unreliableImpl, policy);
    // shouldRetry += 1
    unreliable.alwaysSucceeds();
    // shouldRetry += 2
    unreliable.failsOnceThenSucceeds();
    try {
      // shouldRetry += (maxRetries -1) (just failed once above)
      unreliable.failsTenTimesThenSucceeds();
      fail("Should fail");
    } catch (UnreliableException e) {
      // expected
      verify(policy, times(maxRetries + 2)).shouldRetry(any(Exception.class),
          anyInt(), anyInt(), anyBoolean());
      assertEquals(RetryDecision.FAIL, caughtRetryAction.action);
      assertEquals(RetryPolicies.RetryUpToMaximumCountWithFixedSleep.constructReasonString(
          maxRetries), caughtRetryAction.reason);
    } catch (Exception e) {
      fail("Other exception other than UnreliableException should also get " +
          "failed.");
    }
  }
  
  @Test
  public void testExponentialRetry() throws UnreliableException {
    UnreliableInterface unreliable = (UnreliableInterface) RetryProxy.create(UnreliableInterface.class, unreliableImpl,
        exponentialBackoffRetry(5, 1L, TimeUnit.NANOSECONDS));
    unreliable.alwaysSucceeds();
    unreliable.failsOnceThenSucceeds();
    try {
      unreliable.failsTenTimesThenSucceeds();
      fail("Should fail");
    } catch (UnreliableException e) {
      // expected
    }
  }
  
  @Test
  public void testRetryInterruptible() throws Throwable {
    final UnreliableInterface unreliable = (UnreliableInterface)
        RetryProxy.create(UnreliableInterface.class, unreliableImpl,
            retryUpToMaximumCountWithFixedSleep(10, 10, TimeUnit.SECONDS));
    
    final CountDownLatch latch = new CountDownLatch(1);
    final AtomicReference<Thread> futureThread = new AtomicReference<Thread>();
    ExecutorService exec = Executors.newSingleThreadExecutor();
    try {
      Future<Throwable> future = exec.submit(() -> {
        futureThread.set(Thread.currentThread());
        latch.countDown();
        try {
          unreliable.alwaysFailsWithFatalException();
        } catch (UndeclaredThrowableException ute) {
          return ute.getCause();
        }
        return null;
      });
      latch.await();
      Thread.sleep(1000); // time to fail and sleep
      assertTrue(futureThread.get().isAlive());
      futureThread.get().interrupt();
      Throwable e = future.get(1, TimeUnit.SECONDS); // should return immediately
      assertNotNull(e);
      assertEquals(InterruptedIOException.class, e.getClass());
      assertEquals("Retry interrupted", e.getMessage());
      assertEquals(InterruptedException.class, e.getCause().getClass());
      assertEquals("sleep interrupted", e.getCause().getMessage());
    } finally {
      exec.shutdown();
    }
  }

  @Test
  public void testNoRetryOnSaslError() throws Exception {
    RetryPolicy policy = mock(RetryPolicy.class);
    RetryPolicy realPolicy = RetryPolicies.failoverOnNetworkException(5);
    setupMockPolicy(policy, realPolicy);

    UnreliableInterface unreliable = (UnreliableInterface) RetryProxy.create(
        UnreliableInterface.class, unreliableImpl, policy);

    try {
      unreliable.failsWithSASLExceptionTenTimes();
      fail("Should fail");
    } catch (SaslException e) {
      // expected
      verify(policy, times(1)).shouldRetry(any(Exception.class), anyInt(),
          anyInt(), anyBoolean());
      assertEquals(RetryDecision.FAIL, caughtRetryAction.action);
    }
  }

  @Test
  public void testNoRetryOnAccessControlException() throws Exception {
    RetryPolicy policy = mock(RetryPolicy.class);
    RetryPolicy realPolicy = RetryPolicies.failoverOnNetworkException(5);
    setupMockPolicy(policy, realPolicy);

    UnreliableInterface unreliable = (UnreliableInterface) RetryProxy.create(
        UnreliableInterface.class, unreliableImpl, policy);

    try {
      unreliable.failsWithAccessControlExceptionEightTimes();
      fail("Should fail");
    } catch (AccessControlException e) {
      // expected
      verify(policy, times(1)).shouldRetry(any(Exception.class), anyInt(),
          anyInt(), anyBoolean());
      assertEquals(RetryDecision.FAIL, caughtRetryAction.action);
    }
  }

  @Test
  public void testWrappedAccessControlException() throws Exception {
    RetryPolicy policy = mock(RetryPolicy.class);
    RetryPolicy realPolicy = RetryPolicies.failoverOnNetworkException(5);
    setupMockPolicy(policy, realPolicy);

    UnreliableInterface unreliable = (UnreliableInterface) RetryProxy.create(
        UnreliableInterface.class, unreliableImpl, policy);

    try {
      unreliable.failsWithWrappedAccessControlException();
      fail("Should fail");
    } catch (IOException expected) {
      verify(policy, times(1)).shouldRetry(any(Exception.class), anyInt(),
          anyInt(), anyBoolean());
      assertEquals(RetryDecision.FAIL, caughtRetryAction.action);
    }
  }
}
