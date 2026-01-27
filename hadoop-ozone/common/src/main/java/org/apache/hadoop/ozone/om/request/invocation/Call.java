package org.apache.hadoop.ozone.om.request.invocation;

import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.util.Time;

import java.io.InterruptedIOException;
import java.lang.reflect.Method;
import java.util.Arrays;

class Call {
  private final Method method;
  private final Object[] args;
  private final boolean isRpc;
  private final int callId;
  private final Counters counters = new Counters();

  private final RetryPolicy retryPolicy;
  private final OzoneRetryInvocationHandler<?> retryInvocationHandler;

  private RetryInfo retryInfo;

  Call(Method method, Object[] args, boolean isRpc, int callId,
       OzoneRetryInvocationHandler<?> retryInvocationHandler) {
    this.method = method;
    this.args = args;
    this.isRpc = isRpc;
    this.callId = callId;

    this.retryPolicy = retryInvocationHandler.getRetryPolicy(method);
    this.retryInvocationHandler = retryInvocationHandler;
  }

  public Object[] getArgs() {
    return this.args;
  }

  int getCallId() {
    return callId;
  }

  Counters getCounters() {
    return counters;
  }

  synchronized Long getWaitTime(final long now) {
    return retryInfo == null ? null : retryInfo.getRetryTime() - now;
  }

  /**
   * Invoke the call once without retrying.
   */
  synchronized CallReturn invokeOnce() {
    try {
      if (retryInfo != null) {
        return processWaitTimeAndRetryInfo();
      }

      // The number of times this invocation handler has ever been failed over
      // before this method invocation attempt. Used to prevent concurrent
      // failed method invocations from triggering multiple failover attempts.
      final long failoverCount = retryInvocationHandler.getFailoverCount();
      try {
        return invoke();
      } catch (Exception e) {
        if (OzoneRetryInvocationHandler.LOG.isTraceEnabled()) {
          OzoneRetryInvocationHandler.LOG.trace(toString(), e);
        }
        if (Thread.currentThread().isInterrupted()) {
          // If interrupted, do not retry.
          throw e;
        }

        retryInfo = retryInvocationHandler.handleException(
            method, callId, retryPolicy, counters, failoverCount, e);
        return processWaitTimeAndRetryInfo();
      }
    } catch (Throwable t) {
      return new CallReturn(t);
    }
  }

  /**
   * It first processes the wait time, if there is any,
   * and then invokes {@link #processRetryInfo()}.
   * <p>
   * If the wait time is positive, it either sleeps for synchronous calls
   * or immediately returns for asynchronous calls.
   *
   * @return {@link CallReturn#RETRY} if the retryInfo is processed;
   * otherwise, return {@link CallReturn#WAIT_RETRY}.
   */
  CallReturn processWaitTimeAndRetryInfo() throws InterruptedIOException {
    final Long waitTime = getWaitTime(Time.monotonicNow());
    OzoneRetryInvocationHandler.LOG.trace("#{} processRetryInfo: retryInfo={}, waitTime={}",
        callId, retryInfo, waitTime);
    if (waitTime != null && waitTime > 0) {
      try {
        Thread.sleep(retryInfo.getDelay());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        if (OzoneRetryInvocationHandler.LOG.isDebugEnabled()) {
          OzoneRetryInvocationHandler.LOG.debug("Interrupted while waiting to retry", e);
        }
        InterruptedIOException intIOE = new InterruptedIOException(
            "Retry interrupted");
        intIOE.initCause(e);
        throw intIOE;
      }
    }
    processRetryInfo();
    return CallReturn.RETRY;
  }

  synchronized void processRetryInfo() {
    counters.incRetries();
    if (retryInfo.isFailover()) {
      retryInvocationHandler.getProxyDescriptor().failover(
          retryInfo.getExpectedFailoverCount(), method, callId);
      counters.incFailovers();
    }
    retryInfo = null;
  }

  CallReturn invoke() throws Throwable {
    return new CallReturn(invokeMethod());
  }

  Object invokeMethod() throws Throwable {
    if (isRpc) {
      Client.setCallIdAndRetryCount(callId, counters.getRetries(),
          retryInvocationHandler.getAsyncCallHandler());
    }
    return retryInvocationHandler.invokeMethod(method, args);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "#" + callId + ": "
        + method.getDeclaringClass().getSimpleName() + "." + method.getName()
        + "(" + (args == null || args.length == 0 ? "" : Arrays.toString(args))
        + ")";
  }
}
