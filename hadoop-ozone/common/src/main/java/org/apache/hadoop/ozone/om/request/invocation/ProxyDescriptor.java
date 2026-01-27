package org.apache.hadoop.ozone.om.request.invocation;

import org.apache.hadoop.io.retry.AtMostOnce;
import org.apache.hadoop.io.retry.FailoverProxyProvider;
import org.apache.hadoop.io.retry.Idempotent;

import java.io.IOException;
import java.lang.reflect.Method;

import static org.apache.hadoop.ozone.om.request.invocation.OzoneRetryInvocationHandler.LOG;

class ProxyDescriptor<T> {
  private final FailoverProxyProvider<T> fpp;
  /**
   * Count the associated proxy provider has ever been failed over.
   */
  private long failoverCount = 0;

  private FailoverProxyProvider.ProxyInfo<T> proxyInfo;

  ProxyDescriptor(FailoverProxyProvider<T> fpp) {
    this.fpp = fpp;
    this.proxyInfo = fpp.getProxy();
  }

  FailoverProxyProvider<T> getProxyProvider() {
    return fpp;
  }

  synchronized FailoverProxyProvider.ProxyInfo<T> getProxyInfo() {
    return proxyInfo;
  }

  synchronized T getProxy() {
    return proxyInfo.proxy;
  }

  synchronized long getFailoverCount() {
    return failoverCount;
  }

  synchronized void failover(long expectedFailoverCount, Method method,
                             int callId) {
    // Make sure that concurrent failed invocations only cause a single
    // actual failover.
    if (failoverCount == expectedFailoverCount) {
      fpp.performFailover(proxyInfo.proxy);
      failoverCount++;
    } else {
      LOG.warn("A failover has occurred since the start of call #{} {}", callId,
          proxyInfo.getString(method.getName()));
    }
    proxyInfo = fpp.getProxy();
  }

  boolean idempotentOrAtMostOnce(Method method) throws NoSuchMethodException {
    final Method m = fpp.getInterface()
        .getMethod(method.getName(), method.getParameterTypes());
    return m.isAnnotationPresent(Idempotent.class)
        || m.isAnnotationPresent(AtMostOnce.class);
  }

  void close() throws IOException {
    fpp.close();
  }

  public FailoverProxyProvider<T> getFpp() {
    return fpp;
  }
}
