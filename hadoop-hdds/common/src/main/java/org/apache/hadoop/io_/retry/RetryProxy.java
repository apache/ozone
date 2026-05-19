/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.io_.retry;

import java.lang.reflect.Proxy;
import org.apache.hadoop.io.retry.DefaultFailoverProxyProvider;
import org.apache.hadoop.io.retry.FailoverProxyProvider;
import org.apache.hadoop.io.retry.RetryPolicy;

/**
 * <p>
 * A factory for creating retry proxies.
 * </p>
 */
public class RetryProxy {
  /**
   * <p>
   * Create a proxy for an interface of an implementation class
   * using the same retry policy for each method in the interface. 
   * </p>
   * @param iface the interface that the retry will implement
   * @param implementation the instance whose methods should be retried
   * @param retryPolicy the policy for retrying method call failures
   * @param <T> T.
   * @return the retry proxy
   */
  public static <T> Object create(Class<T> iface, T implementation,
                              RetryPolicy retryPolicy) {
    return RetryProxy.create(iface,
        new DefaultFailoverProxyProvider<T>(iface, implementation),
        retryPolicy);
  }

  /**
   * Create a proxy for an interface of implementations of that interface using
   * the given {@link FailoverProxyProvider} and the same retry policy for each
   * method in the interface.
   * 
   * @param iface the interface that the retry will implement
   * @param proxyProvider provides implementation instances whose methods should be retried
   * @param retryPolicy the policy for retrying or failing over method call failures
   * @param <T> T.
   * @return the retry proxy
   */
  public static <T> Object create(Class<T> iface,
      FailoverProxyProvider<T> proxyProvider, RetryPolicy retryPolicy) {
    return Proxy.newProxyInstance(
        proxyProvider.getInterface().getClassLoader(),
        new Class<?>[] { iface },
        new RetryInvocationHandler<T>(proxyProvider, retryPolicy)
        );
  }

}
