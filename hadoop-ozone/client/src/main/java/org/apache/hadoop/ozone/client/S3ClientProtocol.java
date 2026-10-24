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

package org.apache.hadoop.ozone.client;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Objects;
import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;

/** Adapts client operations to the S3 volume context. */
final class S3ClientProtocol implements InvocationHandler {
  private final ClientProtocol delegate;

  private S3ClientProtocol(ClientProtocol delegate) {
    this.delegate = Objects.requireNonNull(delegate, "delegate == null");
  }

  static ClientProtocol create(ClientProtocol delegate) {
    return (ClientProtocol) Proxy.newProxyInstance(ClientProtocol.class.getClassLoader(),
        new Class<?>[] {ClientProtocol.class}, new S3ClientProtocol(delegate));
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    if (method.getDeclaringClass() == Object.class) {
      switch (method.getName()) {
      case "equals":
        return proxy == args[0];
      case "hashCode":
        return System.identityHashCode(proxy);
      default:
        return delegate.toString();
      }
    }
    if ("deleteKey".equals(method.getName()) && args[0] == null
        && !OzoneManagerVersion.S3_VOLUME_CONTEXT_RESOLUTION.isSupportedBy(delegate.getOmVersion())) {
      args = args.clone();
      args[0] = delegate.getS3VolumeContext().getOmVolumeArgs().getVolume();
    }
    try {
      return method.invoke(delegate, args);
    } catch (InvocationTargetException e) {
      throw e.getCause();
    }
  }
}
