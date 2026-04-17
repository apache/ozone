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

package org.apache.hadoop.hdds.scm.ha.invoker;

import static org.apache.hadoop.hdds.scm.ha.SCMHAInvocationHandler.translateException;

import java.util.function.Function;
import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol.RequestType;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.ha.SCMHandler;
import org.apache.hadoop.hdds.scm.ha.SCMRatisRequest;
import org.apache.hadoop.hdds.scm.ha.SCMRatisResponse;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServer;
import org.apache.ratis.protocol.Message;

/**
 * Invokes methods without using reflection.
 */
public abstract class ScmInvoker<T extends SCMHandler> {
  private final T impl;
  private final T proxy;
  private final SCMRatisServer ratisHandler;

  ScmInvoker(T impl, Function<ScmInvoker<T>, T> proxy, SCMRatisServer ratisHandler) {
    this.impl = impl;
    this.proxy = proxy.apply(this);
    this.ratisHandler = ratisHandler;
  }

  public final RequestType getType() {
    return getImpl().getType();
  }

  public abstract Class<T> getApi();

  public final T getImpl() {
    return impl;
  }

  public final T getProxy() {
    return proxy;
  }

  /** For non-@Replicate methods. */
  public abstract Message invokeLocal(String methodName, Object[] args) throws Exception;

  /** For @Replicate DIRECT methods. */
  final Object invokeReplicateDirect(NameAndParameterTypes method, Object[] args) throws SCMException {
    try {
      final SCMRatisRequest request = SCMRatisRequest.of(
          getType(), method.name(), method.getParameterTypes(args.length), args);
      final SCMRatisResponse response = ratisHandler.submitRequest(request);
      if (response.isSuccess()) {
        return response.getResult();
      }
      throw response.getException();
    } catch (Exception e) {
      throw translateException(e);
    }
  }

  interface NameAndParameterTypes {
    String name();
    
    Class<?>[] getParameterTypes(int numArgs);
  }
}
