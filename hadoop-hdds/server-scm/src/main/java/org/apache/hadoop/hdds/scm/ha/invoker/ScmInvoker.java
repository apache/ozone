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

import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol.RequestType;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.ha.SCMRatisRequest;
import org.apache.hadoop.hdds.scm.ha.SCMRatisResponse;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServer;

/**
 * Invokes methods without using reflection.
 */
public abstract class ScmInvoker<T> {
  static final Object[] NO_ARGS = {};

  private final SCMRatisServer ratisHandler;

  ScmInvoker(SCMRatisServer ratisHandler) {
    this.ratisHandler = ratisHandler;
  }

  public abstract RequestType getType();

  public abstract Class<T> getApi();

  public abstract T getImpl();

  public abstract T getProxy();

  abstract Class<?>[] getParameterTypes(String methodName);

  abstract Object invokeLocal(String methodName, Object[] args) throws Exception;

  Object invokeRatisServer(String methodName, Class<?>[] paramTypes,
          Object[] args) throws SCMException {
    try {
      final SCMRatisRequest request = SCMRatisRequest.of(
              getType(), methodName, paramTypes, args);
      final SCMRatisResponse response = ratisHandler.submitRequest(request);
      if (response.isSuccess()) {
        return response.getResult();
      }
      throw response.getException();
    } catch (Exception e) {
      throw translateException(e);
    }
  }
}
