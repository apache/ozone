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

import java.util.List;
import java.util.UUID;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.ha.SCMRatisResponse;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServer;
import org.apache.hadoop.hdds.security.symmetric.ManagedSecretKey;
import org.apache.hadoop.hdds.security.symmetric.SecretKeyState;
import org.apache.ratis.protocol.Message;

/** Code generated for {@link SecretKeyState}.  Do not modify. */
public class SecretKeyStateInvoker extends ScmInvoker<SecretKeyState> {
  enum ReplicateMethod implements NameAndParameterTypes {
    updateKeys(new Class<?>[][] {
        null,
        new Class<?>[] {List.class}
    });

    private final Class<?>[][] parameterTypes;

    ReplicateMethod(Class<?>[][] parameterTypes) {
      this.parameterTypes = parameterTypes;
    }

    @Override
    public Class<?>[] getParameterTypes(int numArgs) {
      return parameterTypes[numArgs];
    }
  }

  public SecretKeyStateInvoker(SecretKeyState impl, SCMRatisServer ratis) {
    super(impl, SecretKeyStateInvoker::newProxy, ratis);
  }

  @Override
  public Class<SecretKeyState> getApi() {
    return SecretKeyState.class;
  }

  static SecretKeyState newProxy(ScmInvoker<SecretKeyState> invoker) {
    return new SecretKeyState() {

      @Override
      public ManagedSecretKey getCurrentKey() {
        return invoker.getImpl().getCurrentKey();
      }

      @Override
      public ManagedSecretKey getKey(UUID arg0) {
        return invoker.getImpl().getKey(arg0);
      }

      @Override
      public List<ManagedSecretKey> getSortedKeys() {
        return invoker.getImpl().getSortedKeys();
      }

      @Override
      public void reinitialize(List arg0) {
        invoker.getImpl().reinitialize(arg0);
      }

      @Override
      public void updateKeys(List arg0) throws SCMException {
        final Object[] args = {arg0};
        invoker.invokeReplicateDirect(ReplicateMethod.updateKeys, args);
      }
    };
  }

  @SuppressWarnings("unchecked")
  @Override
  public Message invokeLocal(String methodName, Object[] p) throws Exception {
    final Class<?> returnType;
    final Object returnValue;
    switch (methodName) {
    case "getCurrentKey":
      returnType = ManagedSecretKey.class;
      returnValue = getImpl().getCurrentKey();
      break;

    case "getKey":
      final UUID arg0 = p.length > 0 ? (UUID) p[0] : null;
      returnType = ManagedSecretKey.class;
      returnValue = getImpl().getKey(arg0);
      break;

    case "getSortedKeys":
      returnType = List.class;
      returnValue = getImpl().getSortedKeys();
      break;

    case "reinitialize":
      final List arg1 = p.length > 0 ? (List) p[0] : null;
      getImpl().reinitialize(arg1);
      return Message.EMPTY;

    case "updateKeys":
      final List arg2 = p.length > 0 ? (List) p[0] : null;
      getImpl().updateKeys(arg2);
      return Message.EMPTY;

    default:
      throw new IllegalArgumentException("Method not found: " + methodName + " in SecretKeyState");
    }

    return SCMRatisResponse.encode(returnValue, returnType);
  }
}
