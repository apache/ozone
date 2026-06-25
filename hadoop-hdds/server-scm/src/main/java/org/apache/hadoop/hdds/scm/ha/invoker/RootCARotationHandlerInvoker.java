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

import java.io.IOException;
import org.apache.hadoop.hdds.scm.ha.SCMRatisResponse;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServer;
import org.apache.hadoop.hdds.scm.security.RootCARotationHandler;
import org.apache.ratis.protocol.Message;

/** Code generated for {@link RootCARotationHandler}.  Do not modify. */
public class RootCARotationHandlerInvoker extends ScmInvoker<RootCARotationHandler> {
  enum ReplicateMethod implements NameAndParameterTypes {
    rotationCommit(new Class<?>[][] {
        null,
        new Class<?>[] {String.class}
    }),
    rotationCommitted(new Class<?>[][] {
        null,
        new Class<?>[] {String.class}
    }),
    rotationPrepare(new Class<?>[][] {
        null,
        new Class<?>[] {String.class}
    }),
    rotationPrepareAck(new Class<?>[][] {
        null,
        null,
        null,
        new Class<?>[] {String.class, String.class, String.class}
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

  public RootCARotationHandlerInvoker(RootCARotationHandler impl, SCMRatisServer ratis) {
    super(impl, RootCARotationHandlerInvoker::newProxy, ratis);
  }

  @Override
  public Class<RootCARotationHandler> getApi() {
    return RootCARotationHandler.class;
  }

  static RootCARotationHandler newProxy(ScmInvoker<RootCARotationHandler> invoker) {
    return new RootCARotationHandler() {

      @Override
      public void resetRotationPrepareAcks() {
        invoker.getImpl().resetRotationPrepareAcks();
      }

      @Override
      public void rotationCommit(String arg0) throws IOException {
        final Object[] args = {arg0};
        invoker.invokeReplicateDirect(ReplicateMethod.rotationCommit, args);
      }

      @Override
      public void rotationCommitted(String arg0) throws IOException {
        final Object[] args = {arg0};
        invoker.invokeReplicateDirect(ReplicateMethod.rotationCommitted, args);
      }

      @Override
      public void rotationPrepare(String arg0) throws IOException {
        final Object[] args = {arg0};
        invoker.invokeReplicateDirect(ReplicateMethod.rotationPrepare, args);
      }

      @Override
      public void rotationPrepareAck(String arg0, String arg1, String arg2) throws IOException {
        final Object[] args = {arg0, arg1, arg2};
        invoker.invokeReplicateClient(ReplicateMethod.rotationPrepareAck, args);
      }

      @Override
      public int rotationPrepareAcks() {
        return invoker.getImpl().rotationPrepareAcks();
      }

      @Override
      public void setSubCACertId(String arg0) {
        invoker.getImpl().setSubCACertId(arg0);
      }
    };
  }

  @SuppressWarnings("unchecked")
  @Override
  public Message invokeLocal(String methodName, Object[] p) throws Exception {
    final Class<?> returnType;
    final Object returnValue;
    switch (methodName) {
    case "resetRotationPrepareAcks":
      getImpl().resetRotationPrepareAcks();
      return Message.EMPTY;

    case "rotationCommit":
      final String arg0 = p.length > 0 ? (String) p[0] : null;
      getImpl().rotationCommit(arg0);
      return Message.EMPTY;

    case "rotationCommitted":
      final String arg1 = p.length > 0 ? (String) p[0] : null;
      getImpl().rotationCommitted(arg1);
      return Message.EMPTY;

    case "rotationPrepare":
      final String arg2 = p.length > 0 ? (String) p[0] : null;
      getImpl().rotationPrepare(arg2);
      return Message.EMPTY;

    case "rotationPrepareAck":
      final String arg3 = p.length > 0 ? (String) p[0] : null;
      final String arg4 = p.length > 1 ? (String) p[1] : null;
      final String arg5 = p.length > 2 ? (String) p[2] : null;
      getImpl().rotationPrepareAck(arg3, arg4, arg5);
      return Message.EMPTY;

    case "rotationPrepareAcks":
      returnType = int.class;
      returnValue = getImpl().rotationPrepareAcks();
      break;

    case "setSubCACertId":
      final String arg6 = p.length > 0 ? (String) p[0] : null;
      getImpl().setSubCACertId(arg6);
      return Message.EMPTY;

    default:
      throw new IllegalArgumentException("Method not found: " + methodName + " in RootCARotationHandler");
    }

    return SCMRatisResponse.encode(returnValue, returnType);
  }
}
