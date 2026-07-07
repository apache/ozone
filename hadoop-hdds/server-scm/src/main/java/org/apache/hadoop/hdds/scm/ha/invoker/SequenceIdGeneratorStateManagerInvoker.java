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
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.ha.SCMRatisResponse;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServer;
import org.apache.hadoop.hdds.scm.ha.SequenceIdGenerator.StateManager;
import org.apache.hadoop.hdds.scm.ha.SequenceIdType;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.ratis.protocol.Message;

/** Code generated for {@link StateManager}.  Do not modify. */
public class SequenceIdGeneratorStateManagerInvoker extends ScmInvoker<StateManager> {
  enum ReplicateMethod implements NameAndParameterTypes {
    allocateBatch(new Class<?>[][] {
        null,
        null,
        null,
        new Class<?>[] {String.class, Long.class, Long.class}
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

  public SequenceIdGeneratorStateManagerInvoker(StateManager impl, SCMRatisServer ratis) {
    super(impl, SequenceIdGeneratorStateManagerInvoker::newProxy, ratis);
  }

  @Override
  public Class<StateManager> getApi() {
    return StateManager.class;
  }

  static StateManager newProxy(ScmInvoker<StateManager> invoker) {
    return new StateManager() {

      @Override
      public Boolean allocateBatch(String arg0, Long arg1, Long arg2) throws SCMException {
        final Object[] args = {arg0, arg1, arg2};
        return (Boolean)invoker.invokeReplicateDirect(ReplicateMethod.allocateBatch, args);
      }

      @Override
      public Long getLastId(SequenceIdType arg0) {
        return invoker.getImpl().getLastId(arg0);
      }

      @Override
      public void reinitialize(Table arg0) throws IOException {
        invoker.getImpl().reinitialize(arg0);
      }
    };
  }

  @SuppressWarnings("unchecked")
  @Override
  public Message invokeLocal(String methodName, Object[] p) throws Exception {
    final Class<?> returnType;
    final Object returnValue;
    switch (methodName) {
    case "allocateBatch":
      final String arg0 = p.length > 0 ? (String) p[0] : null;
      final Long arg1 = p.length > 1 ? (Long) p[1] : null;
      final Long arg2 = p.length > 2 ? (Long) p[2] : null;
      returnType = Boolean.class;
      returnValue = getImpl().allocateBatch(arg0, arg1, arg2);
      break;

    case "getLastId":
      final SequenceIdType arg3 = p.length > 0 ? (SequenceIdType) p[0] : null;
      returnType = Long.class;
      returnValue = getImpl().getLastId(arg3);
      break;

    case "reinitialize":
      final Table arg4 = p.length > 0 ? (Table) p[0] : null;
      getImpl().reinitialize(arg4);
      return Message.EMPTY;

    default:
      throw new IllegalArgumentException("Method not found: " + methodName + " in StateManager");
    }

    return SCMRatisResponse.encode(returnValue, returnType);
  }
}
