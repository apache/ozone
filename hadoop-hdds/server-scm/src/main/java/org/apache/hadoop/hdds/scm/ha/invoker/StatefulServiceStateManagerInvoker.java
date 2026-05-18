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

import com.google.protobuf.ByteString;
import java.io.IOException;
import org.apache.hadoop.hdds.scm.ha.SCMRatisResponse;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServer;
import org.apache.hadoop.hdds.scm.ha.StatefulServiceStateManager;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.ratis.protocol.Message;

/** Code generated for {@link StatefulServiceStateManager}.  Do not modify. */
public class StatefulServiceStateManagerInvoker extends ScmInvoker<StatefulServiceStateManager> {
  enum ReplicateMethod implements NameAndParameterTypes {
    deleteConfiguration(new Class<?>[][] {
        null,
        new Class<?>[] {String.class}
    }),
    saveConfiguration(new Class<?>[][] {
        null,
        null,
        new Class<?>[] {String.class, ByteString.class}
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

  public StatefulServiceStateManagerInvoker(StatefulServiceStateManager impl, SCMRatisServer ratis) {
    super(impl, StatefulServiceStateManagerInvoker::newProxy, ratis);
  }

  @Override
  public Class<StatefulServiceStateManager> getApi() {
    return StatefulServiceStateManager.class;
  }

  static StatefulServiceStateManager newProxy(ScmInvoker<StatefulServiceStateManager> invoker) {
    return new StatefulServiceStateManager() {

      @Override
      public void deleteConfiguration(String arg0) throws IOException {
        final Object[] args = {arg0};
        invoker.invokeReplicateDirect(ReplicateMethod.deleteConfiguration, args);
      }

      @Override
      public ByteString readConfiguration(String arg0) throws IOException {
        return invoker.getImpl().readConfiguration(arg0);
      }

      @Override
      public void reinitialize(Table arg0) {
        invoker.getImpl().reinitialize(arg0);
      }

      @Override
      public void saveConfiguration(String arg0, ByteString arg1) throws IOException {
        final Object[] args = {arg0, arg1};
        invoker.invokeReplicateDirect(ReplicateMethod.saveConfiguration, args);
      }
    };
  }

  @SuppressWarnings("unchecked")
  @Override
  public Message invokeLocal(String methodName, Object[] p) throws Exception {
    final Class<?> returnType;
    final Object returnValue;
    switch (methodName) {
    case "deleteConfiguration":
      final String arg0 = p.length > 0 ? (String) p[0] : null;
      getImpl().deleteConfiguration(arg0);
      return Message.EMPTY;

    case "readConfiguration":
      final String arg1 = p.length > 0 ? (String) p[0] : null;
      returnType = ByteString.class;
      returnValue = getImpl().readConfiguration(arg1);
      break;

    case "reinitialize":
      final Table arg2 = p.length > 0 ? (Table) p[0] : null;
      getImpl().reinitialize(arg2);
      return Message.EMPTY;

    case "saveConfiguration":
      final String arg3 = p.length > 0 ? (String) p[0] : null;
      final ByteString arg4 = p.length > 1 ? (ByteString) p[1] : null;
      getImpl().saveConfiguration(arg3, arg4);
      return Message.EMPTY;

    default:
      throw new IllegalArgumentException("Method not found: " + methodName + " in StatefulServiceStateManager");
    }

    return SCMRatisResponse.encode(returnValue, returnType);
  }
}
