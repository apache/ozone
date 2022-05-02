/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.ha;

import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

/**
 * A StatefulService is an SCMService that persists configuration to RocksDB.
 * The service must define this configuration as a Protobuf message.
 */
public abstract class StatefulService implements SCMService {
  private final StatefulServiceStateManager stateManager;

  /**
   * Initialize a StatefulService from an extending class.
   * @param scm {@link StorageContainerManager}
   */
  protected StatefulService(StorageContainerManager scm) {
    stateManager = scm.getStatefulServiceStateManager();
  }

  /**
   * Persists the specified {@link GeneratedMessage} configurationMessage
   * to RocksDB with this service's {@link SCMService#getServiceName()} as the
   * key.
   * @param configurationMessage configuration GeneratedMessage to persist
   * @throws IOException on failure to persist configuration
   */
  protected final void saveConfiguration(GeneratedMessage configurationMessage)
      throws IOException {
    stateManager.saveConfiguration(getServiceName(),
        configurationMessage.toByteString());
  }

  /**
   * Reads persisted configuration mapped to this service's
   * {@link SCMService#getServiceName()} name.
   *
   * @param configType the Class object of the protobuf message type
   * @param <T>        the Type of the protobuf message
   * @return persisted protobuf message
   * @throws IOException on failure to fetch the message from DB or when
   *                     parsing it. ensure the specified configType is correct
   */
  protected final <T extends GeneratedMessage> T readConfiguration(
      Class<T> configType) throws IOException {
    try {
      return configType.cast(ReflectionUtil.getMethod(configType,
              "parseFrom", ByteString.class)
          .invoke(null, stateManager.readConfiguration(getServiceName())));
    } catch (NoSuchMethodException | IllegalAccessException
        | InvocationTargetException e) {
      e.printStackTrace();
      throw new InvalidProtocolBufferException("GeneratedMessage cannot " +
          "be parsed for type " + configType + ": " + e.getMessage());
    }
  }
}
