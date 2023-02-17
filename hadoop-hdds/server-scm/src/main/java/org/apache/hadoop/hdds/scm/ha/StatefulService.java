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
import com.google.protobuf.Message;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.TimeoutException;

/**
 * A StatefulService is an SCMService that persists configuration to RocksDB.
 * The service must define this configuration as a Protobuf message.
 */
public abstract class StatefulService implements SCMService {
  private final StatefulServiceStateManager stateManager;

  /**
   * Initialize a StatefulService from an extending class.
   * @param stateManager a reference to the
   * {@link StatefulServiceStateManager} from SCM.
   */
  protected StatefulService(StatefulServiceStateManager stateManager) {
    this.stateManager = stateManager;
  }

  /**
   * Persists the specified {@link Message} configurationMessage
   * to RocksDB with this service's {@link SCMService#getServiceName()} as the
   * key.
   * @param configurationMessage configuration Message to persist
   * @throws IOException on failure to persist configuration
   */
  protected final void saveConfiguration(Message configurationMessage)
      throws IOException, TimeoutException {
    stateManager.saveConfiguration(getServiceName(),
        configurationMessage.toByteString());
  }

  /**
   * Reads persisted configuration mapped to this service's
   * {@link SCMService#getServiceName()} name.
   *
   * @param configType the Class object of the protobuf message type
   * @param <T>        the Type of the protobuf message
   * @return persisted protobuf message or null if the entry is not found
   * @throws IOException on failure to fetch the message from DB or when
   *                     parsing it. ensure the specified configType is correct
   */
  protected final <T extends Message> T readConfiguration(
      Class<T> configType) throws IOException {
    ByteString byteString = stateManager.readConfiguration(getServiceName());
    if (byteString == null) {
      return null;
    }
    try {
      return configType.cast(ReflectionUtil.getMethod(configType,
              "parseFrom", ByteString.class)
          .invoke(null, byteString));
    } catch (NoSuchMethodException | IllegalAccessException
        | InvocationTargetException e) {
      e.printStackTrace();
      throw new IOException("Message cannot be parsed. Ensure that "
          + configType + " is the correct expected message type for " +
          this.getServiceName(), e);
    }

  }
}
