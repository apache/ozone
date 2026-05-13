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

package org.apache.hadoop.hdds.scm.ha;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import java.io.IOException;

/**
 * A StatefulService is an SCMService that persists configuration to RocksDB.
 *
 * @param <CONF> The configuration type, which is a protobuf {@link Message}.
 */
public abstract class StatefulService<CONF extends Message> implements SCMService {
  private final String name;
  private final StatefulServiceStateManager stateManager;
  private final Parser<CONF> parser;

  /**
   * Initialize a StatefulService from an extending class.
   * @param stateManager a reference to the
   * {@link StatefulServiceStateManager} from SCM.
   */
  protected StatefulService(StatefulServiceStateManager stateManager, Parser<CONF> parser) {
    this.name = getClass().getSimpleName();
    this.stateManager = stateManager;
    this.parser = parser;
  }

  @Override
  public final String getServiceName() {
    return name;
  }

  /**
   * Persists the given configuration to RocksDB with {@link SCMService#getServiceName()} as the key.
   * @param configuration configuration to persist
   * @throws IOException on failure to persist configuration
   */
  protected final void saveConfiguration(CONF configuration) throws IOException {
    stateManager.saveConfiguration(getServiceName(), configuration.toByteString());
  }

  /**
   * Reads persisted configuration mapped to this service's
   * {@link SCMService#getServiceName()} name.
   *
   * @return persisted protobuf message or null if the entry is not found
   * @throws IOException on failure to fetch the message from DB or when
   *                     parsing it. ensure the specified configType is correct
   */
  protected final CONF readConfiguration() throws IOException {
    ByteString byteString = stateManager.readConfiguration(getServiceName());
    if (byteString == null) {
      return null;
    }
    return parser.parseFrom(byteString);
  }

  /**
   * Deletes the persisted configuration mapped to the specified serviceName.
   * @throws IOException on failure
   */
  protected final void deleteConfiguration() throws IOException {
    stateManager.deleteConfiguration(getServiceName());
  }
}
