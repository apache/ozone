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
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;

/** Define static properties of stateful services. */
public final class StatefulServiceDefinition<CONF extends Message> {
  private final String name;
  private final Parser<CONF> parser;

  public StatefulServiceDefinition(String name, Parser<CONF> parser) {
    this.name = name;
    this.parser = parser;
  }

  public String getServiceName() {
    return name;
  }

  public CONF deserialize(ByteString serialized) throws InvalidProtocolBufferException {
    if (serialized == null) {
      return null;
    }
    return parser.parseFrom(serialized);
  }
}
