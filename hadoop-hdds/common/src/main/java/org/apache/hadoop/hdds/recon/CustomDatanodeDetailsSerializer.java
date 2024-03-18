/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.recon;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;

import java.io.IOException;

/**
 * This is custom jackson serializer class for DetanodeDetails class.
 * Jackson serializer is being used to serialize DatanodeDetails class object
 * to map and serialize respective object fields to JSON fields.
 */
public class CustomDatanodeDetailsSerializer extends StdSerializer<DatanodeDetails> {

  protected CustomDatanodeDetailsSerializer(Class<DatanodeDetails> t) {
    super(t);
  }
  public CustomDatanodeDetailsSerializer() {
    this(null);
  }

  /**
   * This method is a call back method to serialize respective object fields to JSON fields.
   *
   * @param datanodeDetails the datanodeDetails class object
   * @param gen the JSON Generator
   * @param provider the SerializerProvider
   * @throws IOException
   */
  @Override
  public void serialize(DatanodeDetails datanodeDetails, JsonGenerator gen, SerializerProvider provider)
      throws IOException {
    gen.writeStartObject();
    gen.writeNumberField("level", datanodeDetails.getLevel());
    gen.writeNumberField("cost", datanodeDetails.getCost());
    gen.writeStringField("uuid", datanodeDetails.getUuid().toString());
    gen.writeStringField("uuidString", datanodeDetails.getUuidString());
    gen.writeStringField("ipAddress", datanodeDetails.getIpAddress());
    gen.writeStringField("hostName", datanodeDetails.getHostName());
    gen.writeFieldName("ports");
    gen.writeStartArray();
    for (DatanodeDetails.Port port : datanodeDetails.getPorts()) {
      gen.writeStartObject();
      gen.writeStringField("name", port.getName().name());
      gen.writeNumberField("value", port.getValue());
      gen.writeEndObject();
    }
    gen.writeEndArray();
    gen.writeEndObject();
  }
}

