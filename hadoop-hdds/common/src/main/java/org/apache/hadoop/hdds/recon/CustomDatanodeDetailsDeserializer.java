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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * This is custom jackson deserializer class for DatanodeDetails class.
 * Jackson deserializer is being used to deserialize json using JSON parser
 * and map JSON fields to respective DatanodeDetails class fields.
 */
public class CustomDatanodeDetailsDeserializer extends StdDeserializer<DatanodeDetails> {

  protected CustomDatanodeDetailsDeserializer(Class<DatanodeDetails> vc) {
    super(vc);
  }

  public CustomDatanodeDetailsDeserializer() {
    this(null);
  }

  @Override
  public DatanodeDetails deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
    JsonNode node = p.getCodec().readTree(p);
    List<DatanodeDetails.Port> ports = new ArrayList<>();
    JsonNode portsNode = node.get("ports");
    if (portsNode != null && portsNode.isArray()) {
      for (JsonNode portNode : portsNode) {
        DatanodeDetails.Port port = new DatanodeDetails.Port(
            DatanodeDetails.Port.Name.valueOf(portNode.get("name").asText()), portNode.get("value").asInt());
        ports.add(port);
      }
    }
    DatanodeDetails datanodeDetails = DatanodeDetails.newBuilder()
        .setLevel(node.get("level").asInt())
        .setCost(node.get("cost").asInt())
        .setUuid(UUID.fromString(node.get("uuid").asText()))
        .setUuidAsString(node.get("uuidString").asText())
        .setIpAddress(node.get("ipAddress").asText())
        .setHostName(node.get("hostName").asText())
        .setPorts(ports)
        .build();
    return datanodeDetails;
  }
}


