/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license
 * agreements. See the NOTICE file distributed with this work for additional
 * information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache
 * License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the
 * License. You may obtain a
 * copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software
 * distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.helpers;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Used by Jackson to deserialize Immutable lists.
 */
public class ImmutableListDeserializer extends JsonDeserializer<ImmutableList<?>> {

  private final Class<?> elementClass;

  public ImmutableListDeserializer(Class<?> elementClass) {
    this.elementClass = elementClass;
  }

  @Override
  public ImmutableList<?> deserialize(JsonParser p, DeserializationContext ctxt)
      throws IOException, JsonProcessingException {
    JsonNode node = p.getCodec().readTree(p);
    List<Object> list = new ArrayList<>();
    for (JsonNode elementNode : node) {
      Object element = p.getCodec().treeToValue(elementNode, elementClass);
      list.add(element);
    }
    return ImmutableList.copyOf(list);
  }
}
