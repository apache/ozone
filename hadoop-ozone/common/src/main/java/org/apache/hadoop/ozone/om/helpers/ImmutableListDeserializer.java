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
