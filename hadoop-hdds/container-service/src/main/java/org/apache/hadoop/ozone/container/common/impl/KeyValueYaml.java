/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.container.common.impl;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.yaml.snakeyaml.Yaml;


import java.beans.IntrospectionException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;

import java.io.File;


import java.util.Set;
import java.util.TreeSet;
import java.util.Map;

import org.yaml.snakeyaml.constructor.AbstractConstruct;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.introspector.BeanAccess;
import org.yaml.snakeyaml.introspector.Property;
import org.yaml.snakeyaml.introspector.PropertyUtils;
import org.yaml.snakeyaml.nodes.MappingNode;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.ScalarNode;
import org.yaml.snakeyaml.nodes.Tag;
import org.yaml.snakeyaml.representer.Representer;

/**
 * Class for creating and reading .container files.
 */

public final class KeyValueYaml {

  private KeyValueYaml() {

  }
  /**
   * Creates a .container file in yaml format.
   *
   * @param containerFile
   * @param containerData
   * @throws IOException
   */
  public static void createContainerFile(File containerFile, ContainerData
      containerData) throws IOException {

    Preconditions.checkNotNull(containerFile, "yamlFile cannot be null");
    Preconditions.checkNotNull(containerData, "containerData cannot be null");

    PropertyUtils propertyUtils = new PropertyUtils();
    propertyUtils.setBeanAccess(BeanAccess.FIELD);
    propertyUtils.setAllowReadOnlyProperties(true);

    Representer representer = new KeyValueContainerDataRepresenter();
    representer.setPropertyUtils(propertyUtils);
    representer.addClassTag(org.apache.hadoop.ozone.container.common.impl
        .KeyValueContainerData.class, new Tag("KeyValueContainerData"));

    Constructor keyValueDataConstructor = new KeyValueDataConstructor();

    Yaml yaml = new Yaml(keyValueDataConstructor, representer);

    Writer writer = new OutputStreamWriter(new FileOutputStream(containerFile),
        "UTF-8");
    yaml.dump(containerData, writer);
    writer.close();
  }

  /**
   * Read the yaml file, and return containerData.
   *
   * @param containerFile
   * @throws IOException
   */
  public static KeyValueContainerData readContainerFile(File containerFile)
      throws IOException {
    Preconditions.checkNotNull(containerFile, "containerFile cannot be null");

    InputStream input = null;
    KeyValueContainerData keyValueContainerData;
    try {
      PropertyUtils propertyUtils = new PropertyUtils();
      propertyUtils.setBeanAccess(BeanAccess.FIELD);
      propertyUtils.setAllowReadOnlyProperties(true);

      Representer representer = new KeyValueContainerDataRepresenter();
      representer.setPropertyUtils(propertyUtils);
      representer.addClassTag(org.apache.hadoop.ozone.container.common.impl
          .KeyValueContainerData.class, new Tag("KeyValueContainerData"));

      Constructor keyValueDataConstructor = new KeyValueDataConstructor();

      Yaml yaml = new Yaml(keyValueDataConstructor, representer);
      yaml.setBeanAccess(BeanAccess.FIELD);

      input = new FileInputStream(containerFile);
      keyValueContainerData = (KeyValueContainerData)
          yaml.load(input);
    } finally {
      if (input!= null) {
        input.close();
      }
    }
    return keyValueContainerData;
  }

  /**
   * Representer class to define which fields need to be stored in yaml file.
   */
  private static class KeyValueContainerDataRepresenter extends Representer {
    @Override
    protected Set<Property> getProperties(Class<? extends Object> type)
        throws IntrospectionException {
      Set<Property> set = super.getProperties(type);
      Set<Property> filtered = new TreeSet<Property>();
      if (type.equals(KeyValueContainerData.class)) {
        // filter properties
        for (Property prop : set) {
          String name = prop.getName();
          // When a new field needs to be added, it needs to be added here.
          if (name.equals("containerType") || name.equals("containerId") ||
              name.equals("layOutVersion") || name.equals("state") ||
              name.equals("metadata") || name.equals("dbPath") ||
              name.equals("containerFilePath") || name.equals(
                  "containerDBType")) {
            filtered.add(prop);
          }
        }
      }
      return filtered;
    }
  }

  /**
   * Constructor class for KeyValueData, which will be used by Yaml.
   */
  private static class KeyValueDataConstructor extends Constructor {
    KeyValueDataConstructor() {
      //Adding our own specific constructors for tags.
      this.yamlConstructors.put(new Tag("KeyValueContainerData"),
          new ConstructKeyValueContainerData());
      this.yamlConstructors.put(Tag.INT, new ConstructLong());
    }

    private class ConstructKeyValueContainerData extends AbstractConstruct {
      public Object construct(Node node) {
        MappingNode mnode = (MappingNode) node;
        Map<Object, Object> nodes = constructMapping(mnode);
        String type = (String) nodes.get("containerType");

        ContainerProtos.ContainerType containerType = ContainerProtos
            .ContainerType.KeyValueContainer;
        if (type.equals("KeyValueContainer")) {
          containerType = ContainerProtos.ContainerType.KeyValueContainer;
        }

        //Needed this, as TAG.INT type is by default converted to Long.
        long layOutVersion = (long) nodes.get("layOutVersion");
        int lv = (int) layOutVersion;

        //When a new field is added, it needs to be added here.
        KeyValueContainerData kvData = new KeyValueContainerData(containerType,
            (long) nodes.get("containerId"), lv);
        kvData.setContainerDBType((String)nodes.get("containerDBType"));
        kvData.setDbPath((String) nodes.get("dbPath"));
        kvData.setContainerFilePath((String) nodes.get("containerFilePath"));
        Map<String, String> meta = (Map) nodes.get("metadata");
        meta.forEach((key, val) -> {
          try {
            kvData.addMetadata(key, val);
          } catch (IOException e) {
            throw new IllegalStateException("Unexpected " +
                "Key Value Pair " + "(" + key + "," + val +")in the metadata " +
                "for containerId " + (long) nodes.get("containerId"));
          }
        });
        String state = (String) nodes.get("state");
        switch (state) {
        case "OPEN":
          kvData.setState(ContainerProtos.ContainerLifeCycleState.OPEN);
          break;
        case "CLOSING":
          kvData.setState(ContainerProtos.ContainerLifeCycleState.CLOSING);
          break;
        case "CLOSED":
          kvData.setState(ContainerProtos.ContainerLifeCycleState.CLOSED);
          break;
        default:
          throw new IllegalStateException("Unexpected " +
              "ContainerLifeCycleState " + state + " for the containerId " +
              (long) nodes.get("containerId"));
        }
        return kvData;
      }
    }

    //Below code is taken from snake yaml, as snakeyaml tries to fit the
    // number if it fits in integer, otherwise returns long. So, slightly
    // modified the code to return long in all cases.
    private class ConstructLong extends AbstractConstruct {
      public Object construct(Node node) {
        String value = constructScalar((ScalarNode) node).toString()
            .replaceAll("_", "");
        int sign = +1;
        char first = value.charAt(0);
        if (first == '-') {
          sign = -1;
          value = value.substring(1);
        } else if (first == '+') {
          value = value.substring(1);
        }
        int base = 10;
        if ("0".equals(value)) {
          return Long.valueOf(0);
        } else if (value.startsWith("0b")) {
          value = value.substring(2);
          base = 2;
        } else if (value.startsWith("0x")) {
          value = value.substring(2);
          base = 16;
        } else if (value.startsWith("0")) {
          value = value.substring(1);
          base = 8;
        } else if (value.indexOf(':') != -1) {
          String[] digits = value.split(":");
          int bes = 1;
          int val = 0;
          for (int i = 0, j = digits.length; i < j; i++) {
            val += (Long.parseLong(digits[(j - i) - 1]) * bes);
            bes *= 60;
          }
          return createNumber(sign, String.valueOf(val), 10);
        } else {
          return createNumber(sign, value, 10);
        }
        return createNumber(sign, value, base);
      }
    }

    private Number createNumber(int sign, String number, int radix) {
      Number result;
      if (sign < 0) {
        number = "-" + number;
      }
      result = Long.valueOf(number, radix);
      return result;
    }
  }

}
