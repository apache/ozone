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

package org.apache.hadoop.ozone.debug.ldb;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.cli.AbstractSubcommand;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.server.JsonUtils;
import org.apache.hadoop.hdds.utils.db.DBColumnFamilyDefinition;
import org.apache.hadoop.hdds.utils.db.DBDefinition;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.debug.DBDefinitionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

/**
 * Get schema of value for scm.db, om.db or container db file.
 */
@CommandLine.Command(
    name = "value-schema",
    description = "Schema of value in metadataTable"
)
public class ValueSchema extends AbstractSubcommand implements Callable<Void> {

  @CommandLine.ParentCommand
  private RDBParser parent;

  private static final Logger LOG = LoggerFactory.getLogger(ValueSchema.class);

  @CommandLine.Option(names = {"--column_family", "--column-family", "--cf"},
      required = true,
      description = "Table name")
  private String tableName;

  @CommandLine.Option(names = {"--dnSchema", "--dn-schema", "-d"},
      description = "Datanode DB Schema Version: V1/V2/V3",
      defaultValue = "V3")
  private String dnDBSchemaVersion;

  @CommandLine.Option(names = {"--depth"},
      description = "The level till which the value-schema should be shown. Values in the range [0-10] are allowed)",
      defaultValue = "10")
  private int depth;

  @Override
  public Void call() throws Exception {
    if (depth < 0 || depth > 10) {
      throw new IOException("depth should be specified in the range [0, 10]");
    }

    boolean success = true;

    String dbPath = parent.getDbPath();
    Map<String, Object> fields = new HashMap<>();
    success = getValueFields(dbPath, fields);

    out().println(JsonUtils.toJsonStringWithDefaultPrettyPrinter(fields));

    if (!success) {
      // Trick to set exit code to 1 on error.
      // TODO: Properly set exit code hopefully by refactoring GenericCli
      throw new Exception(
          "Exit code is non-zero. Check the error message above");
    }
    return null;
  }

  public boolean getValueFields(String dbPath, Map<String, Object> valueSchema) {

    dbPath = removeTrailingSlashIfNeeded(dbPath);
    DBDefinitionFactory.setDnDBSchemaVersion(dnDBSchemaVersion);
    DBDefinition dbDefinition = DBDefinitionFactory.getDefinition(Paths.get(dbPath), new OzoneConfiguration());
    if (dbDefinition == null) {
      err().println("Error: Incorrect DB Path");
      return false;
    }
    final DBColumnFamilyDefinition<?, ?> columnFamilyDefinition =
        dbDefinition.getColumnFamily(tableName);
    if (columnFamilyDefinition == null) {
      err().print("Error: Table with name '" + tableName + "' not found");
      return false;
    }

    Class<?> c = columnFamilyDefinition.getValueType();
    valueSchema.put(c.getSimpleName(), getFieldsStructure(c, depth));

    return true;
  }

  private static Object getFieldsStructure(Class<?> clazz, int currentDepth) {
    if (clazz.isPrimitive() || String.class.equals(clazz)) {
      return clazz.getSimpleName();
    } else if (currentDepth == 0) {
      return "struct";
    } else {
      Map<String, Object> finalFields = new HashMap<>();
      List<Field> clazzFields = getAllFields(clazz);
      for (Field field : clazzFields) {
        Class<?> fieldClass;
        try {
          if (Collection.class.isAssignableFrom(field.getType())) {
            fieldClass = (Class<?>) ((ParameterizedType) field.getGenericType()).getActualTypeArguments()[0];
          } else {
            fieldClass = field.getType();
          }
        } catch (ClassCastException ex) {
          fieldClass = field.getType();
        }
        finalFields.put(field.getName(), getFieldsStructure(fieldClass, currentDepth - 1));
      }
      return finalFields;
    }
  }

  public static List<Field> getAllFields(Class clazz) {
    // NOTE: Schema of interface type, like ReplicationConfig, cannot be fetched.
    //       An empty list "[]" will be shown for such types of fields.
    if (clazz == null) {
      return Collections.emptyList();
    }
    // get all fields, including inherited ones, of clazz
    List<Field> result = new ArrayList<>(getAllFields(clazz.getSuperclass()));
    List<Field> filteredFields = Arrays.stream(clazz.getDeclaredFields())
        .filter(f -> !Modifier.isStatic(f.getModifiers()))
        .collect(Collectors.toList());
    result.addAll(filteredFields);
    return result;
  }

  private static String removeTrailingSlashIfNeeded(String dbPath) {
    if (dbPath.endsWith(OzoneConsts.OZONE_URI_DELIMITER)) {
      dbPath = dbPath.substring(0, dbPath.length() - 1);
    }
    return dbPath;
  }
}
