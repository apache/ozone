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
package org.apache.hadoop.ozone.repair;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.cli.SubcommandWithParent;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.DBColumnFamilyDefinition;
import org.apache.hadoop.hdds.utils.db.DBDefinition;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.ozone.debug.RocksDBUtils;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.utils.DBDefinitionFactory;
import org.jooq.meta.derby.sys.Sys;
import org.kohsuke.MetaInfServices;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.RocksDBException;
import picocli.CommandLine;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;


/**
 * Tool to update the highest term-index in transactionInfoTable.
 */
@CommandLine.Command(
    name = "put",
    description = "CLI to put a record to a table.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class
)
@MetaInfServices(SubcommandWithParent.class)
public class RDBPut
    implements Callable<Void>, SubcommandWithParent  {

  @CommandLine.Spec
  private static CommandLine.Model.CommandSpec spec;

  @CommandLine.ParentCommand
  private RDBRepair parent;

  @CommandLine.Option(names = {"--column_family", "--column-family", "--cf"},
      required = true,
      description = "Table name")
  private String tableName;

  @CommandLine.Option(names = {"--key", "--k"},
      description = "Key of the record to be added to the DB")
  private String key;

  @CommandLine.Option(names = {"--value", "--v"},
      description = "Value of the record to be added to the DB")
  private String value;

  @CommandLine.Option(names = {"--dnSchema", "--dn-schema", "-d"},
      description = "Datanode DB Schema Version: V1/V2/V3",
      defaultValue = "V3")
  private String dnDBSchemaVersion;

  @Override
  public Void call() throws Exception {
    List<ColumnFamilyHandle> cfHandleList = new ArrayList<>();
    String dbPath = parent.getDbPath();
    System.out.println("1. enter call: " + dbPath);

    List<ColumnFamilyDescriptor> cfDescList = RocksDBUtils.getColumnFamilyDescriptors(
        dbPath);
    System.out.println("2. desc list:  " + cfDescList);

    String errorMessage = tableName + " is not in a column family in DB for the given path.";

    try (ManagedRocksDB db = ManagedRocksDB.open(dbPath, cfDescList, cfHandleList)) {
      System.out.println("3. in try block");
      ColumnFamilyHandle columnFamilyHandle = RocksDBUtils.getColumnFamilyHandle(tableName, cfHandleList);
      System.out.println("4. columnFamilyHandle:  " + columnFamilyHandle);
      if (columnFamilyHandle == null) {
        throw new IllegalArgumentException(errorMessage);
      }

      DBDefinitionFactory.setDnDBSchemaVersion(dnDBSchemaVersion);
      DBDefinition dbDefinition = DBDefinitionFactory.getDefinition(Paths.get(dbPath), new OzoneConfiguration());
      System.out.println("5. dbDefinition:  " + dbDefinition);
      if (dbDefinition == null) {
        throw new IllegalArgumentException(errorMessage);
      }

      final DBColumnFamilyDefinition<?, ?> columnFamilyDefinition = dbDefinition.getColumnFamily(tableName);
      System.out.println("6. columnFamilyDefinition:  " + columnFamilyDefinition);
      if (columnFamilyDefinition == null) {
        throw new IllegalArgumentException(errorMessage);
      }

      Class<?> c = columnFamilyDefinition.getValueType();
    //Class<?> c = OmVolumeArgs.class;
      System.out.println("7. class of value type:  " + c);

      Object newValue;
      try {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
        objectMapper.enable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

        newValue = objectMapper.readValue(value, c);
        System.out.println(newValue);
      } catch (IOException e) {
        e.printStackTrace();
        throw e;
      }
      Method m = c.getDeclaredMethod("getObjectInfo");
      m.setAccessible(true);
      System.out.println("\n\nnew value class: " + newValue.getClass());
      System.out.println("\n\nnew value: \n" + newValue);
      System.out.println("\n\nnew value: \n" + m.invoke(newValue));

      Object oldValue = RocksDBUtils.getValue(db, columnFamilyHandle, key, columnFamilyDefinition.getValueCodec());
      System.out.println("The original value was \n" + oldValue);

      Class<?> cc = columnFamilyDefinition.getValueCodec().getClass();
      System.out.println(" CC: " + cc);
      Method mm = cc.getDeclaredMethod("toPersistedFormat", Object.class);
      System.out.println(" mm: " + mm.getName() + mm.getReturnType());

      byte[] valueBytes = (byte[]) mm.invoke(columnFamilyDefinition.getValueCodec(), c.cast(newValue));

      System.out.println("Bytes: " + valueBytes);

      db.get().put(columnFamilyHandle, , valueBytes);

    } catch (RocksDBException exception) {
      System.err.println("Failed to update the RocksDB for the given path: " + dbPath);
      System.err.println(exception);
      throw new IOException("Failed to update RocksDB.", exception);
    } finally {
      IOUtils.closeQuietly(cfHandleList);
    }

    return null;
  }


/*
  { \"\/voltest1\": {\r\n  \"metadata\" : { },\r\n  \"objectID\" : -4611686018427000000,\r\n  \"updateID\" : -1,\r\n  \"adminName\" : \"om\",\r\n  \"ownerName\" : \"om\",\r\n  \"volume\" : \"voltest1\",\r\n  \"creationTime\" : 1722579699999,\r\n  \"modificationTime\" : 1722579979329,\r\n  \"quotaInBytes\" : -1,\r\n  \"quotaInNamespace\" : -1,\r\n  \"usedNamespace\" : 0,\r\n  \"acls\" : [ {\r\n    \"type\" : \"USER\",\r\n    \"name\" : \"om\",\r\n    \"aclScope\" : \"ACCESS\"\r\n  }, {\r\n    \"type\" : \"GROUP\",\r\n    \"name\" : \"om\",\r\n    \"aclScope\" : \"ACCESS\"\r\n  } ],\r\n  \"refCount\" : 0\r\n}\r\n }

  { \"\/voltest1\": {  \"metadata\" : { },  \"objectID\" : -4611686018427000000,  \"updateID\" : -1,  \"adminName\" : \"om\",  \"ownerName\" : \"om\",  \"volume\" : \"voltest1\",  \"creationTime\" : 1722579699999,  \"modificationTime\" : 1722579979329,  \"quotaInBytes\" : -1,  \"quotaInNamespace\" : -1,  \"usedNamespace\" : 0,  \"acls\" : [ {    \"type\" : \"USER\",    \"name\" : \"om\",    \"aclScope\" : \"ACCESS\"  }, {    \"type\" : \"GROUP\",    \"name\" : \"om\",    \"aclScope\" : \"ACCESS\"  } ],  \"refCount\" : 0} }


  */

  @Override
  public Class<?> getParentType() {
    return RDBRepair.class;
  }

}
