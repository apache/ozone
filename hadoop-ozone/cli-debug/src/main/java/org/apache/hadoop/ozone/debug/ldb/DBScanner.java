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

import static java.nio.charset.StandardCharsets.UTF_8;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.hdds.cli.AbstractSubcommand;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.DBColumnFamilyDefinition;
import org.apache.hadoop.hdds.utils.db.DBDefinition;
import org.apache.hadoop.hdds.utils.db.FixedLengthStringCodec;
import org.apache.hadoop.hdds.utils.db.LongCodec;
import org.apache.hadoop.hdds.utils.db.RocksDatabase;
import org.apache.hadoop.hdds.utils.db.managed.ManagedReadOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksIterator;
import org.apache.hadoop.hdds.utils.db.managed.ManagedSlice;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.metadata.DatanodeSchemaThreeDBDefinition;
import org.apache.hadoop.ozone.debug.DBDefinitionFactory;
import org.apache.hadoop.ozone.debug.RocksDBUtils;
import org.apache.hadoop.ozone.utils.Filter;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

/**
 * Parser for scm.db, om.db or container db file.
 */
@CommandLine.Command(
    name = "scan",
    description = "Parse specified metadataTable"
)
public class DBScanner extends AbstractSubcommand implements Callable<Void> {

  private static final Logger LOG = LoggerFactory.getLogger(DBScanner.class);
  private static final String SCHEMA_V3 = "V3";

  @CommandLine.ParentCommand
  private RDBParser parent;

  @CommandLine.Option(names = {"--column_family", "--column-family", "--cf"},
      required = true,
      description = "Table name")
  private String tableName;

  @CommandLine.Option(names = {"--with-keys"},
      description = "Print a JSON object of key->value pairs (default)"
          + " instead of a JSON array of only values.",
      defaultValue = "true", fallbackValue = "true")
  private boolean withKey;

  @CommandLine.Option(names = {"--length", "--limit", "-l"},
      description = "Maximum number of items to list.",
      defaultValue = "-1")
  private long limit;

  @CommandLine.Option(names = {"--out", "-o"},
      description = "File to dump table scan data")
  private String fileName;

  @CommandLine.Option(names = {"--startkey", "--sk", "-s"},
      description = "Key from which to iterate the DB")
  private String startKey;

  @CommandLine.Option(names = {"--endkey", "--ek", "-e"},
      description = "Key at which iteration of the DB ends")
  private String endKey;

  @CommandLine.Option(names = {"--fields"},
      description = "Comma-separated list of fields needed for each value. " +
          "eg.) \"name,acls.type\" for showing name and type under acls.")
  private String fieldsFilter;

  @CommandLine.Option(names = {"--filter"},
      description = "Comma-separated list of \"<field>:<operator>:<value>\" where " +
          "<field> is any valid field of the record, " +
          "<operator> is [EQUALS,LESSER, GREATER or REGEX]. (EQUALS compares the exact string, " +
          "REGEX compares with a valid regular expression passed, and LESSER/GREATER works with numeric values), " +
          "<value> is the value of the field. \n" +
          "eg.) \"dataSize:equals:1000\" for showing records having the value 1000 for dataSize, \n" +
          "     \"keyName:regex:^key.*$\" for showing records having keyName that matches the given regex.")
  private String filter;

  @CommandLine.Option(names = {"--dnSchema", "--dn-schema", "-d"},
      description = "Datanode DB Schema Version: V1/V2/V3",
      defaultValue = "V3")
  private String dnDBSchemaVersion;

  @CommandLine.Option(names = {"--container-id", "--cid"},
      description = "Container ID. Applicable if datanode DB Schema is V3",
      defaultValue = "-1")
  private long containerId;

  @CommandLine.Option(names = { "--show-count", "--count" },
      description = "Get estimated key count for the given DB column family",
      defaultValue = "false",
      showDefaultValue = CommandLine.Help.Visibility.ALWAYS)
  private boolean showCount;

  @CommandLine.Option(names = {"--compact"},
      description = "disable the pretty print the output",
      defaultValue = "false")
  private static boolean compact;

  @CommandLine.Option(names = {"--batch-size"},
      description = "Batch size for processing DB data.",
      defaultValue = "10000")
  private int batchSize;

  @CommandLine.Option(names = {"--thread-count"},
      description = "Thread count for concurrent processing.",
      defaultValue = "10")
  private int threadCount;

  @CommandLine.Option(names = {"--max-records-per-file"},
      description = "The number of records to print per file.",
      defaultValue = "0")
  private long recordsPerFile;

  private int fileSuffix = 0;
  private long globalCount = 0;

  private static final String KEY_SEPARATOR_SCHEMA_V3 =
      new OzoneConfiguration().getObject(DatanodeConfiguration.class)
          .getContainerSchemaV3KeySeparator();
  private static volatile boolean exception;
  private static final long FIRST_SEQUENCE_ID = 0L;

  @Override
  public Void call() throws Exception {
    fileSuffix = 0;
    globalCount = 0;
    List<ColumnFamilyDescriptor> cfDescList =
        RocksDBUtils.getColumnFamilyDescriptors(parent.getDbPath());
    final List<ColumnFamilyHandle> cfHandleList = new ArrayList<>();

    final boolean schemaV3 = dnDBSchemaVersion != null &&
        dnDBSchemaVersion.equalsIgnoreCase(SCHEMA_V3) &&
        parent.getDbPath().contains(OzoneConsts.CONTAINER_DB_NAME);

    boolean success;
    try (ManagedRocksDB db = ManagedRocksDB.openReadOnly(
        parent.getDbPath(), cfDescList, cfHandleList)) {
      success = printTable(cfHandleList, db, parent.getDbPath(), schemaV3);
    }

    if (!success) {
      // Trick to set exit code to 1 on error.
      // TODO: Properly set exit code hopefully by refactoring GenericCli
      throw new Exception(
          "Exit code is non-zero. Check the error message above");
    }

    return null;
  }

  public byte[] getValueObject(DBColumnFamilyDefinition dbColumnFamilyDefinition, String key) {
    Class<?> keyType = dbColumnFamilyDefinition.getKeyType();
    if (keyType.equals(String.class)) {
      return key.getBytes(UTF_8);
    } else if (keyType.equals(ContainerID.class)) {
      return ContainerID.getBytes(Long.parseLong(key));
    } else if (keyType.equals(Long.class)) {
      return LongCodec.get().toPersistedFormat(Long.parseLong(key));
    } else if (keyType.equals(PipelineID.class)) {
      return PipelineID.valueOf(UUID.fromString(key)).getProtobuf()
          .toByteArray();
    } else {
      throw new IllegalArgumentException(
          "StartKey and EndKey is not supported for this table.");
    }
  }

  private boolean displayTable(ManagedRocksIterator iterator,
                               DBColumnFamilyDefinition dbColumnFamilyDef,
                               boolean schemaV3)
      throws IOException {

    if (fileName == null) {
      // Print to stdout
      return displayTable(iterator, dbColumnFamilyDef, out(), schemaV3);
    }

    // If there are no parent directories, create them
    File file = new File(fileName);
    File parentFile = file.getParentFile();
    if (!parentFile.exists()) {
      boolean flg = parentFile.mkdirs();
      if (!flg) {
        throw new IOException("An exception occurred while creating " +
            "the directory. Directory: " + parentFile.getAbsolutePath());
      }
    }

    // Write to file output
    while (iterator.get().isValid() && withinLimit(globalCount)) {
      String fileNameTarget = recordsPerFile > 0 ? fileName + "." + fileSuffix++ :
          fileName;
      try (PrintWriter out = new PrintWriter(new BufferedWriter(
          new PrintWriter(fileNameTarget, UTF_8.name())))) {
        if (!displayTable(iterator, dbColumnFamilyDef, out, schemaV3)) {
          return false;
        }
      }
    }
    return true;
  }

  private boolean displayTable(ManagedRocksIterator iterator,
                               DBColumnFamilyDefinition dbColumnFamilyDef,
                               PrintWriter printWriter, boolean schemaV3) {
    exception = false;
    ThreadFactory factory = new ThreadFactoryBuilder()
        .setNameFormat("DBScanner-%d")
        .build();
    ExecutorService threadPool = new ThreadPoolExecutor(
        threadCount, threadCount, 60, TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(1024), factory,
        new ThreadPoolExecutor.CallerRunsPolicy());
    LogWriter logWriter = new LogWriter(printWriter);
    try {
      // Start JSON object (map) or array
      printWriter.print(withKey ? "{ " : "[ ");
      logWriter.start();
      processRecords(iterator, dbColumnFamilyDef, logWriter,
          threadPool, schemaV3);
    } catch (InterruptedException | IOException e) {
      exception = true;
      Thread.currentThread().interrupt();
    } finally {
      threadPool.shutdownNow();
      logWriter.stop();
      logWriter.join();
      // End JSON object (map) or array
      printWriter.println(withKey ? " }" : " ]");
    }
    return !exception;
  }

  private void processRecords(ManagedRocksIterator iterator,
                              DBColumnFamilyDefinition dbColumnFamilyDef,
                              LogWriter logWriter, ExecutorService threadPool,
                              boolean schemaV3) throws InterruptedException, IOException {
    if (startKey != null) {
      iterator.get().seek(getValueObject(dbColumnFamilyDef, startKey));
    }
    ArrayList<ByteArrayKeyValue> batch = new ArrayList<>(batchSize);
    // Used to ensure that the output of a multi-threaded parsed Json is in
    // the same order as the RocksDB iterator.
    long sequenceId = FIRST_SEQUENCE_ID;
    // Count number of keys printed so far
    long count = 0;
    List<Future<Void>> futures = new ArrayList<>();
    boolean reachedEnd = false;

    Map<String, Filter> fieldsFilterSplitMap = new HashMap<>();
    if (filter != null) {
      for (String field : filter.split(",")) {
        String[] fieldValue = field.split(":");
        if (fieldValue.length != 3) {
          err().println("Error: Invalid format for filter \"" + field
              + "\". Usage: <field>:<operator>:<value>. Ignoring filter passed");
        } else {
          Filter filterValue = new Filter(fieldValue[1], fieldValue[2]);
          if (filterValue.getOperator() == null) {
            err().println("Error: Invalid operator for filter \"" + filterValue
                + "\". <operator> can be one of [EQUALS,LESSER,GREATER]. Ignoring filter passed");
          } else {
            String[] subfields = fieldValue[0].split("\\.");
            getFilterSplit(Arrays.asList(subfields), fieldsFilterSplitMap, filterValue);
          }
        }
      }
    }

    while (withinLimit(globalCount) && iterator.get().isValid() && !exception && !reachedEnd) {
      // if invalid endKey is given, it is ignored
      if (null != endKey && Arrays.equals(iterator.get().key(), getValueObject(dbColumnFamilyDef, endKey))) {
        reachedEnd = true;
      }

      Object o = dbColumnFamilyDef.getValueCodec().fromPersistedFormat(iterator.get().value());
      if (filter == null ||
          checkFilteredObject(o, dbColumnFamilyDef.getValueType(), fieldsFilterSplitMap)) {
        // the record passes the filter
        batch.add(new ByteArrayKeyValue(
            iterator.get().key(), iterator.get().value()));
        globalCount++;
        count++;
        if (batch.size() >= batchSize) {
          while (logWriter.getInflightLogCount() > threadCount * 10L
              && !exception) {
            // Prevents too many unfinished Tasks from
            // consuming too much memory.
            Thread.sleep(100);
          }
          Future<Void> future = threadPool.submit(
              new Task(dbColumnFamilyDef, batch, logWriter, sequenceId,
                  withKey, schemaV3, fieldsFilter));
          futures.add(future);
          batch = new ArrayList<>(batchSize);
          sequenceId++;
        }
      }
      iterator.get().next();
      if ((recordsPerFile > 0) && (count >= recordsPerFile)) {
        break;
      }
    }
    if (!batch.isEmpty()) {
      Future<Void> future = threadPool.submit(new Task(dbColumnFamilyDef,
          batch, logWriter, sequenceId, withKey, schemaV3, fieldsFilter));
      futures.add(future);
    }

    for (Future<Void> future : futures) {
      try {
        future.get();
      } catch (ExecutionException e) {
        LOG.error("Task execution failed", e);
      }
    }
  }

  private void getFilterSplit(List<String> fields, Map<String, Filter> fieldMap, Filter leafValue) throws IOException {
    int len = fields.size();
    if (len == 1) {
      Filter currentValue = fieldMap.get(fields.get(0));
      if (currentValue != null) {
        err().println("Cannot pass multiple values for the same field and " +
            "cannot have filter for both parent and child");
        throw new IOException("Invalid filter passed");
      }
      fieldMap.put(fields.get(0), leafValue);
    } else {
      Filter fieldMapGet = fieldMap.computeIfAbsent(fields.get(0), k -> new Filter());
      if (fieldMapGet.getValue() != null) {
        err().println("Cannot pass multiple values for the same field and " +
            "cannot have filter for both parent and child");
        throw new IOException("Invalid filter passed");
      }
      Map<String, Filter> nextLevel = fieldMapGet.getNextLevel();
      if (nextLevel == null) {
        fieldMapGet.setNextLevel(new HashMap<>());
      }
      getFilterSplit(fields.subList(1, len), fieldMapGet.getNextLevel(), leafValue);
    }
  }

  private boolean checkFilteredObject(Object obj, Class<?> clazz, Map<String, Filter> fieldsSplitMap) {
    for (Map.Entry<String, Filter> field : fieldsSplitMap.entrySet()) {
      try {
        Field valueClassField = getRequiredFieldFromAllFields(clazz, field.getKey());
        Object valueObject = valueClassField.get(obj);
        Filter fieldValue = field.getValue();

        if (valueObject == null) {
          // there is no such field in the record. This filter will be ignored for the current record.
          continue;
        }
        if (fieldValue == null) {
          err().println("Malformed filter. Check input");
          throw new IOException("Invalid filter passed");
        } else if (fieldValue.getNextLevel() == null) {
          // reached the end of fields hierarchy, check if they match the filter
          try {
            switch (fieldValue.getOperator()) {
            case EQUALS:
              if (!String.valueOf(valueObject).equals(fieldValue.getValue())) {
                return false;
              }
              break;
            case GREATER:
              if (Double.parseDouble(String.valueOf(valueObject))
                  < Double.parseDouble(String.valueOf(fieldValue.getValue()))) {
                return false;
              }
              break;
            case LESSER:
              if (Double.parseDouble(String.valueOf(valueObject))
                  > Double.parseDouble(String.valueOf(fieldValue.getValue()))) {
                return false;
              }
              break;
            case REGEX:
              Pattern p = Pattern.compile(String.valueOf(fieldValue.getValue()));
              Matcher m = p.matcher(String.valueOf(valueObject));
              if (!m.find()) {
                return false;
              }
              break;
            default:
              err().println("Only EQUALS/LESSER/GREATER/REGEX operator is supported currently.");
              throw new IOException("Invalid filter passed");
            }
          } catch (NumberFormatException ex) {
            err().println("LESSER or GREATER operation can be performed only on numeric values.");
            throw new IOException("Invalid filter passed");
          }
        } else {
          Map<String, Filter> subfields = fieldValue.getNextLevel();
          if (Collection.class.isAssignableFrom(valueObject.getClass())) {
            if (!checkFilteredObjectCollection((Collection) valueObject, subfields)) {
              return false;
            }
          } else if (Map.class.isAssignableFrom(valueObject.getClass())) {
            Map<?, ?> valueObjectMap = (Map<?, ?>) valueObject;
            boolean flag = false;
            for (Map.Entry<?, ?> ob : valueObjectMap.entrySet()) {
              boolean subflag;
              if (Collection.class.isAssignableFrom(ob.getValue().getClass())) {
                subflag = checkFilteredObjectCollection((Collection)ob.getValue(), subfields);
              } else {
                subflag = checkFilteredObject(ob.getValue(), ob.getValue().getClass(), subfields);
              }
              if (subflag) {
                // atleast one item in the map/list of the record has matched the filter,
                // so record passes the filter.
                flag = true;
                break;
              }
            }
            if (!flag) {
              // none of the items in the map/list passed the filter => record doesn't pass the filter
              return false;
            }
          } else {
            if (!checkFilteredObject(valueObject, valueClassField.getType(), subfields)) {
              return false;
            }
          }
        }
      } catch (NoSuchFieldException ex) {
        err().println("ERROR: no such field: " + field);
        exception = true;
        return false;
      } catch (IllegalAccessException e) {
        err().println("ERROR: Cannot get field \"" + field + "\" from record.");
        exception = true;
        return false;
      } catch (Exception ex) {
        err().println("ERROR: field: " + field + ", ex: " + ex);
        exception = true;
        return false;
      }
    }
    return true;
  }

  private boolean checkFilteredObjectCollection(Collection<?> valueObject, Map<String, Filter> fields)
      throws NoSuchFieldException, IllegalAccessException, IOException {
    for (Object ob : valueObject) {
      if (checkFilteredObject(ob, ob.getClass(), fields)) {
        return true;
      }
    }
    return false;
  }

  Field getRequiredFieldFromAllFields(Class clazz, String fieldName) throws NoSuchFieldException {
    List<Field> classFieldList = ValueSchema.getAllFields(clazz);
    Field classField = null;
    for (Field f : classFieldList) {
      if (f.getName().equals(fieldName)) {
        classField = f;
        break;
      }
    }
    if (classField == null) {
      err().println("Error: Invalid field \"" + fieldName + "\" passed for filter");
      throw new NoSuchFieldException();
    }
    classField.setAccessible(true);
    return classField;
  }

  private boolean withinLimit(long i) {
    return limit == -1L || i < limit;
  }

  private ColumnFamilyHandle getColumnFamilyHandle(
      byte[] name, List<ColumnFamilyHandle> columnFamilyHandles) {
    return columnFamilyHandles
        .stream()
        .filter(
            handle -> {
              try {
                return Arrays.equals(handle.getName(), name);
              } catch (Exception ex) {
                throw new RuntimeException(ex);
              }
            })
        .findAny()
        .orElse(null);
  }

  /**
   * Main table printing logic.
   * User-provided args are not in the arg list. Those are instance variables
   * parsed by picocli.
   */
  private boolean printTable(List<ColumnFamilyHandle> columnFamilyHandleList,
                             ManagedRocksDB rocksDB,
                             String dbPath,
                             boolean schemaV3)
      throws IOException, RocksDBException {

    if (limit < 1 && limit != -1) {
      throw new IllegalArgumentException(
          "List length should be a positive number. Only allowed negative" +
              " number is -1 which is to dump entire table");
    }
    dbPath = removeTrailingSlashIfNeeded(dbPath);
    DBDefinitionFactory.setDnDBSchemaVersion(dnDBSchemaVersion);
    DBDefinition dbDefinition = DBDefinitionFactory.getDefinition(
        Paths.get(dbPath), new OzoneConfiguration());
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
    ColumnFamilyHandle columnFamilyHandle = getColumnFamilyHandle(
        columnFamilyDefinition.getName().getBytes(UTF_8),
        columnFamilyHandleList);
    if (columnFamilyHandle == null) {
      throw new IllegalStateException("columnFamilyHandle is null");
    }

    if (showCount) {
      // Only prints estimates key count
      long keyCount = rocksDB.get()
          .getLongProperty(columnFamilyHandle, RocksDatabase.ESTIMATE_NUM_KEYS);
      out().println(keyCount);
      return true;
    }

    ManagedRocksIterator iterator = null;
    ManagedReadOptions readOptions = null;
    ManagedSlice slice = null;
    try {
      if (containerId > 0L && schemaV3) {
        // Handle SchemaV3 DN DB
        readOptions = new ManagedReadOptions();
        slice = new ManagedSlice(
            DatanodeSchemaThreeDBDefinition.getContainerKeyPrefixBytes(
                containerId + 1L));
        readOptions.setIterateUpperBound(slice);
        iterator = new ManagedRocksIterator(
            rocksDB.get().newIterator(columnFamilyHandle, readOptions));
        iterator.get().seek(
            DatanodeSchemaThreeDBDefinition.getContainerKeyPrefixBytes(
                containerId));
      } else {
        iterator = new ManagedRocksIterator(
            rocksDB.get().newIterator(columnFamilyHandle));
        iterator.get().seekToFirst();
      }

      return displayTable(iterator, columnFamilyDefinition, schemaV3);
    } finally {
      IOUtils.closeQuietly(iterator, readOptions, slice);
    }
  }

  private String removeTrailingSlashIfNeeded(String dbPath) {
    if (dbPath.endsWith(OzoneConsts.OZONE_URI_DELIMITER)) {
      dbPath = dbPath.substring(0, dbPath.length() - 1);
    }
    return dbPath;
  }

  /**
   * Utility for centralized JSON serialization using Jackson.
   */
  @VisibleForTesting
  public static class JsonSerializationHelper {
    /**
     * In order to maintain consistency with the original Gson output to do
     * this setup makes the output from Jackson closely match the
     * output of Gson.
     */
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY)
        // Ignore standard getters.
        .setVisibility(PropertyAccessor.GETTER, JsonAutoDetect.Visibility.NONE)
        // Ignore boolean "is" getters.
        .setVisibility(PropertyAccessor.IS_GETTER,
            JsonAutoDetect.Visibility.NONE)
        // Exclude null values.
        .setSerializationInclusion(JsonInclude.Include.NON_NULL)
        .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
    public static final ObjectWriter WRITER;

    static {
      if (compact) {
        WRITER = OBJECT_MAPPER.writer();
      } else {
        WRITER = OBJECT_MAPPER.writerWithDefaultPrettyPrinter();
      }
    }

    public static ObjectWriter getWriter() {
      return WRITER;
    }
  }

  private class Task implements Callable<Void> {

    private final DBColumnFamilyDefinition dbColumnFamilyDefinition;
    private final ArrayList<ByteArrayKeyValue> batch;
    private final LogWriter logWriter;
    private final ObjectWriter writer =
        JsonSerializationHelper.getWriter();
    private final long sequenceId;
    private final boolean withKey;
    private final boolean schemaV3;
    private String valueFields;

    @SuppressWarnings("checkstyle:parameternumber")
    Task(DBColumnFamilyDefinition dbColumnFamilyDefinition,
         ArrayList<ByteArrayKeyValue> batch, LogWriter logWriter,
         long sequenceId, boolean withKey, boolean schemaV3, String valueFields) {
      this.dbColumnFamilyDefinition = dbColumnFamilyDefinition;
      this.batch = batch;
      this.logWriter = logWriter;
      this.sequenceId = sequenceId;
      this.withKey = withKey;
      this.schemaV3 = schemaV3;
      this.valueFields = valueFields;
    }

    Map<String, Object> getFieldSplit(List<String> fields, Map<String, Object> fieldMap) {
      int len = fields.size();
      if (fieldMap == null) {
        fieldMap = new HashMap<>();
      }
      if (len == 1) {
        fieldMap.putIfAbsent(fields.get(0), null);
      } else {
        Map<String, Object> fieldMapGet = (Map<String, Object>) fieldMap.get(fields.get(0));
        if (fieldMapGet == null) {
          fieldMap.put(fields.get(0), getFieldSplit(fields.subList(1, len), null));
        } else {
          fieldMap.put(fields.get(0), getFieldSplit(fields.subList(1, len), fieldMapGet));
        }
      }
      return fieldMap;
    }

    @Override
    public Void call() {
      try {
        ArrayList<String> results = new ArrayList<>(batch.size());
        Map<String, Object> fieldsSplitMap = new HashMap<>();

        if (valueFields != null) {
          for (String field : valueFields.split(",")) {
            String[] subfields = field.split("\\.");
            fieldsSplitMap = getFieldSplit(Arrays.asList(subfields), fieldsSplitMap);
          }
        }

        for (ByteArrayKeyValue byteArrayKeyValue : batch) {
          StringBuilder sb = new StringBuilder();
          if (!(sequenceId == FIRST_SEQUENCE_ID && results.isEmpty())) {
            // Add a comma before each output entry, starting from the second
            // one, to ensure valid JSON format.
            sb.append(", ");
          }
          if (withKey) {
            Object key = dbColumnFamilyDefinition.getKeyCodec()
                .fromPersistedFormat(byteArrayKeyValue.getKey());
            if (schemaV3) {
              int index =
                  DatanodeSchemaThreeDBDefinition.getContainerKeyPrefixLength();
              String keyStr = key.toString();
              if (index > keyStr.length()) {
                err().println("Error: Invalid SchemaV3 table key length. "
                    + "Is this a V2 table? Try again with --dn-schema=V2");
                exception = true;
                break;
              }
              String cid = key.toString().substring(0, index);
              String blockId = key.toString().substring(index);
              sb.append(writer.writeValueAsString(LongCodec.get()
                  .fromPersistedFormat(
                      FixedLengthStringCodec.string2Bytes(cid)) +
                  KEY_SEPARATOR_SCHEMA_V3 + blockId));
            } else {
              sb.append(writer.writeValueAsString(key));
            }
            sb.append(": ");
          }

          Object o = dbColumnFamilyDefinition.getValueCodec()
              .fromPersistedFormat(byteArrayKeyValue.getValue());

          if (valueFields != null) {
            Map<String, Object> filteredValue = new HashMap<>();
            filteredValue.putAll(getFieldsFilteredObject(o, dbColumnFamilyDefinition.getValueType(), fieldsSplitMap));
            sb.append(writer.writeValueAsString(filteredValue));
          } else {
            sb.append(writer.writeValueAsString(o));
          }

          results.add(sb.toString());
        }
        logWriter.log(results, sequenceId);
      } catch (IOException e) {
        exception = true;
        LOG.error("Exception parse Object", e);
      }
      return null;
    }

    Map<String, Object> getFieldsFilteredObject(Object obj, Class<?> clazz, Map<String, Object> fieldsSplitMap) {
      Map<String, Object> valueMap = new HashMap<>();
      for (Map.Entry<String, Object> field : fieldsSplitMap.entrySet()) {
        try {
          Field valueClassField = getRequiredFieldFromAllFields(clazz, field.getKey());
          Object valueObject = valueClassField.get(obj);
          Map<String, Object> subfields = (Map<String, Object>) field.getValue();

          if (subfields == null) {
            valueMap.put(field.getKey(), valueObject);
          } else {
            if (Collection.class.isAssignableFrom(valueObject.getClass())) {
              List<Object> subfieldObjectsList =
                  getFieldsFilteredObjectCollection((Collection) valueObject, subfields);
              valueMap.put(field.getKey(), subfieldObjectsList);
            } else if (Map.class.isAssignableFrom(valueObject.getClass())) {
              Map<Object, Object> subfieldObjectsMap = new HashMap<>();
              Map<?, ?> valueObjectMap = (Map<?, ?>) valueObject;
              for (Map.Entry<?, ?> ob : valueObjectMap.entrySet()) {
                Object subfieldValue;
                if (Collection.class.isAssignableFrom(ob.getValue().getClass())) {
                  subfieldValue = getFieldsFilteredObjectCollection((Collection)ob.getValue(), subfields);
                } else {
                  subfieldValue = getFieldsFilteredObject(ob.getValue(), ob.getValue().getClass(), subfields);
                }
                subfieldObjectsMap.put(ob.getKey(), subfieldValue);
              }
              valueMap.put(field.getKey(), subfieldObjectsMap);
            } else {
              valueMap.put(field.getKey(),
                  getFieldsFilteredObject(valueObject, valueClassField.getType(), subfields));
            }
          }
        } catch (NoSuchFieldException ex) {
          err().println("ERROR: no such field: " + field);
        } catch (IllegalAccessException e) {
          err().println("ERROR: Cannot get field from object: " + field);
        }
      }
      return valueMap;
    }

    List<Object> getFieldsFilteredObjectCollection(Collection<?> valueObject, Map<String, Object> fields)
        throws NoSuchFieldException, IllegalAccessException {
      List<Object> subfieldObjectsList = new ArrayList<>();
      for (Object ob : valueObject) {
        Object subfieldValue = getFieldsFilteredObject(ob, ob.getClass(), fields);
        subfieldObjectsList.add(subfieldValue);
      }
      return subfieldObjectsList;
    }
  }

  private static class ByteArrayKeyValue {
    private final byte[] key;
    private final byte[] value;

    ByteArrayKeyValue(byte[] key, byte[] value) {
      this.key = key;
      this.value = value;
    }

    public byte[] getKey() {
      return key;
    }

    public byte[] getValue() {
      return value;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ByteArrayKeyValue that = (ByteArrayKeyValue) o;
      return Arrays.equals(key, that.key);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(key);
    }

    @Override
    public String toString() {
      return "ByteArrayKeyValue{" +
          "key=" + Arrays.toString(key) +
          ", value=" + Arrays.toString(value) +
          '}';
    }
  }

  private static class LogWriter {
    private final Map<Long, ArrayList<String>> logs;
    private final PrintWriter printWriter;
    private final Thread writerThread;
    private volatile boolean stop = false;
    private long expectedSequenceId = FIRST_SEQUENCE_ID;
    private final Object lock = new Object();
    private final AtomicLong inflightLogCount = new AtomicLong();

    LogWriter(PrintWriter printWriter) {
      this.logs = new HashMap<>();
      this.printWriter = printWriter;
      this.writerThread = new Thread(new WriterTask());
    }

    void start() {
      writerThread.start();
    }

    public void log(ArrayList<String> msg, long sequenceId) {
      synchronized (lock) {
        if (!stop) {
          logs.put(sequenceId, msg);
          inflightLogCount.incrementAndGet();
          lock.notify();
        }
      }
    }

    private final class WriterTask implements Runnable {
      @Override
      public void run() {
        try {
          while (!stop) {
            synchronized (lock) {
              // The sequenceId is incrementally generated as the RocksDB
              // iterator. Thus, based on the sequenceId, we can strictly ensure
              // that the output order here is consistent with the order of the
              // RocksDB iterator.
              // Note that the order here not only requires the sequenceId to be
              // incremental, but also demands that the sequenceId of the
              // next output is the current sequenceId + 1.
              ArrayList<String> results = logs.get(expectedSequenceId);
              if (results != null) {
                for (String result : results) {
                  printWriter.println(result);
                }
                inflightLogCount.decrementAndGet();
                logs.remove(expectedSequenceId);
                // sequenceId of the next output must be the current
                // sequenceId + 1
                expectedSequenceId++;
              } else {
                lock.wait(1000);
              }
            }
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } catch (Exception e) {
          LOG.error("Exception while output", e);
        } finally {
          stop = true;
          synchronized (lock) {
            drainRemainingMessages();
          }
        }
      }
    }

    private void drainRemainingMessages() {
      ArrayList<String> results;
      while ((results = logs.get(expectedSequenceId)) != null) {
        for (String result : results) {
          printWriter.println(result);
        }
        expectedSequenceId++;
      }
    }

    public void stop() {
      if (!stop) {
        stop = true;
        writerThread.interrupt();
      }
    }

    public void join() {
      try {
        writerThread.join();
      } catch (InterruptedException e) {
        LOG.error("InterruptedException while output", e);
        Thread.currentThread().interrupt();
      }
    }

    public long getInflightLogCount() {
      return inflightLogCount.get();
    }
  }
}
