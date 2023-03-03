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

package org.apache.hadoop.hdds.utils;

import org.apache.hadoop.hdds.utils.db.RocksDatabase;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import org.bouncycastle.util.Strings;
import org.rocksdb.HistogramData;
import org.rocksdb.HistogramType;
import org.rocksdb.LiveFileMetaData;
import org.rocksdb.Statistics;
import org.rocksdb.TickerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.InvalidAttributeValueException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.ReflectionException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Adapter JMX bean to publish all the Rocksdb metrics.
 */
public class RocksDBStoreMBean implements DynamicMBean, MetricsSource {

  private Statistics statistics;

  private final RocksDatabase rocksDB;

  private Set<String> histogramAttributes = new HashSet<>();

  private String contextName;

  private static final Logger LOG =
      LoggerFactory.getLogger(RocksDBStoreMBean.class);

  public static final String ROCKSDB_CONTEXT_PREFIX = "Rocksdb_";
  public static final String ROCKSDB_PROPERTY_PREFIX = "rocksdb.";

  // RocksDB properties
  // Column1: rocksDB property original name
  // Column2: aggregate or not all CF value to get a summed value
  // Column2: converted rocksDB property name based on Prometheus naming rule
  private final String[][] cfPros = {
      // 1 if a memtable flush is pending; otherwise, returns 0
      {"mem-table-flush-pending", "false", ""},
      // estimated total number of bytes compaction needs to rewrite to get
      // all levels down to under target size.
      {"estimate-pending-compaction-bytes", "true", ""},
      // 1 if at least one compaction is pending; otherwise, returns 0.
      {"compaction-pending", "false", ""},
      // block cache capacity
      {"block-cache-capacity", "true", ""},
      // the memory size for the entries residing in block cache
      {"block-cache-usage", "true", ""},
      // the memory size for the entries being pinned
      {"block-cache-pinned-usage", "true", ""},
      // number of level to which L0 data will be compacted.
      {"base-level", "false", ""},
      // approximate active mem table size (bytes)
      {"cur-size-active-mem-table", "true", ""},
      // approximate size of active and unflushed immutable (bytes)
      {"cur-size-all-mem-tables", "true", ""},
      // approximate size of active, unflushed immutable, and pinned immutable
      // memtables (bytes)
      {"size-all-mem-tables", "true", ""},
      // number of immutable memtables that have not yet been flushed
      {"num-immutable-mem-table", "true", ""},
      // total size (bytes) of all SST files belong to the latest LSM tree
      {"live-sst-files-size", "true", ""},
      // estimated number of total keys in memtables and storage(Can be very
      // wrong according to RocksDB document)
      {"estimate-num-keys", "true", ""},
      // estimated memory used for reading SST tables, excluding memory used
      // in block cache (e.g., filter and index blocks)
      {"estimate-table-readers-mem", "true", ""}
  };

  // level-x sst file info (Global)
  private static final String NUM_FILES_AT_LEVEL = "num_files_at_level";
  private static final String SIZE_AT_LEVEL = "size_at_level";

  public RocksDBStoreMBean(Statistics statistics, RocksDatabase db,
      String dbName) {
    this.contextName = ROCKSDB_CONTEXT_PREFIX + dbName;
    this.statistics = statistics;
    this.rocksDB = db;
    histogramAttributes.add("Average");
    histogramAttributes.add("Median");
    histogramAttributes.add("Percentile95");
    histogramAttributes.add("Percentile99");
    histogramAttributes.add("StandardDeviation");
    histogramAttributes.add("Max");

    // To get the metric name complied with prometheus naming rule
    for (String[] property : cfPros) {
      property[2] = property[0].replace("-", "_");
    }
  }

  public static RocksDBStoreMBean create(Statistics statistics,
      RocksDatabase db, String contextName) {
    RocksDBStoreMBean rocksDBStoreMBean = new RocksDBStoreMBean(
        statistics, db, contextName);
    MetricsSystem ms = DefaultMetricsSystem.instance();
    MetricsSource metricsSource = ms.getSource(rocksDBStoreMBean.contextName);
    if (metricsSource != null) {
      return (RocksDBStoreMBean)metricsSource;
    } else {
      return ms.register(rocksDBStoreMBean.contextName,
          "RocksDB Metrics",
          rocksDBStoreMBean);
    }
  }

  @Override
  public Object getAttribute(String attribute)
      throws AttributeNotFoundException, MBeanException, ReflectionException {
    for (String histogramAttribute : histogramAttributes) {
      if (attribute.endsWith("_" + histogramAttribute.toUpperCase())) {
        String keyName = attribute
            .substring(0, attribute.length() - histogramAttribute.length() - 1);
        try {
          HistogramData histogram =
              statistics.getHistogramData(HistogramType.valueOf(keyName));
          try {
            Method method =
                HistogramData.class.getMethod("get" + histogramAttribute);
            return method.invoke(histogram);
          } catch (Exception e) {
            throw new ReflectionException(e,
                "Can't read attribute " + attribute);
          }
        } catch (IllegalArgumentException exception) {
          throw new AttributeNotFoundException(
              "No such attribute in RocksDB stats: " + attribute);
        }
      }
    }
    try {
      return statistics.getTickerCount(TickerType.valueOf(attribute));
    } catch (IllegalArgumentException ex) {
      throw new AttributeNotFoundException(
          "No such attribute in RocksDB stats: " + attribute);
    }
  }

  @Override
  public void setAttribute(Attribute attribute)
      throws AttributeNotFoundException, InvalidAttributeValueException,
      MBeanException, ReflectionException {
  }

  @Override
  public AttributeList getAttributes(String[] attributes) {
    AttributeList result = new AttributeList();
    for (String attributeName : attributes) {
      try {
        Object value = getAttribute(attributeName);
        result.add(value);
      } catch (Exception e) {
        //TODO
      }
    }
    return result;
  }

  @Override
  public AttributeList setAttributes(AttributeList attributes) {
    return null;
  }

  @Override
  public Object invoke(String actionName, Object[] params, String[] signature)
      throws MBeanException, ReflectionException {
    return null;
  }

  @Override
  public MBeanInfo getMBeanInfo() {

    List<MBeanAttributeInfo> attributes = new ArrayList<>();
    for (TickerType tickerType : TickerType.values()) {
      attributes.add(new MBeanAttributeInfo(tickerType.name(), "long",
          "RocksDBStat: " + tickerType.name(), true, false, false));
    }
    for (HistogramType histogramType : HistogramType.values()) {
      for (String histogramAttribute : histogramAttributes) {
        attributes.add(new MBeanAttributeInfo(
            histogramType.name() + "_" + histogramAttribute.toUpperCase(),
            "long", "RocksDBStat: " + histogramType.name(), true, false,
            false));
      }
    }

    return new MBeanInfo("", "RocksDBStat",
        attributes.toArray(new MBeanAttributeInfo[0]), null, null, null);

  }

  @Override
  public void getMetrics(MetricsCollector metricsCollector, boolean b) {
    MetricsRecordBuilder rb = metricsCollector.addRecord(contextName);
    getHistogramData(rb);
    getTickerTypeData(rb);
    getDBPropertyData(rb);
  }

  /**
   * Collect all histogram metrics from RocksDB statistics.
   * @param rb Metrics Record Builder.
   */
  private void getHistogramData(MetricsRecordBuilder rb) {
    for (HistogramType histogramType : HistogramType.values()) {
      HistogramData histogram =
          statistics.getHistogramData(
              HistogramType.valueOf(histogramType.name()));
      for (String histogramAttribute : histogramAttributes) {
        try {
          Method method =
              HistogramData.class.getMethod("get" + histogramAttribute);
          double metricValue =  (double) method.invoke(histogram);
          rb.addGauge(Interns.info(histogramType.name() + "_" +
                  histogramAttribute.toUpperCase(), "RocksDBStat"),
              metricValue);
        } catch (Exception e) {
          LOG.error("Error reading histogram data", e);
        }
      }
    }
  }

  /**
   * Collect all Counter metrics from RocksDB statistics.
   * @param rb Metrics Record Builder.
   */
  private void getTickerTypeData(MetricsRecordBuilder rb) {
    for (TickerType tickerType : TickerType.values()) {
      rb.addCounter(Interns.info(tickerType.name(), "RocksDBStat"),
          statistics.getTickerCount(tickerType));
    }
  }

  /**
   * Collect info from rocksdb property.
   * @param rb Metrics Record Builder.
   */
  private void getDBPropertyData(MetricsRecordBuilder rb) {
    int index = 0;
    try {
      for (index = 0; index < cfPros.length; index++) {
        Boolean aggregated = Boolean.valueOf(cfPros[index][1]);
        long sum = 0;
        for (RocksDatabase.ColumnFamily cf : rocksDB.getExtraColumnFamilies()) {
          // Metrics per column family
          long value = Long.parseLong(rocksDB.getProperty(cf,
              ROCKSDB_PROPERTY_PREFIX + cfPros[index][0]));
          rb.addCounter(Interns.info(cf.getName() + "_" +
              cfPros[index][2], "RocksDBProperty"), value);
          if (aggregated) {
            sum += value;
          }
        }
        // Export aggregated value
        if (aggregated) {
          rb.addCounter(
              Interns.info(cfPros[index][2], "RocksDBProperty"), sum);
        }
      }
    } catch (IOException e) {
      LOG.error("Failed to get property {} from rocksdb",
          cfPros[index][0], e);
    }

    // Calculate number of files per level and size per level
    Map<String, Map<Integer, Map<String, Long>>> data = computeSstFileStat();

    // Export file number
    exportSstFileStat(rb, data.get(NUM_FILES_AT_LEVEL), NUM_FILES_AT_LEVEL);

    // Export file total size
    exportSstFileStat(rb, data.get(SIZE_AT_LEVEL), SIZE_AT_LEVEL);
  }

  private Map<String, Map<Integer, Map<String, Long>>> computeSstFileStat() {
    // Calculate number of files per level and size per level
    List<LiveFileMetaData> liveFileMetaDataList =
        rocksDB.getLiveFilesMetaData();

    Map<String, Map<Integer, Map<String, Long>>> ret = new HashMap();
    Map<Integer, Map<String, Long>> numStatPerCF = new HashMap<>();
    Map<Integer, Map<String, Long>> sizeStatPerCF = new HashMap<>();
    Map<String, Long> numStat;
    Map<String, Long> sizeStat;
    for (LiveFileMetaData file: liveFileMetaDataList) {
      numStat = numStatPerCF.get(file.level());
      String cf = Strings.fromByteArray(file.columnFamilyName());
      if (numStat != null) {
        Long value = numStat.get(cf);
        numStat.put(cf, value == null ? 1L : value + 1);
      } else {
        numStat = new HashMap<>();
        numStat.put(cf, 1L);
        numStatPerCF.put(file.level(), numStat);
      }

      sizeStat = sizeStatPerCF.get(file.level());
      if (sizeStat != null) {
        Long value = sizeStat.get(cf);
        sizeStat.put(cf, value == null ? file.size() : value + file.size());
      } else {
        sizeStat = new HashMap<>();
        sizeStat.put(cf, file.size());
        sizeStatPerCF.put(file.level(), sizeStat);
      }
    }

    ret.put(NUM_FILES_AT_LEVEL, numStatPerCF);
    ret.put(SIZE_AT_LEVEL, sizeStatPerCF);
    return ret;
  }

  private void exportSstFileStat(MetricsRecordBuilder rb,
      Map<Integer, Map<String, Long>> numStatPerCF, String metricName) {
    Map<String, Long> numStat;
    for (Map.Entry<Integer, Map<String, Long>> entry: numStatPerCF.entrySet()) {
      numStat = entry.getValue();
      numStat.forEach((cf, v) -> rb.addCounter(Interns.info(
            cf + "_" + metricName + entry.getKey(), "RocksDBProperty"), v));
      rb.addCounter(
          Interns.info(metricName + entry.getKey(), "RocksDBProperty"),
          numStat.values().stream().mapToLong(p -> p.longValue()).sum());
    }
  }
}
