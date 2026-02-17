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

package org.apache.hadoop.ozone.debug.audit.parser.common;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.debug.audit.parser.model.AuditEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Database helper for ozone audit parser tool.
 */
public final class DatabaseHelper {

  private static final Logger LOG =
      LoggerFactory.getLogger(DatabaseHelper.class);

  static {
    loadProperties();
  }

  private static Map<String, String> properties;

  private DatabaseHelper() {
    //Never constructed
  }

  public static boolean setup(String dbName, String logs) throws Exception {
    if (createAuditTable(dbName)) {
      return insertAudits(dbName, logs);
    } else {
      return false;
    }
  }

  private static Connection getConnection(String dbName) throws Exception {
    Class.forName(ParserConsts.DRIVER);
    return DriverManager.getConnection(ParserConsts.CONNECTION_PREFIX + dbName);
  }

  private static void loadProperties() {
    Properties props = new Properties();
    try {
      InputStream inputStream = DatabaseHelper.class.getClassLoader()
          .getResourceAsStream(ParserConsts.PROPS_FILE);
      if (inputStream != null) {
        props.load(inputStream);
        properties = props.entrySet().stream().collect(
            Collectors.toMap(
                e -> e.getKey().toString(),
                e -> e.getValue().toString()
            )
        );
      } else {
        throw new FileNotFoundException("property file '"
            + ParserConsts.PROPS_FILE + "' not found in the classpath");
      }
    } catch (Exception e) {
      LOG.error(e.getMessage());
    }

  }

  private static boolean createAuditTable(String dbName) throws Exception {
    try (Connection connection = getConnection(dbName);
         Statement st = connection.createStatement()) {
      st.executeUpdate(properties.get(ParserConsts.CREATE_AUDIT_TABLE));
    }
    return true;
  }

  private static boolean insertAudits(String dbName, String logs)
      throws Exception {
    try (Connection connection = getConnection(dbName);
         PreparedStatement preparedStatement = connection.prepareStatement(
             properties.get(ParserConsts.INSERT_AUDITS))) {
      ArrayList<AuditEntry> auditEntries = parseAuditLogs(logs);
      final int batchSize = 1000;
      int count = 0;
      //Insert list to db
      for (AuditEntry audit : auditEntries) {
        preparedStatement.setString(1, audit.getTimestamp());
        preparedStatement.setString(2, audit.getLevel());
        preparedStatement.setString(3, audit.getLogger());
        preparedStatement.setString(4, audit.getUser());
        preparedStatement.setString(5, audit.getIp());
        preparedStatement.setString(6, audit.getOp());
        preparedStatement.setString(7, audit.getParams());
        preparedStatement.setString(8, audit.getResult());
        preparedStatement.setString(9, audit.getException());
        preparedStatement.addBatch();
        if (++count % batchSize == 0) {
          preparedStatement.executeBatch();
        }
      }
      if (auditEntries.size() % batchSize != 0) {
        // insert remaining records
        preparedStatement.executeBatch();
      }
    }
    return true;
  }

  @SuppressWarnings("squid:S3776")
  private static ArrayList<AuditEntry> parseAuditLogs(String filePath)
      throws IOException {
    ArrayList<AuditEntry> listResult = new ArrayList<>();
    try (InputStream fis = Files.newInputStream(Paths.get(filePath));
         InputStreamReader isr = new InputStreamReader(fis, UTF_8);
         BufferedReader bReader = new BufferedReader(isr)) {
      String currentLine = bReader.readLine();
      String nextLine = bReader.readLine();
      String[] entry;
      AuditEntry tempEntry = null;

      while (true) {
        if (tempEntry == null) {
          tempEntry = new AuditEntry();
        }

        if (currentLine == null) {
          break;
        } else {
          if (!currentLine.matches(ParserConsts.DATE_REGEX)) {
            tempEntry.appendException(currentLine);
          } else {
            entry = StringUtils.stripAll(currentLine.split("\\|"));
            String[] ops =
                entry[5].substring(entry[5].indexOf('=') + 1).split(" ", 2);
            tempEntry = new AuditEntry.Builder()
                .setTimestamp(entry[0])
                .setLevel(entry[1])
                .setLogger(entry[2])
                .setUser(entry[3].substring(entry[3].indexOf('=') + 1))
                .setIp(entry[4].substring(entry[4].indexOf('=') + 1))
                .setOp(ops[0])
                .setParams(ops[1])
                .setResult(entry[6].substring(entry[6].indexOf('=') + 1))
                .build();
            if (entry.length == 8) {
              tempEntry.setException(entry[7]);
            }
          }
          if (nextLine == null || nextLine.matches(ParserConsts.DATE_REGEX)) {
            listResult.add(tempEntry);
            tempEntry = null;
          }
          currentLine = nextLine;
          nextLine = bReader.readLine();
        }
      }
    }

    return listResult;
  }

  public static String executeCustomQuery(String dbName, String query)
      throws Exception {
    return executeStatement(dbName, query);
  }

  public static String executeTemplate(String dbName, String template)
      throws Exception {
    return executeStatement(dbName, properties.get(template));
  }

  private static String executeStatement(String dbName, String sql)
      throws Exception {
    StringBuilder result = new StringBuilder();
    ResultSetMetaData rsm;
    try (Connection connection = getConnection(dbName);
        PreparedStatement ps = connection.prepareStatement(sql);
        ResultSet rs = ps.executeQuery()) {
      if (rs != null) {
        rsm = rs.getMetaData();
        int cols = rsm.getColumnCount();
        while (rs.next()) {
          for (int index = 1; index <= cols; index++) {
            result.append(rs.getObject(index));
            result.append('\t');
          }
          result.append('\n');
        }
      }
    }
    return result.toString();
  }

  public static boolean validateTemplate(String templateName) {
    return (properties.get(templateName) != null);
  }
}
