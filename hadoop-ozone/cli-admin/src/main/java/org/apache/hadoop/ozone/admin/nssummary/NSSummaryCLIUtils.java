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

package org.apache.hadoop.ozone.admin.nssummary;

import static java.net.HttpURLConnection.HTTP_CREATED;
import static java.net.HttpURLConnection.HTTP_OK;

import java.io.InputStream;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import javax.security.sasl.AuthenticationException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import picocli.CommandLine.Help.Ansi;

/**
 * Utility class to support Namespace CLI.
 */
public final class NSSummaryCLIUtils {

  private static final String OFS_PREFIX = "ofs://";

  public static String makeHttpCall(StringBuilder url, String path,
                                    boolean isSpnegoEnabled,
                                    ConfigurationSource conf)
      throws Exception {
    return makeHttpCall(url, path, false, false, isSpnegoEnabled, conf);
  }

  public static String makeHttpCall(StringBuilder url, String path,
                                    boolean listFile, boolean withReplica,
                                    boolean isSpnegoEnabled,
                                    ConfigurationSource conf)
      throws Exception {

    url.append("?path=").append(path);

    if (listFile) {
      url.append("&files=true");
    }
    if (withReplica) {
      url.append("&replica=true");
    }

    System.out.println("Connecting to Recon: " + url + " ...");
    final URLConnectionFactory connectionFactory =
        URLConnectionFactory.newDefaultURLConnectionFactory(
            (Configuration) conf);

    HttpURLConnection httpURLConnection;

    try {
      httpURLConnection = (HttpURLConnection)
          connectionFactory.openConnection(new URL(url.toString()),
              isSpnegoEnabled);
      httpURLConnection.connect();
      int errorCode = httpURLConnection.getResponseCode();
      InputStream inputStream = httpURLConnection.getInputStream();

      if ((errorCode == HTTP_OK) || (errorCode == HTTP_CREATED)) {
        return IOUtils.toString(inputStream, StandardCharsets.UTF_8);
      }

      if (httpURLConnection.getErrorStream() != null) {
        System.out.println("Recon is being initialized. Please wait a moment");
        return null;
      } else {
        System.out.println("Unexpected null in http payload," +
            " while processing request");
      }
      return null;
    } catch (ConnectException ex) {
      System.err.println("Connection Refused. Please make sure the " +
          "Recon Server has been started.");
      return null;
    } catch (AuthenticationException authEx) {
      System.err.println("Authentication Failed. Please make sure you " +
          "have login or disable Ozone security settings.");
      return null;
    }
  }

  public static void printNewLines(int cnt) {
    for (int i = 0; i < cnt; ++i) {
      System.out.println();
    }
  }

  public static void printSpaces(int cnt) {
    for (int i = 0; i < cnt; ++i) {
      System.out.print(" ");
    }
  }

  public static void printEmptyPathRequest() {
    System.err.println("The path parameter is empty.\n" +
        "If you mean the root path, use / instead.");
  }

  public static void printPathNotFound() {
    System.err.println("Path not found in the system.\n" +
        "Did you remove any protocol prefix before the path?");
  }

  public static void printTypeNA(String requestType) {
    String markUp = "@|underline " + requestType + "|@";
    System.err.println("Path found in the system.\nBut the entity type " +
        "is not applicable to the " + Ansi.AUTO.string(markUp) + " request");
  }

  public static void printKVSeparator() {
    System.out.print(" : ");
  }

  public static void printWithUnderline(String str, boolean newLine) {
    String markupStr = "@|underline " + str + "|@";
    if (newLine) {
      System.out.println(Ansi.AUTO.string(markupStr));
    } else {
      System.out.print(Ansi.AUTO.string(markupStr));
    }
  }

  public static String parseInputPath(String path) {
    if (!path.startsWith("ofs://")) {
      return path;
    }
    int idx = path.indexOf("/", OFS_PREFIX.length());
    if (idx == -1) {
      return path.substring(OFS_PREFIX.length());
    }
    return path.substring(idx);
  }

  private NSSummaryCLIUtils() {

  }
}
