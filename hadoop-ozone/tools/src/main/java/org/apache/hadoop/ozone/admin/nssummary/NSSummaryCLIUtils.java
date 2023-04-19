/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.admin.nssummary;

import com.google.gson.Gson;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import picocli.CommandLine.Help.Ansi;

import javax.security.sasl.AuthenticationException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

import static java.net.HttpURLConnection.HTTP_CREATED;
import static java.net.HttpURLConnection.HTTP_OK;

/**
 * Utility class to support Namespace CLI.
 */
public final class NSSummaryCLIUtils {

  private NSSummaryCLIUtils() {

  }

  private static final String OFS_PREFIX = "ofs://";

  public static String makeHttpCall(StringBuffer url, String path,
                                    boolean isSpnegoEnabled,
                                    ConfigurationSource conf)
      throws Exception {
    return makeHttpCall(url, path, false, false, isSpnegoEnabled, conf);
  }

  public static String makeHttpCall(StringBuffer url, String path,
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

  public static HashMap<String, Object> getResponseMap(String response) {
    return new Gson().fromJson(response, HashMap.class);
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

  public static void printBucketReminder() {
    printNewLines(1);
    System.out.println(
        "[Warning] Namespace CLI is not designed for OBS bucket layout.\n" +
            "Bucket being accessed must be of type FILE_SYSTEM_OPTIMIZED " +
            "bucket layout or \nLEGACY bucket layout with " +
            "'ozone.om.enable.filesystem.paths' set to true.");
    printNewLines(1);
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
}
