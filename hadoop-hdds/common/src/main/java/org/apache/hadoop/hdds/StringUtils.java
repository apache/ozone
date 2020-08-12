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
package org.apache.hadoop.hdds;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.SystemUtils;
import org.apache.hadoop.hdds.utils.SignalLogger;
import org.apache.hadoop.hdds.utils.VersionInfo;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.ShutdownHookManager;
import org.slf4j.Logger;

/**
 * Simple utility class to collection string conversion methods.
 */
public final class StringUtils {

  private StringUtils() {
  }

  // Using the charset canonical name for String/byte[] conversions is much
  // more efficient due to use of cached encoders/decoders.
  private static final String UTF8_CSN = StandardCharsets.UTF_8.name();

  /**
   * Priority of the StringUtils shutdown hook.
   */
  private static final int SHUTDOWN_HOOK_PRIORITY = 0;

  /**
   * Decode a specific range of bytes of the given byte array to a string
   * using UTF8.
   *
   * @param bytes  The bytes to be decoded into characters
   * @param offset The index of the first byte to decode
   * @param length The number of bytes to decode
   * @return The decoded string
   */
  public static String bytes2String(byte[] bytes, int offset, int length) {
    try {
      return new String(bytes, offset, length, UTF8_CSN);
    } catch (UnsupportedEncodingException e) {
      // should never happen!
      throw new IllegalArgumentException("UTF8 encoding is not supported", e);
    }
  }

  /**
   * Decode a specific range of bytes of the given byte array to a string
   * using UTF8.
   *
   * @param bytes The bytes to be decoded into characters
   * @return The decoded string
   */
  public static String bytes2String(byte[] bytes) {
    return bytes2String(bytes, 0, bytes.length);
  }

  /**
   * Converts a string to a byte array using UTF8 encoding.
   */
  public static byte[] string2Bytes(String str) {
    try {
      return str.getBytes(UTF8_CSN);
    } catch (UnsupportedEncodingException e) {
      // should never happen!
      throw new IllegalArgumentException("UTF8 decoding is not supported", e);
    }
  }

  /**
   * Return a message for logging.
   * @param prefix prefix keyword for the message
   * @param msg content of the message
   * @return a message for logging
   */
  public static String toStartupShutdownString(String prefix, String... msg) {
    StringBuilder b = new StringBuilder(prefix);
    b.append("\n/************************************************************");
    for (String s : msg) {
      b.append("\n").append(prefix).append(s);
    }
    b.append("\n************************************************************/");
    return b.toString();
  }

  public static void startupShutdownMessage(VersionInfo versionInfo,
      Class<?> clazz, String[] args, Logger log) {
    final String hostname = NetUtils.getHostname();
    final String className = clazz.getSimpleName();
    if (log.isInfoEnabled()) {
      log.info(createStartupShutdownMessage(versionInfo, className, hostname,
          args));
    }

    if (SystemUtils.IS_OS_UNIX) {
      try {
        SignalLogger.INSTANCE.register(log);
      } catch (Throwable t) {
        log.warn("failed to register any UNIX signal loggers: ", t);
      }
    }
    ShutdownHookManager.get().addShutdownHook(
        () -> log.info(toStartupShutdownString("SHUTDOWN_MSG: ",
            "Shutting down " + className + " at " + hostname)),
        SHUTDOWN_HOOK_PRIORITY);

  }

  /**
   * Generate the text for the startup/shutdown message of processes.
   * @param className short name of the class
   * @param hostname hostname
   * @param args Command arguments
   * @return a string to log.
   */
  public static String createStartupShutdownMessage(VersionInfo versionInfo,
      String className, String hostname, String[] args) {
    return toStartupShutdownString("STARTUP_MSG: ",
        "Starting " + className,
        "  host = " + hostname,
        "  args = " + (args != null ? Arrays.asList(args) : new ArrayList<>()),
        "  version = " + versionInfo.getVersion(),
        "  classpath = " + System.getProperty("java.class.path"),
        "  build = " + versionInfo.getUrl() + "/"
            + versionInfo.getRevision()
            + " ; compiled by '" + versionInfo.getUser()
            + "' on " + versionInfo.getDate(),
        "  java = " + System.getProperty("java.version"));
  }

  public static String appendIfNotPresent(String str, char c) {
    Preconditions.checkNotNull(str, "Input string is null");
    return str.isEmpty() || str.charAt(str.length() - 1) != c ? str + c: str;
  }
}
