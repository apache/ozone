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

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.SystemUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.SignalLogger;
import org.apache.hadoop.hdds.utils.VersionInfo;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.ratis.thirdparty.io.netty.buffer.Unpooled;
import org.slf4j.Logger;

/**
 * Simple utility class to collection string conversion methods.
 */
public final class StringUtils {

  private StringUtils() {
  }

  private static final Charset UTF8 = StandardCharsets.UTF_8;

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
    return new String(bytes, offset, length, UTF8);
  }

  public static String bytes2String(ByteBuffer bytes) {
    return bytes2String(bytes, UTF8);
  }

  public static String bytes2String(ByteBuffer bytes, Charset charset) {
    return Unpooled.wrappedBuffer(bytes.asReadOnlyBuffer()).toString(charset);
  }

  public static String bytes2Hex(ByteBuffer buffer, int max) {
    buffer = buffer.asReadOnlyBuffer();
    final int remaining = buffer.remaining();
    final int n = Math.min(max, remaining);
    final StringBuilder builder = new StringBuilder(3 * n);
    for (int i = 0; i < n; i++) {
      builder.append(String.format("%02X ", buffer.get()));
    }
    return builder + (remaining > max ? "..." : "");
  }

  public static String bytes2Hex(ByteBuffer buffer) {
    return bytes2Hex(buffer, buffer.remaining());
  }

  public static String bytes2Hex(byte[] array) {
    return bytes2Hex(ByteBuffer.wrap(array));
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
    return str.getBytes(UTF8);
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
      Class<?> clazz, String[] args, Logger log, OzoneConfiguration conf) {
    final String hostname = NetUtils.getHostname();
    final String className = clazz.getSimpleName();

    if (log.isInfoEnabled()) {
      log.info(createStartupShutdownMessage(versionInfo, className, hostname,
          args, HddsUtils.processForLogging(conf)));
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
      String className, String hostname, String[] args,
      Map<String, String> conf) {
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
        "  java = " + System.getProperty("java.version"),
        "  conf = " + conf);
  }

  public static String appendIfNotPresent(String str, char c) {
    Preconditions.checkNotNull(str, "Input string is null");
    return str.isEmpty() || str.charAt(str.length() - 1) != c ? str + c : str;
  }
}
