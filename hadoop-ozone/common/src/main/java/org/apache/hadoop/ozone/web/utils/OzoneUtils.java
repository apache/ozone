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

package org.apache.hadoop.ozone.web.utils;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.Objects;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.ratis.util.TimeDuration;

/**
 * Set of Utility functions used in ozone.
 */
@InterfaceAudience.Private
public final class OzoneUtils {

  public static final Charset ENCODING = StandardCharsets.UTF_8;

  /**
   * Date format that used in ozone. Here the format is thread safe to use.
   */
  private static final ThreadLocal<SimpleDateFormat> DATE_FORMAT =
      new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
          SimpleDateFormat format = new SimpleDateFormat(
              OzoneConsts.OZONE_DATE_FORMAT, Locale.US);
          format.setTimeZone(TimeZone.getTimeZone(OzoneConsts.OZONE_TIME_ZONE));

          return format;
        }
      };

  private OzoneUtils() {
    // Never constructed
  }

  /**
   * Verifies that max key length is a valid value.
   *
   * @param length
   *          The max key length to be validated
   *
   * @throws IllegalArgumentException
   */
  public static void verifyMaxKeyLength(String length)
      throws IllegalArgumentException {
    int maxKey = 0;
    try {
      maxKey = Integer.parseInt(length);
    } catch (NumberFormatException nfe) {
      throw new IllegalArgumentException(
          "Invalid max key length, the value should be digital.");
    }

    if (maxKey <= 0) {
      throw new IllegalArgumentException(
          "Invalid max key length, the value should be a positive number.");
    }
  }

  /**
   * Returns a random Request ID.
   *
   * Request ID is returned to the client as well as flows through the system
   * facilitating debugging on why a certain request failed.
   *
   * @return String random request ID
   */
  public static String getRequestID() {
    return UUID.randomUUID().toString();
  }

  /**
   * Return host name if possible.
   *
   * @return Host Name or localhost
   */
  public static String getHostName() {
    String host = "localhost";
    try {
      host = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      // Ignore the error
    }
    return host;
  }

  /**
   * Convert time in millisecond to a human readable format required in ozone.
   * @return a human readable string for the input time
   */
  public static String formatTime(long millis) {
    return DATE_FORMAT.get().format(millis);
  }

  /**
   * Convert time in ozone date format to millisecond.
   * @return time in milliseconds
   */
  public static long formatDate(String date) throws ParseException {
    Objects.requireNonNull(date, "Date string should not be null.");
    return DATE_FORMAT.get().parse(date).getTime();
  }
  
  /**
   * verifies that bucket name / volume name is a valid DNS name.
   *
   * @param resName Bucket or volume Name to be validated
   *
   * @throws IllegalArgumentException
   */
  public static void verifyResourceName(String resName)
      throws IllegalArgumentException {
    HddsClientUtils.verifyResourceName(resName);
  }

  /**
   * Return the TimeDuration configured for the given key. If not configured,
   * return the default value.
   */
  public static TimeDuration getTimeDuration(ConfigurationSource conf,
      String key,
      TimeDuration defaultValue) {
    TimeUnit defaultTimeUnit = defaultValue.getUnit();
    long timeDurationInDefaultUnit = conf.getTimeDuration(key,
        defaultValue.getDuration(), defaultTimeUnit);
    return TimeDuration.valueOf(timeDurationInDefaultUnit, defaultTimeUnit);
  }

  /**
   * Return the time configured for the given key in milliseconds.
   */
  public static long getTimeDurationInMS(ConfigurationSource conf, String key,
      TimeDuration defaultValue) {
    return getTimeDuration(conf, key, defaultValue)
        .toLong(TimeUnit.MILLISECONDS);
  }

}
