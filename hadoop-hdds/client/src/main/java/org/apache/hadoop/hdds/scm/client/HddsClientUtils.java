/**
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

package org.apache.hadoop.hdds.scm.client;

import java.text.ParseException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.ratis.conf.RatisClientConfig;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.ratis.protocol.exceptions.AlreadyClosedException;
import org.apache.ratis.protocol.exceptions.GroupMismatchException;
import org.apache.ratis.protocol.exceptions.NotReplicatedException;
import org.apache.ratis.protocol.exceptions.RaftRetryFailureException;

/**
 * Utility methods for Ozone and Container Clients.
 *
 * The methods to retrieve SCM service endpoints assume there is a single
 * SCM service instance. This will change when we switch to replicated service
 * instances for redundancy.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public final class HddsClientUtils {

  private HddsClientUtils() {
  }

  private static final List<Class<? extends Exception>> EXCEPTION_LIST =
      ImmutableList.<Class<? extends Exception>>builder()
          .add(TimeoutException.class)
          .add(StorageContainerException.class)
          .add(RaftRetryFailureException.class)
          .add(AlreadyClosedException.class)
          .add(GroupMismatchException.class)
          // Not Replicated Exception will be thrown if watch For commit
          // does not succeed
          .add(NotReplicatedException.class)
          .build();

  /**
   * Date format that used in ozone. Here the format is thread safe to use.
   */
  private static final ThreadLocal<DateTimeFormatter> DATE_FORMAT =
      ThreadLocal.withInitial(() -> {
        DateTimeFormatter format =
            DateTimeFormatter.ofPattern(OzoneConsts.OZONE_DATE_FORMAT);
        return format.withZone(ZoneId.of(OzoneConsts.OZONE_TIME_ZONE));
      });


  /**
   * Convert time in millisecond to a human readable format required in ozone.
   * @return a human readable string for the input time
   */
  public static String formatDateTime(long millis) {
    ZonedDateTime dateTime = ZonedDateTime.ofInstant(
        Instant.ofEpochMilli(millis), DATE_FORMAT.get().getZone());
    return DATE_FORMAT.get().format(dateTime);
  }

  /**
   * Convert time in ozone date format to millisecond.
   * @return time in milliseconds
   */
  public static long formatDateTime(String date) throws ParseException {
    Preconditions.checkNotNull(date, "Date string should not be null.");
    return ZonedDateTime.parse(date, DATE_FORMAT.get())
        .toInstant().toEpochMilli();
  }

  private static void doNameChecks(String resName) {
    if (resName == null) {
      throw new IllegalArgumentException("Bucket or Volume name is null");
    }

    if (resName.length() < OzoneConsts.OZONE_MIN_BUCKET_NAME_LENGTH ||
        resName.length() > OzoneConsts.OZONE_MAX_BUCKET_NAME_LENGTH) {
      throw new IllegalArgumentException(
          "Bucket or Volume length is illegal, "
              + "valid length is 3-63 characters");
    }

    if (resName.charAt(0) == '.' || resName.charAt(0) == '-') {
      throw new IllegalArgumentException(
          "Bucket or Volume name cannot start with a period or dash");
    }

    if (resName.charAt(resName.length() - 1) == '.' ||
        resName.charAt(resName.length() - 1) == '-') {
      throw new IllegalArgumentException("Bucket or Volume name "
          + "cannot end with a period or dash");
    }
  }

  private static boolean isSupportedCharacter(char c) {
    return (c == '.' || c == '-' ||
        Character.isLowerCase(c) || Character.isDigit(c));
  }

  private static void doCharacterChecks(char currChar, char prev) {
    if (Character.isUpperCase(currChar)) {
      throw new IllegalArgumentException(
          "Bucket or Volume name does not support uppercase characters");
    }
    if (!isSupportedCharacter(currChar)) {
      throw new IllegalArgumentException("Bucket or Volume name has an " +
          "unsupported character : " + currChar);
    }
    if (prev == '.' && currChar == '.') {
      throw new IllegalArgumentException("Bucket or Volume name should not " +
          "have two contiguous periods");
    }
    if (prev == '-' && currChar == '.') {
      throw new IllegalArgumentException(
          "Bucket or Volume name should not have period after dash");
    }
    if (prev == '.' && currChar == '-') {
      throw new IllegalArgumentException(
          "Bucket or Volume name should not have dash after period");
    }
  }

  /**
   * verifies that bucket name / volume name is a valid DNS name.
   *
   * @param resName Bucket or volume Name to be validated
   *
   * @throws IllegalArgumentException
   */
  public static void verifyResourceName(String resName) {

    doNameChecks(resName);

    boolean isIPv4 = true;
    char prev = (char) 0;

    for (int index = 0; index < resName.length(); index++) {
      char currChar = resName.charAt(index);
      if (currChar != '.') {
        isIPv4 = ((currChar >= '0') && (currChar <= '9')) && isIPv4;
      }
      doCharacterChecks(currChar, prev);
      prev = currChar;
    }

    if (isIPv4) {
      throw new IllegalArgumentException(
          "Bucket or Volume name cannot be an IPv4 address or all numeric");
    }
  }

  /**
   * verifies that bucket / volume name is a valid DNS name.
   *
   * @param resourceNames Array of bucket / volume names to be verified.
   */
  public static void verifyResourceName(String... resourceNames) {
    for (String resourceName : resourceNames) {
      HddsClientUtils.verifyResourceName(resourceName);
    }
  }

  /**
   * verifies that key name is a valid name.
   *
   * @param keyName key name to be validated
   *
   * @throws IllegalArgumentException
   */
  public static void verifyKeyName(String keyName) {
    if (keyName == null) {
      throw new IllegalArgumentException("Key name is null");
    }
    if(!OzoneConsts.KEYNAME_ILLEGAL_CHARACTER_CHECK_REGEX
            .matcher(keyName).matches()){
      throw new IllegalArgumentException("Invalid key name: " + keyName);
    }
  }

  /**
   * Checks that object parameters passed as reference is not null.
   *
   * @param references Array of object references to be checked.
   * @param <T>
   */
  public static <T> void checkNotNull(T... references) {
    for (T ref: references) {
      Preconditions.checkNotNull(ref);
    }
  }

  /**
   * Returns the cache value to be used for list calls.
   * @param conf Configuration object
   * @return list cache size
   */
  public static int getListCacheSize(ConfigurationSource conf) {
    return conf.getInt(OzoneConfigKeys.OZONE_CLIENT_LIST_CACHE_SIZE,
        OzoneConfigKeys.OZONE_CLIENT_LIST_CACHE_SIZE_DEFAULT);
  }

  /**
   * Returns the s3VolumeName configured in ConfigurationSource.
   * @param conf Configuration object
   * @return s3 volume name
   */
  public static String getS3VolumeName(ConfigurationSource conf) {
    return conf.get(OzoneConfigKeys.OZONE_S3_VOLUME_NAME,
            OzoneConfigKeys.OZONE_S3_VOLUME_NAME_DEFAULT);
  }

  /**
   * Returns the maximum no of outstanding async requests to be handled by
   * Standalone and Ratis client.
   */
  public static int getMaxOutstandingRequests(ConfigurationSource config) {
    return config
        .getObject(RatisClientConfig.RaftConfig.class)
        .getMaxOutstandingRequests();
  }


  // This will return the underlying exception after unwrapping
  // the exception to see if it matches with expected exception
  // list otherwise will return the exception back.
  public static Throwable checkForException(Exception e) {
    Throwable t = e;
    while (t != null && t.getCause() != null) {
      for (Class<? extends Exception> cls : getExceptionList()) {
        if (cls.isInstance(t)) {
          return t;
        }
      }
      t = t.getCause();
    }
    return t;
  }

  public static RetryPolicy createRetryPolicy(int maxRetryCount,
      long retryInterval) {
    // retry with fixed sleep between retries
    return RetryPolicies.retryUpToMaximumCountWithFixedSleep(
        maxRetryCount, retryInterval, TimeUnit.MILLISECONDS);
  }

  public static Map<Class<? extends Throwable>,
      RetryPolicy> getRetryPolicyByException(int maxRetryCount,
      long retryInterval) {
    Map<Class<? extends Throwable>, RetryPolicy> policyMap = new HashMap<>();
    for (Class<? extends Exception> ex : EXCEPTION_LIST) {
      if (ex == TimeoutException.class
          || ex == RaftRetryFailureException.class) {
        // retry without sleep
        policyMap.put(ex, createRetryPolicy(maxRetryCount, 0));
      } else {
        // retry with fixed sleep between retries
        policyMap.put(ex, createRetryPolicy(maxRetryCount, retryInterval));
      }
    }
    // Default retry policy
    policyMap
        .put(Exception.class, createRetryPolicy(maxRetryCount, retryInterval));
    return policyMap;
  }

  public static List<Class<? extends Exception>> getExceptionList() {
    return EXCEPTION_LIST;
  }
}