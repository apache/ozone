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

package org.apache.hadoop.hdds.scm.client;

import com.google.common.collect.ImmutableList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
  static final int MAX_BUCKET_NAME_LENGTH_IN_LOG =
      2 * OzoneConsts.OZONE_MAX_BUCKET_NAME_LENGTH;

  private static final String VALID_LENGTH_MESSAGE = String.format(
      "valid length is %d-%d characters",
      OzoneConsts.OZONE_MIN_BUCKET_NAME_LENGTH,
      OzoneConsts.OZONE_MAX_BUCKET_NAME_LENGTH);

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

  private HddsClientUtils() {
  }

  private static void doNameChecks(String resName, String resType) {
    if (resName == null) {
      throw new IllegalArgumentException(resType + " name is null");
    }

    if (resName.length() < OzoneConsts.OZONE_MIN_BUCKET_NAME_LENGTH) {
      throw new IllegalArgumentException(resType +
          " name '" + resName + "' is too short, " +
          VALID_LENGTH_MESSAGE);
    }

    if (resName.length() > OzoneConsts.OZONE_MAX_BUCKET_NAME_LENGTH) {
      String nameToReport;

      if (resName.length() > MAX_BUCKET_NAME_LENGTH_IN_LOG) {
        nameToReport = String.format(
            "%s...",
            resName.substring(0, MAX_BUCKET_NAME_LENGTH_IN_LOG));
      } else {
        nameToReport = resName;
      }

      throw new IllegalArgumentException(resType +
          " name '" + nameToReport + "' is too long, " +
          VALID_LENGTH_MESSAGE);
    }

    if (resName.charAt(0) == '.' || resName.charAt(0) == '-') {
      throw new IllegalArgumentException(resType +
          " name cannot start with a period or dash");
    }

    if (resName.charAt(resName.length() - 1) == '.' ||
        resName.charAt(resName.length() - 1) == '-') {
      throw new IllegalArgumentException(resType +
          " name cannot end with a period or dash");
    }
  }

  private static boolean isSupportedCharacter(char c, boolean isStrictS3) {
    // When isStrictS3 is set as false,
    // ozone allows namespace to follow other volume/bucket naming convention,
    // for example, here supports '_',
    // which is a valid character in POSIX-compliant system, like HDFS.
    if (c >= '0' && c <= '9') {
      return true;
    } else if (c >= 'a' && c <= 'z') {
      return true;
    } else if (c == '-' || c == '.') {
      return true;
    } else if (c == '_' && !isStrictS3) {
      return true;
    }
    return false;
  }

  private static void doCharacterChecks(char currChar, char prev, String resType,
      boolean isStrictS3) {
    if (Character.isUpperCase(currChar)) {
      throw new IllegalArgumentException(resType +
          " name does not support uppercase characters");
    }
    if (!isSupportedCharacter(currChar, isStrictS3)) {
      throw new IllegalArgumentException(resType +
          " name has an unsupported character : " + currChar);
    }
    if (prev == '.' && currChar == '.') {
      throw new IllegalArgumentException(resType +
          " name should not have two contiguous periods");
    }
    if (prev == '-' && currChar == '.') {
      throw new IllegalArgumentException(resType +
          " name should not have period after dash");
    }
    if (prev == '.' && currChar == '-') {
      throw new IllegalArgumentException(resType +
          " name should not have dash after period");
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
    verifyResourceName(resName, "resource", true);
  }

  public static void verifyResourceName(String resName, String resType) {
    verifyResourceName(resName, resType, true);
  }

  /**
   * verifies that bucket name / volume name is a valid DNS name.
   *
   * @param resName Bucket or volume Name to be validated
   *
   * @throws IllegalArgumentException
   */
  public static void verifyResourceName(String resName, String resType, boolean isStrictS3) {
    boolean isIPv4 = true;
    char prev = (char) 0;

    for (int index = 0; index < resName.length(); index++) {
      char currChar = resName.charAt(index);
      if (currChar != '.') {
        isIPv4 = ((currChar >= '0') && (currChar <= '9')) && isIPv4;
      }
      doCharacterChecks(currChar, prev, resType, isStrictS3);
      prev = currChar;
    }

    if (isIPv4) {
      throw new IllegalArgumentException(resType +
          " name cannot be an IPv4 address or all numeric");
    }

    doNameChecks(resName, resType);
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
    if (!OzoneConsts.KEYNAME_ILLEGAL_CHARACTER_CHECK_REGEX
            .matcher(keyName).matches()) {
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
      Objects.requireNonNull(ref, "ref == null");
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
   * Returns the default S3 Volume name configured in ConfigurationSource,
   * which will be used for S3 buckets that are not part of a tenant.
   * @param conf Configuration object
   * @return s3 volume name
   */
  public static String getDefaultS3VolumeName(ConfigurationSource conf) {
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

  // This will return the underlying expected exception if it exists
  // in an exception trace. Otherwise, returns null.
  public static Throwable containsException(Throwable t,
            Class<? extends Exception> expectedExceptionClass) {
    while (t != null) {
      if (expectedExceptionClass.isInstance(t)) {
        return t;
      }
      t = t.getCause();
    }
    return null;
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
