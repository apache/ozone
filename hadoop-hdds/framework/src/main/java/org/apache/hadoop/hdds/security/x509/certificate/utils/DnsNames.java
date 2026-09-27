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

package org.apache.hadoop.hdds.security.x509.certificate.utils;

import java.net.IDN;
import java.util.Optional;
import org.apache.commons.validator.routines.InetAddressValidator;

/**
 * Shared helper for validating and normalizing RFC 1123 DNS names used as
 * certificate Subject Alternative Names.
 */
public final class DnsNames {

  private static final int MAX_NAME_LENGTH = 253;
  private static final int MAX_LABEL_LENGTH = 63;

  private DnsNames() {
  }

  /**
   * Normalizes a candidate DNS name for use as a certificate SAN value.
   * Strips at most one trailing '.', converts it to its ASCII/A-label form
   * via IDN, and validates the result with {@link #isValidDnsName(String)}.
   *
   * @param candidate the raw candidate DNS name
   * @return the normalized DNS name, or {@link Optional#empty()} if the
   *     candidate is null, empty, or not a valid DNS name
   */
  public static Optional<String> toDnsSanValue(String candidate) {
    if (candidate == null || candidate.isEmpty()) {
      return Optional.empty();
    }

    String stripped = candidate.endsWith(".")
        ? candidate.substring(0, candidate.length() - 1)
        : candidate;

    String ascii;
    try {
      ascii = IDN.toASCII(stripped, IDN.ALLOW_UNASSIGNED);
    } catch (IllegalArgumentException e) {
      return Optional.empty();
    }

    return isValidDnsName(ascii) ? Optional.of(ascii) : Optional.empty();
  }

  /**
   * Validates that the given value is a syntactically valid RFC 1123 DNS
   * name for use as a certificate Subject Alternative Name. Does not perform
   * IDN conversion or trailing-dot stripping.
   *
   * @param value the DNS name to validate
   * @return true iff the value is a valid RFC 1123 DNS name
   */
  public static boolean isValidDnsName(String value) {
    if (value == null || value.isEmpty() || value.length() > MAX_NAME_LENGTH) {
      return false;
    }

    if (InetAddressValidator.getInstance().isValid(value)) {
      return false;
    }

    String[] labels = value.split("\\.", -1);
    for (String label : labels) {
      if (!isValidLabel(label)) {
        return false;
      }
    }
    return true;
  }

  private static boolean isValidLabel(String label) {
    int length = label.length();
    if (length < 1 || length > MAX_LABEL_LENGTH) {
      return false;
    }
    if (label.charAt(0) == '-' || label.charAt(length - 1) == '-') {
      return false;
    }
    for (int i = 0; i < length; i++) {
      char c = label.charAt(i);
      boolean isAlphaNumeric = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9');
      if (!isAlphaNumeric && c != '-') {
        return false;
      }
    }
    return true;
  }
}
