/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.client;

import org.apache.hadoop.ozone.OzoneConsts;

/**
 * OzoneRecoverWindow that can be applied to bucket.
 */
public class OzoneRecoverWindow {

  public static final String OZONE_RECOVER_WINDOW_MIN = "MIN";
  public static final String OZONE_RECOVER_WINDOW_HR = "HR";
  public static final String OZONE_RECOVER_WINDOW_DAY = "DAY";

  private Units unit;
  private long windowLength;

  /** Recover-window Units.*/
  public enum Units {UNDEFINED, MIN, HR, DAY}

  /**
   * Returns recover-window length.
   */
  public long getWindowLength() {
    return windowLength;
  }

  /**
   * Returns Units.
   *
   * @return Unit in MIN, HR or DAY
   */
  public Units getUnit() {
    return unit;
  }

  /**
   * Constructs a default OzoneRecoverWindow object.
   */
  public OzoneRecoverWindow() {
    this.windowLength = 0;
    this.unit = Units.UNDEFINED;
  }

  /**
   * Constructor for OzoneRecoverWindow.
   * @param windowLength recover-window length
   * @param unit MIN, HR or DAY
   */
  public OzoneRecoverWindow(long windowLength, Units unit) {
    this.windowLength = windowLength;
    this.unit = unit;
  }

  /**
   * Formats recover-window as a string.
   */
  public static String formatWindow(OzoneRecoverWindow window) {
    return String.valueOf(window.windowLength) + window.unit;
  }

  /**
   * Parses user provided string and returns the OzoneRecoverWindow.
   */
  public static OzoneRecoverWindow parseWindow(String windowString) {

    if ((windowString == null) || (windowString.isEmpty())) {
      throw new IllegalArgumentException(
          "Recover-Window string cannot be null or empty.");
    }

    String uppercase = windowString.toUpperCase().replaceAll("\\s+", "");
    String length = "";
    int nLength;
    Units currUnit = Units.MIN;
    boolean found = false;
    if (uppercase.endsWith(OZONE_RECOVER_WINDOW_MIN)) {
      length = uppercase
          .substring(0, uppercase.length() - OZONE_RECOVER_WINDOW_MIN.length());
      currUnit = Units.MIN;
      found = true;
    }

    if (uppercase.endsWith(OZONE_RECOVER_WINDOW_HR)) {
      length = uppercase
          .substring(0, uppercase.length() - OZONE_RECOVER_WINDOW_HR.length());
      currUnit = Units.HR;
      found = true;
    }

    if (uppercase.endsWith(OZONE_RECOVER_WINDOW_DAY)) {
      length = uppercase
          .substring(0, uppercase.length() - OZONE_RECOVER_WINDOW_DAY.length());
      currUnit = Units.DAY;
      found = true;
    }

    if (!found) {
      throw new IllegalArgumentException("Window-length unit not recognized. " +
          "Supported values are MIN, HR and DAY.");
    }

    nLength = Integer.parseInt(length);
    if (nLength < 0) {
      throw new IllegalArgumentException("Window-length cannot be negative.");
    }

    return new OzoneRecoverWindow(nLength, currUnit);
  }


  /**
   * Returns length in seconds or -1 if there is no Window.
   */
  public long lengthInSeconds() {
    switch (this.unit) {
    case MIN:
      return this.getWindowLength() * OzoneConsts.MIN;
    case HR:
      return this.getWindowLength() * OzoneConsts.HR;
    case DAY:
      return this.getWindowLength() * OzoneConsts.DAY;
    case UNDEFINED:
    default:
      return -1;
    }
  }


  /**
   * Returns OzoneRecoverWindow corresponding to window length in seconds.
   */
  public static OzoneRecoverWindow getOzoneRecoverWindow(long windowInSeconds) {
    long length;
    Units unit;
    if (windowInSeconds % OzoneConsts.DAY == 0) {
      length = windowInSeconds / OzoneConsts.DAY;
      unit = Units.DAY;
    } else if (windowInSeconds % OzoneConsts.HR == 0) {
      length = windowInSeconds / OzoneConsts.HR;
      unit = Units.HR;
    } else if (windowInSeconds % OzoneConsts.MIN == 0) {
      length = windowInSeconds / OzoneConsts.MIN;
      unit = Units.MIN;
    } else {
      length = 0;
      unit = Units.MIN;
    }
    return new OzoneRecoverWindow((int)length, unit);
  }

  @Override
  public String toString() {
    return windowLength + " " + unit;
  }
}
