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

package org.apache.hadoop.ozone.client.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;

/**
 * Exception used to indicate a problem with a specific block location, allowing
 * the failed location to be communicated back to the caller.
 */
public class BadDataLocationException extends IOException {

  private final List<DatanodeDetails> failedLocations = new ArrayList<>();
  private int failedLocationIndex;

  /**
   * Required for Unwrapping {@code RemoteException}. Used by
   * {@link org.apache.hadoop.ipc_.RemoteException#unwrapRemoteException()}
   */
  public BadDataLocationException(String message) {
    super(message);
  }

  public BadDataLocationException(DatanodeDetails dn) {
    super();
    failedLocations.add(dn);
  }

  public BadDataLocationException(DatanodeDetails dn, String message) {
    super(message);
    failedLocations.add(dn);
  }

  public BadDataLocationException(DatanodeDetails dn, String message,
      Throwable ex) {
    super(message, ex);
    failedLocations.add(dn);
  }

  public BadDataLocationException(DatanodeDetails dn, Throwable ex) {
    super(ex);
    failedLocations.add(dn);
  }

  public BadDataLocationException(int failedIndex,
      Throwable ex, List<DatanodeDetails> failedLocations) {
    super(ex);
    failedLocationIndex = failedIndex;
    this.failedLocations.addAll(failedLocations);
  }

  public BadDataLocationException(DatanodeDetails dn, int failedIndex,
      Throwable ex) {
    super(ex);
    failedLocations.add(dn);
    failedLocationIndex = failedIndex;
  }

  public List<DatanodeDetails> getFailedLocations() {
    return failedLocations;
  }

  public void addFailedLocations(List<DatanodeDetails> dns) {
    failedLocations.addAll(dns);
  }

  public int getFailedLocationIndex() {
    return failedLocationIndex;
  }
}
