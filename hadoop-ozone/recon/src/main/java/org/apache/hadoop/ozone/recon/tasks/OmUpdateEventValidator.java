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

package org.apache.hadoop.ozone.recon.tasks;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.ozone.om.codec.OMDBDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OmUpdateEventValidator is a utility class for validating OMDBUpdateEvents
 * It can be further extended to different types of validations.
 */
public class OmUpdateEventValidator {

  private static Logger log =
      LoggerFactory.getLogger(OmUpdateEventValidator.class);
  private OMDBDefinition omdbDefinition;

  public OmUpdateEventValidator(OMDBDefinition omdbDefinition) {
    this.omdbDefinition = omdbDefinition;
  }

  /**
   * Validates the OMDBUpdateEvent based on the expected value type for a
   * given table.
   *
   * @param tableName        the name of the table associated with the event.
   * @param actualValueType  the actual value type of the event.
   * @param keyType          the key type of the event.
   * @param action           the action performed on the event.
   * @return true if the event is valid, false otherwise.
   */
  public boolean isValidEvent(String tableName,
                              Object actualValueType,
                              Object keyType,
                              OMDBUpdateEvent.OMDBUpdateAction action) {

    String expectedValueTypeString =
        omdbDefinition.getColumnFamily(tableName).getValueType().getName();
    String actualValueTypeString = actualValueType.getClass().getName();

    // Check if both objects are of the same type
    if (expectedValueTypeString.equals(actualValueTypeString)) {
      // Both objects are of the same type
      return true;
    }
    // Objects are not of the same type
    logWarn(keyType.toString(), tableName, action.toString(),
        expectedValueTypeString,
        actualValueTypeString);
    return false;
  }

  /**
   * Logs an warning message indicating a validation failure.
   */
  private void logWarn(String keyType, String tableName, String action,
                        String expectedValueType, String actualValueType) {
    String warnMessage = String.format(
        "Validation failed for keyType: %s, tableName: %s, action: %s, " +
            "Expected value type: %s, Actual value type: %s",
        keyType, tableName, action, expectedValueType, actualValueType);
    // Log the warning message as an WARN level log
    log.warn(warnMessage);
  }

  @VisibleForTesting
  public static void setLogger(Logger logger) {
    OmUpdateEventValidator.log = logger;
  }

}
