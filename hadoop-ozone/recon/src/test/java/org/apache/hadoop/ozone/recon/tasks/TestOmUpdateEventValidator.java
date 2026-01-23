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

import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.initializeNewOmMetadataManager;
import static org.apache.hadoop.ozone.recon.tasks.OMDBUpdateEvent.OMDBUpdateAction.PUT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.codec.OMDBDefinition;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmPrefixInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;

/**
 * Test class for OmUpdateEventValidator.
 */
public class TestOmUpdateEventValidator {

  private OmUpdateEventValidator eventValidator;
  private final OMDBDefinition omdbDefinition = OMDBDefinition.get();
  private OMMetadataManager omMetadataManager;
  private Logger logger;
  @TempDir
  private Path temporaryFolder;

  @BeforeEach
  public void setUp() throws IOException {
    omMetadataManager = initializeNewOmMetadataManager(
        temporaryFolder.toFile());
    eventValidator = new OmUpdateEventValidator(omdbDefinition);
    // Create a mock logger
    logger = mock(Logger.class);
    OmUpdateEventValidator.setLogger(logger);
  }

  @Test
  public void testValidEvents() throws IOException {
    // Validate a valid event for KeyTable
    assertTrue(eventValidator.isValidEvent(
        omMetadataManager.getKeyTable(BucketLayout.LEGACY).getName(),
        mock(OmKeyInfo.class), "key1", PUT));

    // Validate a valid event for BucketTable
    assertTrue(eventValidator.isValidEvent(
        omMetadataManager.getBucketTable().getName(),
        mock(OmBucketInfo.class), "key1", PUT));

    // Validate a valid event for DeletedTable
    assertTrue(eventValidator.isValidEvent(
        omMetadataManager.getDeletedTable().getName(),
        mock(RepeatedOmKeyInfo.class), "key1", PUT));

    // Validate a valid event for Prefix table
    assertTrue(eventValidator.isValidEvent(
        omMetadataManager.getPrefixTable().getName(),
        mock(OmPrefixInfo.class), "key1", PUT));

    // Validate a valid event for SnapshotInfo table
    assertTrue(eventValidator.isValidEvent(
        omMetadataManager.getSnapshotInfoTable().getName(),
        mock(SnapshotInfo.class), "key1", PUT));

    // Verify that no log message is printed
    verify(logger, never()).warn(anyString());
  }

  @Test
  public void testInvalidEvents() throws IOException {

    // Validate an invalid event for VolumeInfo table
    assertFalse(eventValidator.isValidEvent(
        omMetadataManager.getVolumeTable().getName(),
        "Invalid Object", "key1", PUT));

    // Validate an invalid event for BucketTable
    assertFalse(eventValidator.isValidEvent(
        omMetadataManager.getBucketTable().getName(),
        "Invalid Object", "key1", PUT));

    // Validate an invalid event for DeletedTable
    assertFalse(eventValidator.isValidEvent(
        omMetadataManager.getDeletedTable().getName(),
        "Invalid Object", "key1", PUT));

    // Validate an invalid event for Prefix table
    assertFalse(eventValidator.isValidEvent(
        omMetadataManager.getPrefixTable().getName(),
        "Invalid Object", "key1", PUT));

    // Validate an invalid event for SnapshotInfo table
    assertFalse(eventValidator.isValidEvent(
        omMetadataManager.getSnapshotInfoTable().getName(),
        "Invalid Object", "key1", PUT));
    // Verify that the logger is called 5 times
    verifyLogMessage(logger);
  }

  private void verifyLogMessage(Logger localLogger) {
    // Use ArgumentCaptor to capture the log message
    ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
    verify(localLogger, times(5)).warn(captor.capture());

    // Assert that the captured log messages are not empty
    List<String> logMessages = captor.getAllValues();
    for (String logMessage : logMessages) {
      assertThat(logMessage).isNotEmpty();
    }
  }

}
