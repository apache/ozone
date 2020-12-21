/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.om.upgrade;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PrepareStatusResponse.PrepareStatus;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OzoneManagerPrepareState;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;

public class TestOzoneManagerPrepareState {
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private int testIndex;
  private OzoneConfiguration conf;

  @Before
  public void setup() throws Exception {
    testIndex = 5;
    conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS,
        folder.getRoot().getAbsolutePath());

    // Clean up marker file from previous runs.
    OzoneManagerPrepareState.cancelPrepare(conf);
  }

  @Test
  public void testStartPrepare() {
    assertPrepareNotStarted();
    OzoneManagerPrepareState.startPrepare();
    assertPrepareInProgress();
  }

  @Test
  public void testFinishPrepare() throws Exception {
    try {
      OzoneManagerPrepareState.finishPrepare(conf, testIndex);
      Assert.fail("OMException should have been thrown when finishing prepare" +
          " without starting.");
    } catch (OMException ex) {
      Assert.assertEquals(OMException.ResultCodes.INTERNAL_ERROR,
          ex.getResult());
    }

    OzoneManagerPrepareState.startPrepare();
    OzoneManagerPrepareState.finishPrepare(conf, testIndex);
    assertPrepareCompleted(testIndex);
  }

  @Test
  public void testCancelPrepare() throws Exception {
    // Test cancel after start.
    OzoneManagerPrepareState.startPrepare();
    OzoneManagerPrepareState.cancelPrepare(conf);
    assertPrepareNotStarted();

    // Test cancel after finish.
    OzoneManagerPrepareState.startPrepare();
    OzoneManagerPrepareState.finishPrepare(conf, testIndex);
    OzoneManagerPrepareState.cancelPrepare(conf);
    assertPrepareNotStarted();

    // Test cancel after cancel.
    OzoneManagerPrepareState.cancelPrepare(conf);
    OzoneManagerPrepareState.cancelPrepare(conf);
    assertPrepareNotStarted();

  }

  @Test
  public void testRequestAllowed() {
    // When not prepared, all requests should be allowed.
    for (Type cmdType: Type.values()) {
      Assert.assertTrue(OzoneManagerPrepareState.requestAllowed(cmdType));
    }

    OzoneManagerPrepareState.startPrepare();

    // Once preparation has begun, only prepare and cancel prepare should be
    // allowed.
    for (Type cmdType: Type.values()) {
      if (cmdType == Type.Prepare) {
        // TODO: Add cancelPrepare to allowed request types when it is added.
        Assert.assertTrue(OzoneManagerPrepareState.requestAllowed(cmdType));
      } else {
        Assert.assertFalse(OzoneManagerPrepareState.requestAllowed(cmdType));
      }
    }
  }

  @Test
  public void testRestoreCorrectIndex() throws Exception {
    writePrepareMarkerFile(testIndex);
    PrepareStatus status = OzoneManagerPrepareState.restorePrepare(conf,
        testIndex);
    Assert.assertEquals(PrepareStatus.PREPARE_COMPLETED, status);

    assertPrepareCompleted(testIndex);
  }

  @Test
  public void testRestoreIncorrectIndex() throws Exception {
    writePrepareMarkerFile(testIndex);
    PrepareStatus status = OzoneManagerPrepareState.restorePrepare(conf,
        testIndex + 1);
    Assert.assertEquals(PrepareStatus.PREPARE_NOT_STARTED, status);

    assertPrepareNotStarted();
  }

  @Test
  public void testRestoreGarbageMarkerFile() throws Exception {
    byte[] randomBytes = new byte[10];
    new Random().nextBytes(randomBytes);
    writePrepareMarkerFile(randomBytes);

    PrepareStatus status = OzoneManagerPrepareState.restorePrepare(conf,
        testIndex);
    Assert.assertEquals(PrepareStatus.PREPARE_NOT_STARTED, status);

    assertPrepareNotStarted();
  }

  @Test
  public void testRestoreEmptyMarkerFile() throws Exception {
    writePrepareMarkerFile(new byte[]{});

    PrepareStatus status = OzoneManagerPrepareState.restorePrepare(conf,
        testIndex);
    Assert.assertEquals(PrepareStatus.PREPARE_NOT_STARTED, status);

    assertPrepareNotStarted();
  }

  @Test
  public void testRestoreNoMarkerFile() throws Exception {
    PrepareStatus status = OzoneManagerPrepareState.restorePrepare(conf,
        testIndex);
    Assert.assertEquals(PrepareStatus.PREPARE_NOT_STARTED, status);

    assertPrepareNotStarted();
  }

  @Test
  public void testRestoreAfterStart() throws Exception {
    OzoneManagerPrepareState.startPrepare();

    // If prepare is started but never finished, no marker file is written to
    // disk, and restoring the prepare state on an OM restart should leave it
    // not prepared.
    PrepareStatus status = OzoneManagerPrepareState.restorePrepare(conf,
        testIndex);
    Assert.assertEquals(PrepareStatus.PREPARE_NOT_STARTED, status);
    assertPrepareNotStarted();
  }

  @Test
  public void testMultipleRestores() throws Exception {
    PrepareStatus status = OzoneManagerPrepareState.restorePrepare(conf,
        testIndex);
    Assert.assertEquals(PrepareStatus.PREPARE_NOT_STARTED, status);
    assertPrepareNotStarted();

    status = OzoneManagerPrepareState.restorePrepare(conf, testIndex);
    Assert.assertEquals(PrepareStatus.PREPARE_NOT_STARTED, status);
    assertPrepareNotStarted();

    OzoneManagerPrepareState.startPrepare();
    OzoneManagerPrepareState.finishPrepare(conf, testIndex);

    status = OzoneManagerPrepareState.restorePrepare(conf, testIndex);
    Assert.assertEquals(PrepareStatus.PREPARE_COMPLETED, status);
    assertPrepareCompleted(testIndex);

    status = OzoneManagerPrepareState.restorePrepare(conf, testIndex);
    Assert.assertEquals(PrepareStatus.PREPARE_COMPLETED, status);
    assertPrepareCompleted(testIndex);
  }

  private void writePrepareMarkerFile(long index) throws IOException {
    writePrepareMarkerFile(Long.toString(index).getBytes());
  }

  private void writePrepareMarkerFile(byte[] bytes) throws IOException {
    File markerFile = OzoneManagerPrepareState.getPrepareMarkerFile(conf);
    markerFile.getParentFile().mkdirs();
    try(FileOutputStream stream =
            new FileOutputStream(markerFile)) {
      stream.write(bytes);
    }
  }

  private long readPrepareMarkerFile() throws Exception {
    long index = 0;
    File prepareMarkerFile = OzoneManagerPrepareState.
        getPrepareMarkerFile(conf);
    byte[] data = new byte[(int) prepareMarkerFile.length()];

    try (FileInputStream stream = new FileInputStream(prepareMarkerFile)) {
      stream.read(data);
      index = Long.parseLong(new String(data));
    }

    return index;
  }

  private void assertPrepareInProgress() {
    Assert.assertTrue(OzoneManagerPrepareState.isPrepareGateEnabled());
    Assert.assertEquals(PrepareStatus.PREPARE_IN_PROGRESS,
        OzoneManagerPrepareState.getStatus());
    Assert.assertFalse(OzoneManagerPrepareState
        .getPrepareMarkerFile(conf).exists());
  }

  private void assertPrepareNotStarted() {
    Assert.assertFalse(OzoneManagerPrepareState.isPrepareGateEnabled());
    Assert.assertEquals(PrepareStatus.PREPARE_NOT_STARTED,
        OzoneManagerPrepareState.getStatus());
    Assert.assertEquals(OzoneManagerPrepareState.NO_PREPARE_INDEX,
        OzoneManagerPrepareState.getPrepareIndex());
    Assert.assertFalse(OzoneManagerPrepareState
        .getPrepareMarkerFile(conf).exists());
  }

  private void assertPrepareCompleted(long index) throws Exception {
    Assert.assertEquals(PrepareStatus.PREPARE_COMPLETED,
        OzoneManagerPrepareState.getStatus());
    Assert.assertTrue(OzoneManagerPrepareState.isPrepareGateEnabled());
    Assert.assertEquals(index, OzoneManagerPrepareState.getPrepareIndex());
    Assert.assertEquals(index, readPrepareMarkerFile());
  }
}
