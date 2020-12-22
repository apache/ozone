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
import org.junit.After;
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

  private static final int TEST_INDEX = 5;
  private OzoneManagerPrepareState prepareState;

  @Before
  public void setup() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS,
        folder.getRoot().getAbsolutePath());

    prepareState = new OzoneManagerPrepareState(conf);
  }

  @After
  public void tearDown() throws Exception {
    // Clean up marker file from previous runs.
    prepareState.cancelPrepare();
  }

  @Test
  public void testStartPrepare() {
    assertPrepareNotStarted();
    prepareState.enablePrepareGate();
    assertPrepareInProgress();
  }

  @Test
  public void testFinishPrepare() throws Exception {
    try {
      prepareState.finishPrepare(TEST_INDEX);
      Assert.fail("OMException should have been thrown when finishing prepare" +
          " without starting.");
    } catch (OMException ex) {
      Assert.assertEquals(OMException.ResultCodes.INTERNAL_ERROR,
          ex.getResult());
    }

    prepareState.enablePrepareGate();
    prepareState.finishPrepare(TEST_INDEX);
    assertPrepareCompleted(TEST_INDEX);
  }

  @Test
  public void testCancelPrepare() throws Exception {
    // Test cancel after start.
    prepareState.enablePrepareGate();
    prepareState.cancelPrepare();
    assertPrepareNotStarted();

    // Test cancel after finish.
    prepareState.enablePrepareGate();
    prepareState.finishPrepare(TEST_INDEX);
    prepareState.cancelPrepare();
    assertPrepareNotStarted();

    // Test cancel after cancel.
    prepareState.cancelPrepare();
    prepareState.cancelPrepare();
    assertPrepareNotStarted();

  }

  @Test
  public void testRequestAllowed() {
    // When not prepared, all requests should be allowed.
    for (Type cmdType: Type.values()) {
      Assert.assertTrue(prepareState.requestAllowed(cmdType));
    }

    prepareState.enablePrepareGate();

    // Once preparation has begun, only prepare and cancel prepare should be
    // allowed.
    for (Type cmdType: Type.values()) {
      if (cmdType == Type.Prepare) {
        // TODO: Add cancelPrepare to allowed request types when it is added.
        Assert.assertTrue(prepareState.requestAllowed(cmdType));
      } else {
        Assert.assertFalse(prepareState.requestAllowed(cmdType));
      }
    }
  }

  @Test
  public void testRestoreCorrectIndex() throws Exception {
    writePrepareMarkerFile(TEST_INDEX);
    PrepareStatus status = prepareState.restorePrepare(TEST_INDEX);
    Assert.assertEquals(PrepareStatus.PREPARE_COMPLETED, status);

    assertPrepareCompleted(TEST_INDEX);
  }

  @Test
  public void testRestoreIncorrectIndex() throws Exception {
    writePrepareMarkerFile(TEST_INDEX);
    PrepareStatus status = prepareState.restorePrepare(TEST_INDEX + 1);
    Assert.assertEquals(PrepareStatus.PREPARE_NOT_STARTED, status);

    assertPrepareNotStarted();
  }

  @Test
  public void testRestoreGarbageMarkerFile() throws Exception {
    byte[] randomBytes = new byte[10];
    new Random().nextBytes(randomBytes);
    writePrepareMarkerFile(randomBytes);

    PrepareStatus status = prepareState.restorePrepare(TEST_INDEX);
    Assert.assertEquals(PrepareStatus.PREPARE_NOT_STARTED, status);

    assertPrepareNotStarted();
  }

  @Test
  public void testRestoreEmptyMarkerFile() throws Exception {
    writePrepareMarkerFile(new byte[]{});

    PrepareStatus status = prepareState.restorePrepare(TEST_INDEX);
    Assert.assertEquals(PrepareStatus.PREPARE_NOT_STARTED, status);

    assertPrepareNotStarted();
  }

  @Test
  public void testRestoreNoMarkerFile() throws Exception {
    PrepareStatus status = prepareState.restorePrepare(TEST_INDEX);
    Assert.assertEquals(PrepareStatus.PREPARE_NOT_STARTED, status);

    assertPrepareNotStarted();
  }

  @Test
  public void testRestoreAfterStart() throws Exception {
    prepareState.enablePrepareGate();

    // If prepare is started but never finished, no marker file is written to
    // disk, and restoring the prepare state on an OM restart should leave it
    // not prepared.
    PrepareStatus status = prepareState.restorePrepare(TEST_INDEX);
    Assert.assertEquals(PrepareStatus.PREPARE_NOT_STARTED, status);
    assertPrepareNotStarted();
  }

  @Test
  public void testMultipleRestores() throws Exception {
    PrepareStatus status = prepareState.restorePrepare(TEST_INDEX);
    Assert.assertEquals(PrepareStatus.PREPARE_NOT_STARTED, status);
    assertPrepareNotStarted();

    status = prepareState.restorePrepare(TEST_INDEX);
    Assert.assertEquals(PrepareStatus.PREPARE_NOT_STARTED, status);
    assertPrepareNotStarted();

    prepareState.enablePrepareGate();
    prepareState.finishPrepare(TEST_INDEX);

    status = prepareState.restorePrepare(TEST_INDEX);
    Assert.assertEquals(PrepareStatus.PREPARE_COMPLETED, status);
    assertPrepareCompleted(TEST_INDEX);

    status = prepareState.restorePrepare(TEST_INDEX);
    Assert.assertEquals(PrepareStatus.PREPARE_COMPLETED, status);
    assertPrepareCompleted(TEST_INDEX);
  }

  private void writePrepareMarkerFile(long index) throws IOException {
    writePrepareMarkerFile(Long.toString(index).getBytes());
  }

  private void writePrepareMarkerFile(byte[] bytes) throws IOException {
    File markerFile = prepareState.getPrepareMarkerFile();
    markerFile.getParentFile().mkdirs();
    try(FileOutputStream stream =
            new FileOutputStream(markerFile)) {
      stream.write(bytes);
    }
  }

  private long readPrepareMarkerFile() throws Exception {
    long index = 0;
    File prepareMarkerFile = prepareState.getPrepareMarkerFile();
    byte[] data = new byte[(int) prepareMarkerFile.length()];

    try (FileInputStream stream = new FileInputStream(prepareMarkerFile)) {
      stream.read(data);
      index = Long.parseLong(new String(data));
    }

    return index;
  }

  private void assertPrepareNotStarted() {
    Assert.assertEquals(PrepareStatus.PREPARE_NOT_STARTED,
        prepareState.getStatus());
    Assert.assertEquals(OzoneManagerPrepareState.NO_PREPARE_INDEX,
        prepareState.getPrepareIndex());
    Assert.assertFalse(prepareState.getPrepareMarkerFile().exists());
  }

  private void assertPrepareInProgress() {
    Assert.assertEquals(PrepareStatus.PREPARE_IN_PROGRESS,
        prepareState.getStatus());
    Assert.assertEquals(OzoneManagerPrepareState.NO_PREPARE_INDEX,
        prepareState.getPrepareIndex());
    Assert.assertFalse(prepareState.getPrepareMarkerFile().exists());
  }

  private void assertPrepareCompleted(long index) throws Exception {
    Assert.assertEquals(PrepareStatus.PREPARE_COMPLETED,
        prepareState.getStatus());
    Assert.assertEquals(index, prepareState.getPrepareIndex());
    Assert.assertEquals(index, readPrepareMarkerFile());
  }
}
