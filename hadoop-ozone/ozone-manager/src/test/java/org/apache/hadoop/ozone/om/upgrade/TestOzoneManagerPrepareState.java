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

package org.apache.hadoop.ozone.om.upgrade;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OzoneManagerPrepareState;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PrepareStatusResponse.PrepareStatus;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.ozone.test.LambdaTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Class to test Ozone Manager prepare state maintenance.
 */
public class TestOzoneManagerPrepareState {
  @TempDir
  private Path folder;

  private static final int TEST_INDEX = 5;
  private OzoneManagerPrepareState prepareState;
  private static final Random RANDOM = new Random();

  @BeforeEach
  public void setup() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS,
        folder.toAbsolutePath().toString());

    prepareState = new OzoneManagerPrepareState(conf);
  }

  @AfterEach
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
    // Test finish as a follower OM, where it would be called without setting
    // the prepare gate first.
    prepareState.finishPrepare(TEST_INDEX);
    assertPrepareCompleted(TEST_INDEX);
    prepareState.cancelPrepare();

    // Test finish as a leader OM, where the prepare gate would be put up
    // before prepare is finished.
    prepareState.enablePrepareGate();
    prepareState.finishPrepare(TEST_INDEX);
    assertPrepareCompleted(TEST_INDEX);

    // A following prepare operation should not raise errors.
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
    assertPrepareGateDown();
    prepareState.enablePrepareGate();
    assertPrepareGateUp();
  }

  @Test
  public void testRestoreCorrectIndex() throws Exception {
    writePrepareMarkerFile(TEST_INDEX);
    prepareState.restorePrepareFromFile(TEST_INDEX);
    assertPrepareCompleted(TEST_INDEX);
  }

  @Test
  public void testRestoreIncorrectIndex() throws Exception {
    writePrepareMarkerFile(TEST_INDEX);
    // Ratis is allowed to apply transactions after prepare, like commit and
    // conf entries, but OM write requests are not allowed.
    // Therefore prepare restoration should only fail if the marker file index
    // is less than the OM's txn index.
    assertPrepareFailedException(() ->
        prepareState.restorePrepareFromFile(TEST_INDEX - 1));
  }

  @Test
  public void testRestoreGarbageMarkerFile() throws Exception {
    byte[] randomBytes = new byte[10];
    RANDOM.nextBytes(randomBytes);
    writePrepareMarkerFile(randomBytes);

    assertPrepareFailedException(() ->
        prepareState.restorePrepareFromFile(TEST_INDEX));
  }

  @Test
  public void testRestoreEmptyMarkerFile() throws Exception {
    writePrepareMarkerFile(new byte[]{});

    assertPrepareFailedException(() ->
        prepareState.restorePrepareFromFile(TEST_INDEX));
  }

  @Test
  public void testRestoreNoMarkerFile() throws Exception {
    assertPrepareFailedException(() ->
        prepareState.restorePrepareFromFile(TEST_INDEX));
    assertPrepareNotStarted();
  }

  @Test
  public void testRestoreWithGateOnly() throws Exception {
    prepareState.enablePrepareGate();

    // If prepare is started but never finished, (gate is up but no marker
    // file written), then restoring the prepare state from the file should
    // fail, but leave the in memory state the same.
    assertPrepareFailedException(() ->
        prepareState.restorePrepareFromFile(TEST_INDEX));
    assertPrepareInProgress();
  }

  @Test
  public void testMultipleRestores() throws Exception {
    assertPrepareFailedException(() ->
        prepareState.restorePrepareFromFile(TEST_INDEX));
    assertPrepareNotStarted();

    assertPrepareFailedException(() ->
        prepareState.restorePrepareFromFile(TEST_INDEX));
    assertPrepareNotStarted();

    prepareState.enablePrepareGate();
    prepareState.finishPrepare(TEST_INDEX);

    prepareState.restorePrepareFromFile(TEST_INDEX);
    assertPrepareCompleted(TEST_INDEX);

    prepareState.restorePrepareFromFile(TEST_INDEX);
    assertPrepareCompleted(TEST_INDEX);
  }

  private void writePrepareMarkerFile(long index) throws IOException {
    writePrepareMarkerFile(Long.toString(index).getBytes(
        Charset.defaultCharset()));
  }

  private void writePrepareMarkerFile(byte[] bytes) throws IOException {
    File markerFile = prepareState.getPrepareMarkerFile();
    boolean mkdirs = markerFile.getParentFile().mkdirs();
    if (!mkdirs) {
      throw new IOException("Unable to create marker file directory.");
    }
    try (OutputStream stream =
             Files.newOutputStream(markerFile.toPath())) {
      stream.write(bytes);
    }
  }

  private long readPrepareMarkerFile() throws Exception {
    long index = 0;
    File prepareMarkerFile = prepareState.getPrepareMarkerFile();
    byte[] data = new byte[(int) prepareMarkerFile.length()];

    try (InputStream stream = Files.newInputStream(prepareMarkerFile.toPath())) {
      stream.read(data);
      index = Long.parseLong(new String(data, Charset.defaultCharset()));
    }

    return index;
  }

  private void assertPrepareNotStarted() {
    OzoneManagerPrepareState.State state = prepareState.getState();
    assertEquals(PrepareStatus.NOT_PREPARED, state.getStatus());
    assertEquals(OzoneManagerPrepareState.NO_PREPARE_INDEX,
        state.getIndex());
    assertFalse(prepareState.getPrepareMarkerFile().exists());

    assertPrepareGateDown();
  }

  private void assertPrepareInProgress() {
    OzoneManagerPrepareState.State state = prepareState.getState();
    assertEquals(PrepareStatus.PREPARE_GATE_ENABLED,
        state.getStatus());
    assertEquals(OzoneManagerPrepareState.NO_PREPARE_INDEX,
        state.getIndex());
    assertFalse(prepareState.getPrepareMarkerFile().exists());

    assertPrepareGateUp();
  }

  private void assertPrepareCompleted(long index) throws Exception {
    OzoneManagerPrepareState.State state = prepareState.getState();
    assertEquals(PrepareStatus.PREPARE_COMPLETED,
        state.getStatus());
    assertEquals(index, state.getIndex());
    assertEquals(index, readPrepareMarkerFile());

    assertPrepareGateUp();
  }

  private void assertPrepareGateUp() {
    // Once preparation has begun, only prepare and cancel prepare should be
    // allowed.
    for (Type cmdType: Type.values()) {
      if (cmdType == Type.Prepare || cmdType == Type.CancelPrepare) {
        assertTrue(prepareState.requestAllowed(cmdType));
      } else {
        assertFalse(prepareState.requestAllowed(cmdType));
      }
    }
  }

  private void assertPrepareGateDown() {
    for (Type cmdType: Type.values()) {
      assertTrue(prepareState.requestAllowed(cmdType));
    }
  }

  private void assertPrepareFailedException(LambdaTestUtils.VoidCallable call)
      throws Exception {
    try {
      call.call();
    } catch (OMException ex) {
      if (ex.getResult() != OMException.ResultCodes.PREPARE_FAILED) {
        throw ex;
      }
    }
  }
}
