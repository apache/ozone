package org.apache.hadoop.ozone.om.upgrade;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OzoneManagerPrepareState;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Random;

public class TestOzoneManagerPrepareState {
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private int testIndex;
  private OzoneConfiguration conf;

  @Before
  public void setup() {
    testIndex = 5;
    conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS,
        folder.getRoot().getAbsolutePath());

    // Clean up from previous runs.
    OzoneManagerPrepareState.setPrepared(false);

    File markerFile = OzoneManagerPrepareState.getPrepareMarkerFile(conf);
    if (markerFile.exists()) {
      markerFile.delete();
    }
  }

  @Test
  public void testGetAndSetPrepare() {
    // Default value should be false.
    Assert.assertFalse(OzoneManagerPrepareState.isPrepared());
    OzoneManagerPrepareState.setPrepared(true);
    Assert.assertTrue(OzoneManagerPrepareState.isPrepared());
  }

  @Test
  public void testRequestAllowed() {
    // When not prepared, all requests should be allowed.
    for (Type cmdType: Type.values()) {
      Assert.assertTrue(OzoneManagerPrepareState.requestAllowed(cmdType));
    }

    OzoneManagerPrepareState.setPrepared(true);

    // Once prepared, only prepare and cancel prepare should be allowed.
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
  public void testCorrectMarkerFileIndex() throws Exception {
    OzoneManagerPrepareState.writePrepareMarkerFile(conf, testIndex);
    OzoneManagerPrepareState.checkPrepareMarkerFile(conf, testIndex);
    Assert.assertTrue(OzoneManagerPrepareState.isPrepared());
  }

  @Test
  public void testIncorrectMarkerFileIndex() throws Exception {
    OzoneManagerPrepareState.setPrepared(true);
    OzoneManagerPrepareState.writePrepareMarkerFile(conf, testIndex);
    Assert.assertTrue(
        OzoneManagerPrepareState.getPrepareMarkerFile(conf).exists());

    OzoneManagerPrepareState.checkPrepareMarkerFile(conf, testIndex + 1);
    Assert.assertFalse(OzoneManagerPrepareState.isPrepared());
  }

  @Test
  public void testGarbageMarkerFile() throws Exception {
    File prepareFile = OzoneManagerPrepareState.getPrepareMarkerFile(conf);
    byte[] randomBytes = new byte[10];
    new Random().nextBytes(randomBytes);

    prepareFile.getParentFile().mkdirs();
    try(FileOutputStream stream =
            new FileOutputStream(prepareFile)) {
      stream.write(randomBytes);
    }

    OzoneManagerPrepareState.setPrepared(true);
    Assert.assertTrue(prepareFile.exists());

    OzoneManagerPrepareState.checkPrepareMarkerFile(conf, testIndex);
    Assert.assertFalse(OzoneManagerPrepareState.isPrepared());
  }

  @Test
  public void testEmptyMarkerFile() throws Exception {
    File prepareFile = OzoneManagerPrepareState.getPrepareMarkerFile(conf);
    prepareFile.getParentFile().mkdirs();
    Assert.assertTrue(prepareFile.createNewFile());
    Assert.assertTrue(prepareFile.exists());

    OzoneManagerPrepareState.setPrepared(true);
    OzoneManagerPrepareState.checkPrepareMarkerFile(conf, testIndex);
    Assert.assertFalse(OzoneManagerPrepareState.isPrepared());
  }

  @Test
  public void testNoMarkerFile() {
    OzoneManagerPrepareState.setPrepared(true);
    Assert.assertFalse(
        OzoneManagerPrepareState.getPrepareMarkerFile(conf).exists());
    OzoneManagerPrepareState.checkPrepareMarkerFile(conf, testIndex);
    Assert.assertFalse(OzoneManagerPrepareState.isPrepared());
  }

  @Test
  public void testOverwritingMarkerFile() throws Exception {
    OzoneManagerPrepareState.setPrepared(true);

    OzoneManagerPrepareState.writePrepareMarkerFile(conf, testIndex);
    Assert.assertTrue(
        OzoneManagerPrepareState.getPrepareMarkerFile(conf).exists());

    OzoneManagerPrepareState.writePrepareMarkerFile(conf, testIndex + 1);
    Assert.assertTrue(
        OzoneManagerPrepareState.getPrepareMarkerFile(conf).exists());

    OzoneManagerPrepareState.checkPrepareMarkerFile(conf, testIndex + 1);
    Assert.assertTrue(OzoneManagerPrepareState.isPrepared());
  }
}
