package org.apache.hadoop.ozone.container.common;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class CleanUpManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(CleanUpManager.class);

  private final KeyValueContainerData keyValueContainerData;
  private final ConfigurationSource configurationSource;

  private DatanodeConfiguration datanodeConf;

  public CleanUpManager(KeyValueContainerData keyValueContainerData,
                        ConfigurationSource configurationSource) {
    this.keyValueContainerData = keyValueContainerData;
    this.configurationSource = configurationSource;
    this.datanodeConf =
        configurationSource.getObject(DatanodeConfiguration.class);
//    datanodeConf.setDiskTmpDirectoryPath("/home/xbis/tmpDir");
  }

  public DatanodeConfiguration getDatanodeConf() {
    return datanodeConf;
  }

  public boolean checkContainerSchemaV3Enabled() {
    if (keyValueContainerData.getSchemaVersion()
        .equals(OzoneConsts.SCHEMA_V3)) {
      return true;
    } else {
      return false;
    }
  }

  public void renameDir() throws IOException {
    String tmpDirPath = datanodeConf.getDiskTmpDirectoryPath();

    String containerPath = keyValueContainerData.getContainerPath();
    File container = new File(containerPath);

    String containerDirName = container.getName();

    String destinationDirPath = tmpDirPath + "/" + containerDirName;

    try {
      FileUtils.moveDirectory(container, new File(destinationDirPath));
    } catch (IOException ex) {
      LOG.error("Error while moving metadata and chunks under Tmp volume", ex);
    }

    keyValueContainerData.setMetadataPath(destinationDirPath + "/metadata");
    keyValueContainerData.setChunksPath(destinationDirPath + "/chunks");
  }

  public boolean checkTmpDirIsEmpty() throws IOException {
    String tmpDirPath = datanodeConf.getDiskTmpDirectoryPath();
    File tmpDir = new File(tmpDirPath);

    try (Stream<Path> entries = Files.list(tmpDir.toPath())) {
      return !entries.findFirst().isPresent();
    }
  }

  public List<String> getDeleteLeftovers() {
    List<String> leftovers = new ArrayList<String>();
    String tmpDirPath = datanodeConf.getDiskTmpDirectoryPath();
    File tmpDir = new File(tmpDirPath);

    for (File file : tmpDir.listFiles()) {
      String fileName = file.getName();
      leftovers.add(fileName);
    }
    return leftovers;
  }

  public void cleanTmpDir() throws IOException {
    String tmpDirPath = datanodeConf.getDiskTmpDirectoryPath();
    File tmpDir = new File(tmpDirPath);
    for (File file : tmpDir.listFiles()) {
      FileUtils.deleteDirectory(file);
    }
  }

  public void deleteTmpDir() throws IOException {
    String tmpDirPath = datanodeConf.getDiskTmpDirectoryPath();
    File tmpDir = new File(tmpDirPath);
    FileUtils.deleteDirectory(tmpDir);
  }
}
