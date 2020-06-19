package org.apache.hadoop.ozone.om.response;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.hdds.utils.db.DBConfigFromFile;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.reflections.Reflections;

import java.io.File;
import java.util.Set;

public class TestCleanupTableInfo {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void checkAnnotationAndTableName() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    File newFolder = folder.newFolder();
    if (!newFolder.exists()) {
      Assert.assertTrue(newFolder.mkdirs());
    }
    ServerUtils.setOzoneMetaDirPath(conf, newFolder.toString());
    OMMetadataManager omMetadataManager = new OmMetadataManagerImpl(conf);

    Set<String> tables = omMetadataManager.listTableNames();
    Reflections reflections = new Reflections("org.apache.hadoop.ozone.om.response");
    Set<Class<? extends OMClientResponse>> subTypes =
        reflections.getSubTypesOf(OMClientResponse.class);
    subTypes.forEach(aClass -> {
      Assert.assertTrue(aClass.isAnnotationPresent(CleanupTableInfo.class));
      String[] cleanupTables =
          aClass.getAnnotation(CleanupTableInfo.class).cleanupTables();
      Assert.assertTrue(cleanupTables.length >=1);
      for (String tableName : cleanupTables) {
        Assert.assertTrue(tables.contains(tableName));
      }
    });
  }
}
