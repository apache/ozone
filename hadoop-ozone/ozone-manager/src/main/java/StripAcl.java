
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import java.io.IOException;
import java.util.ArrayList;

public class StripAcl {

  OzoneConfiguration conf;
  OMMetadataManager metadataStore;

  public StripAcl(String dbPath) throws IOException {
    this.conf = new OzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_OM_DB_DIRS, dbPath);
    this.metadataStore = new OmMetadataManagerImpl(conf, null);
    this.metadataStore.start(conf);
  }

  private void updateDirectoryTable() throws IOException {
    Table<String, OmDirectoryInfo> dirTable = metadataStore.getDirectoryTable();
    int count = 0;
    try (TableIterator<String,
        ? extends Table.KeyValue<String, OmDirectoryInfo>>
             iterator = dirTable.iterator()) {
      BatchOperation writeBatch =
               metadataStore.getStore().initBatchOperation();
      while (iterator.hasNext()) {
        Table.KeyValue<String, OmDirectoryInfo> keyValue = iterator.next();
        OmDirectoryInfo dirInfo = keyValue.getValue();
        OmDirectoryInfo copy = new OmDirectoryInfo.Builder()
            .setParentObjectID(dirInfo.getParentObjectID())
            .setObjectID(dirInfo.getObjectID())
            .setUpdateID(dirInfo.getUpdateID())
            .setName(dirInfo.getName())
            .setCreationTime(dirInfo.getCreationTime())
            .setModificationTime(dirInfo.getModificationTime())
            .setAcls(new ArrayList<>())
            .addAllMetadata(dirInfo.getMetadata()).build();
        dirTable.putWithBatch(writeBatch, keyValue.getKey(), copy);
        count++;
        if (count % (100*1000) == 0) {
          if (count % (1000*1000) == 0) {
            System.err.println("Processed " + count + " directories");
          }
          metadataStore.getStore().commitBatchOperation(writeBatch);
          writeBatch.close();
          writeBatch =
              metadataStore.getStore().initBatchOperation();
        }
      }
      metadataStore.getStore().commitBatchOperation(writeBatch);
      writeBatch.close();
      System.err.println("Processed " + count + " directories");
    }
  }

  private void updateDeletedDirTable() throws IOException {
    Table<String, OmKeyInfo> deletedDirTable = metadataStore.getDeletedDirTable();
    System.err.println("Processing deleted directories");
    updateOmKeyInfoTables(deletedDirTable);
  }

  private void updateFileTable() throws IOException {
    Table<String, OmKeyInfo> fileTable = metadataStore.getFileTable();
    System.err.println("Processing files");
    updateOmKeyInfoTables(fileTable);
    Table<String, OmKeyInfo> keyTable = metadataStore.getKeyTable(BucketLayout.OBJECT_STORE);
    System.err.println("Processing keys");
    updateOmKeyInfoTables(keyTable);
  }


  private void updateOpenKeyTable() throws IOException {
    Table<String, OmKeyInfo> table = metadataStore.getOpenKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED);
    System.err.println("Processing FSO open keys");
    updateOmKeyInfoTables(table);
    table = metadataStore.getOpenKeyTable(BucketLayout.OBJECT_STORE);
    System.err.println("Processing OBS open keys");
    updateOmKeyInfoTables(table);
  }

  private void updateOmKeyInfoTables(Table<String, OmKeyInfo> fileTable) throws IOException {
    int count = 0;
    try (TableIterator<String,
        ? extends Table.KeyValue<String, OmKeyInfo>>
             iterator = fileTable.iterator()) {
      BatchOperation writeBatch =
          metadataStore.getStore().initBatchOperation();
      while (iterator.hasNext()) {
        Table.KeyValue<String, OmKeyInfo> keyValue = iterator.next();
        OmKeyInfo keyInfo = keyValue.getValue();
        OmKeyInfo copy = new OmKeyInfo.Builder()
            .setVolumeName(keyInfo.getVolumeName())
            .setBucketName(keyInfo.getBucketName())
            .setKeyName(keyInfo.getKeyName())
            .setFileName(keyInfo.getFileName())
            .setOmKeyLocationInfos(keyInfo.getKeyLocationVersions())
            .setDataSize(keyInfo.getDataSize())
            .setCreationTime(keyInfo.getCreationTime())
            .setModificationTime(keyInfo.getModificationTime())
            .setReplicationConfig(keyInfo.getReplicationConfig())
            .addAllMetadata(keyInfo.getMetadata())
            .setFileEncryptionInfo(keyInfo.getFileEncryptionInfo())

            .setAcls(new ArrayList<>())

            .setParentObjectID(keyInfo.getParentObjectID())
            .setObjectID(keyInfo.getObjectID())
            .setUpdateID(keyInfo.getUpdateID())
            .setFileChecksum(keyInfo.getFileChecksum())
            .setFile(keyInfo.isFile())
            .build();
        fileTable.putWithBatch(writeBatch, keyValue.getKey(), copy);
        count++;
        if (count % (100*1000) == 0) {
          if (count % (1000*1000) == 0) {
            System.err.println("Processed " + count + " files");
          }
          metadataStore.getStore().commitBatchOperation(writeBatch);
          writeBatch.close();
          writeBatch =
              metadataStore.getStore().initBatchOperation();
        }
      }
      metadataStore.getStore().commitBatchOperation(writeBatch);
      writeBatch.close();
      System.err.println("Processed " + count + " files");
    }
  }

  public void stop() throws Exception {
    metadataStore.stop();
  }


  public static void main(String[] args) throws Exception{
    if (args.length < 1) {
      System.err.println("Incorrect argument!");
      System.err.println("Usage:");
      System.err.println("StripAcl <path to om.db>");
      System.exit(2);
    }

    String dbPath = args[0];
    StripAcl updater = new StripAcl(dbPath);
    updater.updateDirectoryTable();
    updater.updateDeletedDirTable();
    updater.updateFileTable();
    updater.updateOpenKeyTable();
    // ignore deletedTable for now
    updater.stop();
  }
}

