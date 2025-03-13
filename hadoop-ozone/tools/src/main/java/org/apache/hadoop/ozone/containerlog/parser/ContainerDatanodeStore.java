package org.apache.hadoop.ozone.containerlog.parser;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.DBColumnFamilyDefinition;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.hdds.utils.db.LongCodec;
import org.apache.hadoop.hdds.utils.db.StringCodec;
import java.io.File;
import java.io.IOException;
import java.util.List;

public class ContainerDatanodeStore {
  private static final String CONTAINER_TABLE_NAME = "ContainerLogTable";
  private static final DBColumnFamilyDefinition<Long, List<ContainerInfo>> CONTAINER_LOG_TABLE_COLUMN_FAMILY
      = new DBColumnFamilyDefinition<>(
      CONTAINER_TABLE_NAME,
      LongCodec.get(),
      new GenericInfoCodec<ContainerInfo>()
  );
  private static final String DATANODE_TABLE_NAME = "DatanodeContainerLogTable";
  private static final DBColumnFamilyDefinition<String, List<DatanodeContainerInfo>> DATANODE_CONTAINER_LOG_TABLE_COLUMN_FAMILY
      = new DBColumnFamilyDefinition<>(
      DATANODE_TABLE_NAME,
      StringCodec.get(),
      new GenericInfoCodec<DatanodeContainerInfo>()
  );
  private DBStore containerDbStore = null;
  private ContainerLogTable<Long, List<ContainerInfo>> containerLogTable = null;
  private DatanodeContainerLogTable<String, List<DatanodeContainerInfo>> datanodeContainerLogTable = null;
  private DBStore dbStore = null;

  private DBStore openDb(File dbPath) {

    File dbFile = new File(dbPath, "ContainerDatanodeLogStore.db");

    try {

      ConfigurationSource conf = new OzoneConfiguration();
      DBStoreBuilder dbStoreBuilder = DBStoreBuilder.newBuilder(conf);
      dbStoreBuilder.setName("ContainerDatanodeLogStore.db");
      dbStoreBuilder.setPath(dbFile.toPath());

      dbStoreBuilder.addTable(CONTAINER_LOG_TABLE_COLUMN_FAMILY.getName());
      dbStoreBuilder.addTable(DATANODE_CONTAINER_LOG_TABLE_COLUMN_FAMILY.getName());
      dbStoreBuilder.addCodec(CONTAINER_LOG_TABLE_COLUMN_FAMILY.getKeyType(),
          CONTAINER_LOG_TABLE_COLUMN_FAMILY.getKeyCodec());
      dbStoreBuilder.addCodec(CONTAINER_LOG_TABLE_COLUMN_FAMILY.getValueType(),
          CONTAINER_LOG_TABLE_COLUMN_FAMILY.getValueCodec());
      dbStoreBuilder.addCodec(DATANODE_CONTAINER_LOG_TABLE_COLUMN_FAMILY.getKeyType(),
          DATANODE_CONTAINER_LOG_TABLE_COLUMN_FAMILY.getKeyCodec());
      dbStoreBuilder.addCodec(DATANODE_CONTAINER_LOG_TABLE_COLUMN_FAMILY.getValueType(),
          DATANODE_CONTAINER_LOG_TABLE_COLUMN_FAMILY.getValueCodec());

      dbStore = dbStoreBuilder.build();

      containerLogTable = new ContainerLogTable<>(dbStore.getTable(CONTAINER_LOG_TABLE_COLUMN_FAMILY.getName(),
          Long.class, new GenericInfoCodec<ContainerInfo>().getTypeClass()));
      datanodeContainerLogTable = new DatanodeContainerLogTable<>(dbStore.getTable(DATANODE_CONTAINER_LOG_TABLE_COLUMN_FAMILY.getName(),
          String.class, new GenericInfoCodec<DatanodeContainerInfo>().getTypeClass()));

      return dbStore;
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }

  public void initialize(File dbFile) {

    containerDbStore=openDb(dbFile);
  }

  public void close() throws IOException{
    if (containerDbStore != null) {
      containerDbStore.close();
    }
  }
}
