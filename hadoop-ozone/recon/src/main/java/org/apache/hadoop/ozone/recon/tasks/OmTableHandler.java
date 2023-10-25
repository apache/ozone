package org.apache.hadoop.ozone.recon.tasks;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;

public interface OmTableHandler {

  void handlePutEvent(OMDBUpdateEvent<String, Object> event,
                      String tableName,
                      Collection<String> sizeRelatedTables,
                      HashMap<String, Long> objectCountMap,
                      HashMap<String, Long> unreplicatedSizeCountMap,
                      HashMap<String, Long> replicatedSizeCountMap)
      throws IOException;

  void handleDeleteEvent(OMDBUpdateEvent<String, Object> event,
                         String tableName,
                         Collection<String> sizeRelatedTables,
                         HashMap<String, Long> objectCountMap,
                         HashMap<String, Long> unreplicatedSizeCountMap,
                         HashMap<String, Long> replicatedSizeCountMap)
      throws IOException;

  void handleUpdateEvent(OMDBUpdateEvent<String, Object> event,
                         String tableName,
                         Collection<String> sizeRelatedTables,
                         HashMap<String, Long> objectCountMap,
                         HashMap<String, Long> unreplicatedSizeCountMap,
                         HashMap<String, Long> replicatedSizeCountMap);

}
