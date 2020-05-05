package org.apache.hadoop.ozone.debug;

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.impl.ChunkLayOutVersion;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;

import java.io.File;


public final class ChunkFileUtility {

    static String getChunkLocationPath(String containerLocation) {
        return containerLocation + File.separator + OzoneConsts.STORAGE_DIR_CHUNKS;
    }

    public static String getChunkFilePath(ContainerProtos.ChunkInfo
      chunkInfo, OmKeyLocationInfo keyLocation,
      ContainerProtos.ContainerDataProto data,
      ChunkLayOutVersion layOutVersion)
      throws StorageContainerException {
        switch (layOutVersion) {
        case FILE_PER_CHUNK:
          return  getChunkLocationPath(data
                  .getContainerPath())
                  + File.separator
                  + chunkInfo.getChunkName();
        case FILE_PER_BLOCK:
          return  getChunkLocationPath(data
                  .getContainerPath())
                  + File.separator
                  + keyLocation.getLocalID() + ".block";
        default:
          throw new StorageContainerException("chunk strategy does not exist",
                  ContainerProtos.Result.UNABLE_TO_FIND_CHUNK);
        }
    }
}
