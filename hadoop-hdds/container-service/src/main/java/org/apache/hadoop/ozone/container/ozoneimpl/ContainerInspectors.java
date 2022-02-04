package org.apache.hadoop.ozone.container.ozoneimpl;

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.metadata.DatanodeStore;
import sun.jvm.hotspot.ui.action.InspectAction;

import java.sql.Array;
import java.util.ArrayList;
import java.util.List;

public final class ContainerInspectors {

  private static final List<ContainerInspector> inspectors = new ArrayList<>();
  static {
    // If new inspectors need to be added, they should be added to this list.
    inspectors.add(new KeyValueContainerMetadataInspector());
  }

  private ContainerInspectors() { }

  public static void load() {
    for (ContainerInspector inspector: inspectors) {
      inspector.load();
    }
  }

  public static void unload() {
    for (ContainerInspector inspector: inspectors) {
      inspector.unload();
    }
  }

  public static boolean isReadOnly(ContainerProtos.ContainerType type) {
    boolean readOnly = true;
    for (ContainerInspector inspector: inspectors) {
      if (inspector.getContainerType() == type) {
        if (!inspector.isReadOnly()) {
          readOnly = false;
          break;
        }
      }
    }
    return readOnly;
  }

  public static void process(ContainerData data, DatanodeStore store) {
    for (ContainerInspector inspector: inspectors) {
      if (inspector.getContainerType() == data.getContainerType()) {
        inspector.process(data, store);
      }
    }
  }
}
