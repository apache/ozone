package org.apache.hadoop.ozone.container.ozoneimpl;

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.metadata.DatanodeStore;

/**
 * A ContainerInspector is tool used to log information about all
 * containers as they are being processed during datanode startup. It could
 * also be used to repair containers if necessary.
 *
 * These are primarily debug/developer utilities that will slow down datanode
 * startup and are only meant to be run as needed.
 */
public interface ContainerInspector {
  /**
   * Loads necessary configurations to determine how/if to run the inspector
   * when the process method is called.
   *
   * @return true if the inspector will operate when process is called. False
   * otherwise.
   */
  boolean load();

  /**
   * Removes configurations to run the inspector, so that the process method
   * becomes a no-op.
   */
  void unload();

  /**
   * Determines whether the inspector will be modifying containers as part of
   * the process method.
   *
   * @return true if the inspector will only read the container, false if it
   * will be making modifications/repairs.
   */
  boolean isReadOnly();

  /**
   * Operates on the container as the inspector is configured. This may
   * involve logging information or fixing errors.
   *
   * Multiple containers may be processed in parallel by calling this method
   * on the same inspector instance, but only one inspector will be invoked
   * per container at a time. Implementations must ensure that:
   * 1. Information they log is batched so that log output from other
   * inspectors working on other containers is not interleaved.
   * 2. Multiple process calls to the same inspector instance with different
   * containers are thread safe.
   *
   * @param data Container data for the container to process.
   * @param store The metadata store for this container.
   */
  void process(ContainerData data, DatanodeStore store);

  /**
   * @return The type container this inspector can operate on.
   */
  ContainerProtos.ContainerType getContainerType();
}
