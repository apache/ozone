package org.apache.hadoop.ozone.om.bucket.server;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;

import java.io.IOException;
import java.util.List;

/** A server endpoint that acts as the communication layer for Ozone
 * buckets. */
public interface XbeiverServerSpi {

  /** Starts the server. */
  void start() throws IOException;

  /** Stops a running server. */
  void stop();

  /**
   * submits a containerRequest to be performed by the replication pipeline.
   * @param request ContainerCommandRequest
   */
  OzoneManagerProtocolProtos.OMResponse submitRequest(OzoneManagerProtocolProtos.OMRequest omRequest)
      throws ServiceException;

  /**
   * Returns true if the given pipeline exist.
   *
   * @return true if pipeline present, else false
   */
  boolean isExist(RaftGroupId pipelineId);

  /**
   * Join a new pipeline.
   */
  default void addGroup(RaftGroupId groupId,
                        List<RaftPeer> peers) throws IOException {
  }

  /**
   * Join a new pipeline with priority.
   */
  default void addGroup(RaftGroupId groupId,
                        List<RaftPeer> peers,
                        List<Integer> priorityList) throws IOException {
  }

  /**
   * Exit a pipeline.
   */
  default void removeGroup(HddsProtos.PipelineID pipelineId)
      throws IOException {
  }
}
