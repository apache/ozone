/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.container.common.interfaces;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.impl.ContainerDataYaml;

/**
 * Service to pack/unpack ContainerData container data to/from a single byte
 * stream.
 */
public interface ContainerPacker<CONTAINERDATA extends ContainerData> {

  /**
   * Extract the container data to the path defined by the container.
   * <p>
   * This doesn't contain the extraction of the container descriptor file.
   *
   * @return the byte content of the descriptor (which won't be written to a
   * file but returned).
   */
  byte[] unpackContainerData(Container<CONTAINERDATA> container,
      InputStream inputStream, Path tmpDir, Path destContainerDir)
      throws IOException;

  /**
   * Compress all the container data (chunk data, metadata db AND container
   * descriptor) to one single archive.
   */
  void pack(Container<CONTAINERDATA> container, OutputStream destination)
      throws IOException;

  /**
   * Read the descriptor from the finished archive to get the data before
   * importing the container.
   */
  byte[] unpackContainerDescriptor(InputStream inputStream)
      throws IOException;

  /**
   * Persists the custom state for a container. This method allows saving the container file to a custom location.
   */
  default void persistCustomContainerState(Container<? extends ContainerData> container, byte[] descriptorContent,
      ContainerProtos.ContainerDataProto.State state, Path containerMetadataPath) throws IOException {
    if (descriptorContent == null) {
      return;
    }
    ContainerData originalContainerData = ContainerDataYaml.readContainer(descriptorContent);
    container.getContainerData().setState(state);
    container.update(originalContainerData.getMetadata(), true, containerMetadataPath.toString());
  }
}
