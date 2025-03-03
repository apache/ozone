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

package org.apache.hadoop.ozone.debug.datanode.container;

import java.io.IOException;
import java.util.concurrent.Callable;
import org.apache.hadoop.hdds.cli.AbstractSubcommand;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerMetadataInspector;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.metadata.DatanodeStore;
import picocli.CommandLine;
import picocli.CommandLine.Command;

/**
 * {@code ozone debug datanode container inspect},
 * a command to run {@link KeyValueContainerMetadataInspector}.
 */
@Command(
    name = "inspect",
    description
        = "Check the metadata of all container replicas on this datanode.")
public class InspectSubcommand extends AbstractSubcommand implements Callable<Void> {

  @CommandLine.ParentCommand
  private ContainerCommands parent;

  @Override
  public Void call() throws IOException {
    final OzoneConfiguration conf = getOzoneConf();
    parent.loadContainersFromVolumes();

    final KeyValueContainerMetadataInspector inspector
        = new KeyValueContainerMetadataInspector(
            KeyValueContainerMetadataInspector.Mode.INSPECT);
    for (Container<?> container : parent.getController().getContainers()) {
      final ContainerData data = container.getContainerData();
      if (!(data instanceof KeyValueContainerData)) {
        continue;
      }
      final KeyValueContainerData kvData = (KeyValueContainerData) data;
      try (DatanodeStore store = BlockUtils.getUncachedDatanodeStore(
          kvData, conf, true)) {
        final String json = inspector.process(kvData, store, null);
        System.out.println(json);
      } catch (IOException e) {
        System.err.print("Failed to inspect container "
            + kvData.getContainerID() + ": ");
        e.printStackTrace();
      }
    }

    return null;
  }
}
