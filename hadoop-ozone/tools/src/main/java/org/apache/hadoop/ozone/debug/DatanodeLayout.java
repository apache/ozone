/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.debug;

import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.cli.SubcommandWithParent;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;

import org.kohsuke.MetaInfServices;
import picocli.CommandLine.Spec;
import picocli.CommandLine.Model.CommandSpec;

import picocli.CommandLine;

import java.util.UUID;
import java.util.concurrent.Callable;

import static org.apache.hadoop.hdds.DFSConfigKeysLegacy.DFS_DATANODE_DATA_DIR_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_DATANODE_DIR_KEY;

/**
 * Parse Ratis Log CLI implementation.
 */
@CommandLine.Command(
    name = "dnlayout",
    description = "Shell of updating datanode layout format",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true)
@MetaInfServices(SubcommandWithParent.class)
public class DatanodeLayout extends GenericCli
    implements Callable<Void>, SubcommandWithParent{
  private static UUID dummyDatanodeUuid = UUID.randomUUID();

  @CommandLine.Option(names = {"--path"},
      required = true,
      description = "File Path")
  private String storagePath;

  @Spec
  private CommandSpec spec;

  @Override
  public Void call() throws Exception {
    System.out.println("in datanode layout tool");
    OzoneConfiguration conf = createOzoneConfiguration();

    conf.unset(HDDS_DATANODE_DIR_KEY);

    conf.set(HDDS_DATANODE_DIR_KEY, storagePath);
    conf.setBoolean(ScmConfigKeys.HDDS_DATANODE_UPGRADE_LAYOUT_INLINE, true);

    MutableVolumeSet volumeSet =
        new MutableVolumeSet(null, conf);
    ContainerSet containerSet = new ContainerSet();
    OzoneContainer.buildContainerSet(volumeSet, containerSet, conf);

    volumeSet.shutdown();
    return null;
  }

  public static void main(String[] args) {
    new DatanodeLayout().run(args);
  }

  @Override
  public Class<?> getParentType() {
    return OzoneDebug.class;
  }
}