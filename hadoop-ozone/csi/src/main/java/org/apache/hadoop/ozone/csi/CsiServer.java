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

package org.apache.hadoop.ozone.csi;

import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerDomainSocketChannel;
import io.netty.channel.unix.DomainSocketAddress;
import java.util.concurrent.Callable;
import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigTag;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.util.OzoneVersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;

/**
 * CLI entrypoint of the CSI service daemon.
 */
@Command(name = "ozone csi",
    hidden = true, description = "CSI service daemon.",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true)
public class CsiServer extends GenericCli implements Callable<Void> {

  private static final Logger LOG = LoggerFactory.getLogger(CsiServer.class);

  @Override
  public Void call() throws Exception {
    String[] originalArgs = getCmd().getParseResult().originalArgs()
            .toArray(new String[0]);
    OzoneConfiguration ozoneConfiguration = getOzoneConf();
    HddsServerUtil.startupShutdownMessage(OzoneVersionInfo.OZONE_VERSION_INFO,
            CsiServer.class, originalArgs, LOG, ozoneConfiguration);
    CsiConfig csiConfig = ozoneConfiguration.getObject(CsiConfig.class);

    OzoneClient rpcClient = OzoneClientFactory.getRpcClient(ozoneConfiguration);

    EpollEventLoopGroup group = new EpollEventLoopGroup();

    if (csiConfig.getVolumeOwner().isEmpty()) {
      throw new IllegalArgumentException(
          "ozone.csi.owner is not set. You should set this configuration "
              + "variable to define which user should own all the created "
              + "buckets.");
    }

    Server server =
        NettyServerBuilder
            .forAddress(new DomainSocketAddress(csiConfig.getSocketPath()))
            .channelType(EpollServerDomainSocketChannel.class)
            .workerEventLoopGroup(group)
            .bossEventLoopGroup(group)
            .addService(new IdentityService())
            .addService(new ControllerService(rpcClient,
                csiConfig.getDefaultVolumeSize()))
            .addService(new NodeService(csiConfig))
            .build();

    server.start();
    server.awaitTermination();
    rpcClient.close();
    return null;
  }

  public static void main(String[] args) {
    new CsiServer().run(args);
  }

  /**
   * Configuration settings specific to the CSI server.
   */
  @ConfigGroup(prefix = "ozone.csi")
  public static class CsiConfig {

    @Config(key = "ozone.csi.socket",
        defaultValue = "/var/lib/csi.sock",
        description =
            "The socket where all the CSI services will listen (file name).",
        tags = ConfigTag.STORAGE)
    private String socketPath;

    @Config(key = "ozone.csi.default-volume-size",
        defaultValue = "1000000000",
        description =
            "The default size of the create volumes (if not specified).",
        tags = ConfigTag.STORAGE)
    private long defaultVolumeSize;

    @Config(key = "ozone.csi.s3g.address",
        defaultValue = "http://localhost:9878",
        description =
            "The address of S3 Gateway endpoint.",
        tags = ConfigTag.STORAGE)
    private String s3gAddress;

    @Config(key = "ozone.csi.owner",
        defaultValue = "",
        description =
            "This is the username which is used to create the requested "
                + "storage. Used as a hadoop username and the generated ozone"
                + " volume used to store all the buckets. WARNING: It can "
                + "be a security hole to use CSI in a secure environments as "
                + "ALL the users can request the mount of a specific bucket "
                + "via the CSI interface.",
        tags = ConfigTag.STORAGE)
    private String volumeOwner;

    @Config(key = "ozone.csi.mount.command",
        defaultValue = "goofys --endpoint %s %s %s",
        description =
            "This is the mount command which is used to publish volume."
                + " these %s will be replicated by s3gAddress, volumeId "
                + " and target path.",
        tags = ConfigTag.STORAGE)
    private String mountCommand;

    public String getSocketPath() {
      return socketPath;
    }

    public String getVolumeOwner() {
      return volumeOwner;
    }

    public void setVolumeOwner(String volumeOwner) {
      this.volumeOwner = volumeOwner;
    }

    public void setSocketPath(String socketPath) {
      this.socketPath = socketPath;
    }

    public long getDefaultVolumeSize() {
      return defaultVolumeSize;
    }

    public void setDefaultVolumeSize(long defaultVolumeSize) {
      this.defaultVolumeSize = defaultVolumeSize;
    }

    public String getS3gAddress() {
      return s3gAddress;
    }

    public void setS3gAddress(String s3gAddress) {
      this.s3gAddress = s3gAddress;
    }

    public String getMountCommand() {
      return mountCommand;
    }
  }
}
