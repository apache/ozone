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

package org.apache.hadoop.ozone.freon;

import com.codahale.metrics.Timer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.kohsuke.MetaInfServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;

/**
 * Data generator tool test om performance.
 */
@Command(name = "fr",
    aliases = "follower-reader",
    description = "Read the same keySize from multiple threads.",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)
@MetaInfServices(FreonSubcommand.class)
public class FollowerReader extends BaseFreonGenerator
    implements Callable<Void> {

  private static final Logger LOG =
      LoggerFactory.getLogger(FollowerReader.class);

  @CommandLine.Option(names = {"-v", "--volume"},
      description = "Name of the bucket which contains the test data. Will be"
          + " created if missing.",
      defaultValue = "vol1")
  private String volumeName;

  @CommandLine.Option(names = {"-b", "--bucket"},
      description = "Name of the bucket which contains the test data.",
      defaultValue = "bucket1")
  private String bucketName;

  @CommandLine.Option(names = {"-k", "--key"},
      description = "Name of the key which contains the test data.",
      defaultValue = "key1")
  private String keyName;

  private String omServiceID = null;

  private Timer timer;

  private final List<OzoneClient> rpcClients = new ArrayList<>();

  @Override
  public Void call() throws Exception {
    init();
    OzoneConfiguration ozoneConfiguration = createOzoneConfiguration();

    for (int i = 0; i < getThreadNo(); i++) {
      OzoneClient rpcClient = createOzoneClient(omServiceID, ozoneConfiguration);
      rpcClients.add(rpcClient);
    }

    timer = getMetrics().timer("follower-read");

    runTests(this::readKeySize);
    return null;
  }

  private void readKeySize(long counter) throws Exception {
    int clientIdx = (int) (counter % rpcClients.size());
    timer.time(() -> {
      long unused = rpcClients.get(clientIdx).getObjectStore().getVolume(volumeName)
          .getBucket(bucketName).getKey(keyName).getDataSize();
      return null;
    });
  }

}
