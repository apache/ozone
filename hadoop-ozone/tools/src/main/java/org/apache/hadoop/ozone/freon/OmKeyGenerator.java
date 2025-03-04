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

import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.ALL;

import com.codahale.metrics.Timer;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs.Builder;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.helpers.OzoneAclUtil;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.security.UserGroupInformation;
import org.kohsuke.MetaInfServices;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

/**
 * Data generator tool test om performance.
 */
@Command(name = "omkg",
    aliases = "om-key-generator",
    description = "Create keys to the om metadata table.",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)
@MetaInfServices(FreonSubcommand.class)
public class OmKeyGenerator extends BaseFreonGenerator
    implements Callable<Void> {

  @Option(names = {"-v", "--volume"},
      description = "Name of the volume which contains the test data. Will be"
          + " created if missing.",
      defaultValue = "vol1")
  private String volumeName;

  @Option(names = {"-b", "--bucket"},
      description = "Name of the bucket which contains the test data. Will be"
          + " created if missing.",
      defaultValue = "bucket1")
  private String bucketName;

  @Mixin
  private FreonReplicationOptions replication;

  @Option(
      names = "--om-service-id",
      description = "OM Service ID"
  )
  private String omServiceID = null;

  private OzoneManagerProtocol ozoneManagerClient;

  private Timer timer;
  private ReplicationConfig replicationConfig;

  @Override
  public Void call() throws Exception {
    init();

    OzoneConfiguration conf = createOzoneConfiguration();
    replicationConfig = replication.fromParams(conf).orElse(null);

    try (OzoneClient rpcClient = createOzoneClient(omServiceID, conf)) {

      ensureVolumeAndBucketExist(rpcClient, volumeName, bucketName);

      ozoneManagerClient = createOmClient(conf, omServiceID);

      timer = getMetrics().timer("key-create");

      runTests(this::createKey);
    } finally {
      if (ozoneManagerClient != null) {
        ozoneManagerClient.close();
      }
    }

    return null;
  }

  private void createKey(long counter) throws Exception {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    String ownerName = ugi.getShortUserName();
    OmKeyArgs keyArgs = new Builder()
        .setBucketName(bucketName)
        .setVolumeName(volumeName)
        .setKeyName(generateObjectName(counter))
        .setReplicationConfig(replicationConfig)
        .setLocationInfoList(new ArrayList<>())
        .setAcls(OzoneAclUtil.getAclList(ugi, ALL, ALL))
        .setOwnerName(ownerName)
        .build();

    timer.time(() -> {
      OpenKeySession openKeySession = ozoneManagerClient.openKey(keyArgs);

      ozoneManagerClient.commitKey(keyArgs, openKeySession.getId());
      return null;
    });
  }

}
