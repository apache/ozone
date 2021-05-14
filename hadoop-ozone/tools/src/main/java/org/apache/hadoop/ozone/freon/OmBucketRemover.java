/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.freon;

import com.codahale.metrics.Timer;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.concurrent.Callable;

/**
 * Data generator tool test om performance.
 */
@Command(name = "ombr",
    aliases = "om-bucket-remover",
    description = "Remove ozone buckets on OM side.",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)
public class OmBucketRemover extends BaseFreonGenerator
    implements Callable<Void> {

  @Option(names = {"-v", "--volume"},
      description = "Name of the bucket which contains the test data. Will be"
          + " created if missing.",
      defaultValue = "vol1")
  private String volumeName;

  @Option(
      names = "--om-service-id",
      description = "OM Service ID"
  )
  private String omServiceID = null;

  private OzoneManagerProtocol ozoneManagerClient;

  private Timer bucketCreationTimer;

  @Override
  public Void call() throws Exception {

    init();

    OzoneConfiguration ozoneConfiguration = createOzoneConfiguration();

    try {

      ozoneManagerClient = createOmClient(ozoneConfiguration, omServiceID);

      bucketCreationTimer = getMetrics().timer("bucket-remove");

      runTests(this::removeBucket);

    } finally {
      if (ozoneManagerClient != null) {
        ozoneManagerClient.close();
      }
    }

    return null;
  }

  private void removeBucket(long index) throws Exception {

    String bucketName = generateBucketName(index);

    bucketCreationTimer.time(() -> {
      ozoneManagerClient.deleteBucket(volumeName, bucketName);
      return null;
    });
  }

}
