/*
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
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.concurrent.Callable;

/**
 * Data remover tool test om performance.
 */
@Command(name = "ocr",
    aliases = "ozone-client-remover",
    description = "Remove keys with the help of the ozone clients.",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)
public class OzoneClientRemover extends BaseFreonGenerator
    implements Callable<Void> {

  @Option(names = {"-v", "--volume"},
      description = "Name of the bucket which contains the test data. Will be"
          + " created if missing.",
      defaultValue = "vol1")
  private String volumeName;

  @Option(names = {"-b", "--bucket"},
      description = "Name of the bucket which contains the test data. Will be"
          + " ignored when \"--remove-bucket\" is set.",
      defaultValue = "bucket1")
  private String bucketName;

  @Option(
      names = "--om-service-id",
      description = "OM Service ID"
  )
  private String omServiceID = null;

  @Option(names = {"--remove-bucket"},
      description = "If turned on, Remover will be used to remove buckets. "
          + "Can only set one option between \"--remove-key\" "
          + "and \"--remove-bucket\"")
  private boolean isRemoveBucket;

  @Option(names = {"--remove-key"},
      description = "If turned on, Remover will be used to remove keys. "
          + "Can only set one option between \"--remove-key\" "
          + "and \"--remove-bucket\"")
  private boolean isRemoveKey;

  private Timer timer;

  private OzoneVolume ozoneVolume;
  private OzoneBucket ozoneBucket;

  @Override
  public Void call() throws Exception {

    checkOption();

    init();
    OzoneConfiguration ozoneConfiguration = createOzoneConfiguration();

    try (OzoneClient rpcClient = createOzoneClient(omServiceID,
        ozoneConfiguration)) {
      ozoneVolume = rpcClient.getObjectStore().getVolume(volumeName);

      timer = getMetrics().timer("remove");

      if (isRemoveBucket) {
        runTests(this::removeBucket);
      } else if (isRemoveKey) {
        ozoneBucket = ozoneVolume.getBucket(bucketName);
        runTests(this::removeKey);
      }
    }

    return null;
  }

  private void removeBucket(long counter) throws Exception {
    final String bucket = generateBucketName(counter);

    timer.time(() -> {
      ozoneVolume.deleteBucket(bucket);
      return null;
    });
  }

  private void removeKey(long counter) throws Exception {
    final String key = generateObjectName(counter);

    timer.time(() -> {
      ozoneBucket.deleteKey(key);
      return null;
    });
  }

  private void checkOption() {
    if (isRemoveBucket && isRemoveKey) {
      throw new UnsupportedOperationException("Invalid Option. Can only support"
          + " one option between \"--remove-bucket\" and \"--remove-key\".");

    }
  }
}
