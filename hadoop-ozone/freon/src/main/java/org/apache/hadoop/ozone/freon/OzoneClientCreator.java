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
import java.util.concurrent.Callable;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.kohsuke.MetaInfServices;
import picocli.CommandLine;

/**
 * Creates and closes Ozone clients.
 */
@CommandLine.Command(name = "occ",
    aliases = "ozone-client-creator",
    description = "Create and close Ozone clients without doing anything useful",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)
@MetaInfServices(FreonSubcommand.class)
public class OzoneClientCreator extends BaseFreonGenerator implements Callable<Void> {

  @CommandLine.Option(names = "--om-service-id",
      description = "OM Service ID"
  )
  private String omServiceID;

  private Timer timer;
  private OzoneConfiguration conf;

  @Override
  public Void call() {
    init();
    conf = createOzoneConfiguration();
    timer = getMetrics().timer("client-create");
    runTests(this::createClient);
    return null;
  }

  private void createClient(long step) {
    timer.time(this::createClientSafely);
  }

  private void createClientSafely() {
    try {
      createOzoneClient(omServiceID, conf).close();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
