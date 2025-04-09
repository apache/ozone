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

package org.apache.hadoop.ozone.shell;

import com.fasterxml.jackson.databind.node.ArrayNode;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.Callable;
import org.apache.hadoop.hdds.cli.AbstractSubcommand;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.server.JsonUtils;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for shell commands that connect via Ozone client.
 */
@SuppressWarnings("squid:S106") // CLI
public abstract class Handler extends AbstractSubcommand implements Callable<Void> {

  protected static final Logger LOG = LoggerFactory.getLogger(Handler.class);

  private OzoneConfiguration conf;

  protected OzoneAddress getAddress() throws OzoneClientException {
    return new OzoneAddress();
  }

  protected abstract void execute(OzoneClient client, OzoneAddress address)
      throws IOException;

  /**
   * Checks whether the current command should be executed or not.
   * If it is skipped, an informational message should be output.
   * Eg. some commands only work in secure clusters.
   *
   * @return true if the command should be executed
   */
  protected boolean isApplicable() {
    return true;
  }

  @Override
  public Void call() throws Exception {
    conf = getOzoneConf();

    if (!isApplicable()) {
      return null;
    }

    OzoneAddress address = getAddress();
    try (OzoneClient client = createClient(address)) {
      execute(client, address);
    }

    return null;
  }

  protected OzoneClient createClient(OzoneAddress address)
      throws IOException {
    return address.createClient(conf);
  }

  protected boolean securityEnabled() {
    boolean enabled = OzoneSecurityUtil.isSecurityEnabled(conf);
    if (!enabled) {
      err().printf("Error: '%s' operation works only when security is " +
          "enabled. To enable security set ozone.security.enabled to " +
          "true.%n", spec().qualifiedName().trim());
    }
    return enabled;
  }

  protected void printObjectAsJson(Object o) throws IOException {
    System.out.println(JsonUtils.toJsonStringWithDefaultPrettyPrinter(o));
  }

  /**
   * Pretty print the result as a valid JSON array to out().
   * @return Number of entries actually printed.
   */
  protected int printAsJsonArray(Iterator<?> iterator, int limit) {
    int counter = 0;
    final ArrayNode arrayNode = JsonUtils.createArrayNode();
    while (limit > counter && iterator.hasNext()) {
      arrayNode.add(JsonUtils.createObjectNode(iterator.next()));
      counter++;
    }
    out().println(arrayNode.toPrettyString());
    return counter;
  }

  protected void printMsg(String msg) {
    out().println(msg);
  }

  protected OzoneConfiguration getConf() {
    return conf;
  }

}
