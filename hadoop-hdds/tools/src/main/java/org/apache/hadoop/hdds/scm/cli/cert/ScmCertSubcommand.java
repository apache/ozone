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
package org.apache.hadoop.hdds.scm.cli.cert;

import org.apache.hadoop.hdds.protocol.SCMSecurityProtocol;
import org.apache.hadoop.hdds.scm.cli.ScmOption;
import picocli.CommandLine;

import java.io.IOException;
import java.util.concurrent.Callable;

/**
 * Base class for admin commands that connect via SCM security client.
 */
public abstract class ScmCertSubcommand implements Callable<Void> {

  @CommandLine.Mixin
  private ScmOption scmOption;

  protected abstract void execute(SCMSecurityProtocol client)
      throws IOException;

  @Override
  public final Void call() throws Exception {
    SCMSecurityProtocol client = scmOption.createScmSecurityClient();
    execute(client);
    return null;
  }
}
