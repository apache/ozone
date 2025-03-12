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

package org.apache.hadoop.ozone.debug.audit.parser.handler;

import java.sql.SQLException;
import java.util.concurrent.Callable;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.ozone.debug.audit.parser.AuditParser;
import org.apache.hadoop.ozone.debug.audit.parser.common.DatabaseHelper;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ParentCommand;

/**
 * Template command handler for ozone audit parser.
 */
@Command(name = "template",
    aliases = "t",
    description = "Execute template query.\n\n" +
        "To run a template query on the audit logs loaded to the database:\n" +
        "ozone debug auditparser <path to db file> template <template>\n\n" +
        "Following templates are available:\n" +
        "(Template)              (Description)\n" +
        "top5users              : Top 5 users.\n" +
        "top5cmds               : Top 5 commands.\n" +
        "top5activetimebyseconds: Top 5 active times, grouped by seconds.\n",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class TemplateCommandHandler implements Callable<Void> {

  @Parameters(arity = "1..1", description = "Custom query")
  private String template;

  @ParentCommand
  private AuditParser auditParser;

  @Override
  public Void call() throws Exception {
    try {
      if (DatabaseHelper.validateTemplate(template)) {
        System.out.println(
            DatabaseHelper.executeTemplate(auditParser.getDatabase(),
                template)
        );
      } else {
        System.err.println("ERROR: Invalid template name - " + template);
      }
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    return null;
  }

}
