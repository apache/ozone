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

package org.apache.hadoop.ozone.repair.scm;

import org.apache.hadoop.hdds.cli.RepairSubcommand;
import org.apache.hadoop.ozone.repair.TransactionInfoRepair;
import org.apache.hadoop.ozone.repair.scm.cert.CertRepair;
import org.kohsuke.MetaInfServices;
import picocli.CommandLine;

/**
 * Ozone Repair CLI for SCM.
 */
@CommandLine.Command(name = "scm",
    description = "Operational tool to repair SCM.",
    subcommands = {
        CertRepair.class,
        TransactionInfoRepair.class
    }
)
@MetaInfServices(RepairSubcommand.class)
public class SCMRepair implements RepairSubcommand {

}
