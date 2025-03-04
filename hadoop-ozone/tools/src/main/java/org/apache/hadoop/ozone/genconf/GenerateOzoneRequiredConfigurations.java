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

package org.apache.hadoop.ozone.genconf;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * GenerateOzoneRequiredConfigurations - A tool to generate ozone-site.xml<br>
 * This tool generates an ozone-site.xml with minimally required configs.
 * This tool can be invoked as follows:<br>
 * <ul>
 * <li>ozone genconf {@literal <Path to output file>}</li>
 * <li>ozone genconf --help</li>
 * <li>ozone genconf -h</li>
 * </ul>
 */
@Command(
    name = "ozone genconf",
    description = "Tool to generate template ozone-site.xml",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true)
public final class GenerateOzoneRequiredConfigurations extends GenericCli implements Callable<Void> {

  @Parameters(arity = "1..1",
      description = "Directory path where ozone-site file should be generated.")
  private String path;

  @Option(names = "--security", description = "Generates security config " +
      "template, update Kerberos principal and keytab file before use.")
  private boolean genSecurityConf;

  public static void main(String[] args) throws Exception {
    new GenerateOzoneRequiredConfigurations().run(args);
  }

  @Override
  public Void call() throws Exception {
    generateConfigurations();
    return null;
  }

  private void generateConfigurations() throws
      JAXBException, IOException {

    if (!isValidPath(path)) {
      throw new IllegalArgumentException("Invalid directory path.");
    }

    if (!canWrite(path)) {
      throw new IllegalArgumentException("Insufficient permission.");
    }

    OzoneConfiguration oc = new OzoneConfiguration();

    ClassLoader cL = Thread.currentThread().getContextClassLoader();
    if (cL == null) {
      cL = OzoneConfiguration.class.getClassLoader();
    }
    URL url = cL.getResource("ozone-default.xml");

    List<OzoneConfiguration.Property> allProperties =
        oc.readPropertyFromXml(url);

    List<OzoneConfiguration.Property> requiredProperties = new ArrayList<>();

    for (OzoneConfiguration.Property p : allProperties) {
      if (p.getTag() != null && (p.getTag().contains("REQUIRED") ||
          (genSecurityConf && p.getTag().contains("KERBEROS")))) {
        // Set default value for common required configs
        if (p.getName().equalsIgnoreCase(
            OzoneConfigKeys.OZONE_METADATA_DIRS)) {
          p.setValue(System.getProperty(OzoneConsts.JAVA_TMP_DIR));
        } else if (p.getName().equalsIgnoreCase(
            OMConfigKeys.OZONE_OM_ADDRESS_KEY)
            || p.getName().equalsIgnoreCase(ScmConfigKeys.OZONE_SCM_NAMES)
            || p.getName().equalsIgnoreCase(
              ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY)) {
          p.setValue(OzoneConsts.LOCALHOST);
        }

        // Set default value for KERBEROS configs
        if (p.getName().equalsIgnoreCase(
            OzoneConfigKeys.OZONE_SECURITY_ENABLED_KEY)) {
          p.setValue(OzoneConsts.OZONE_SECURITY_ENABLED_SECURE);
        } else if (p.getName().equalsIgnoreCase(
            OzoneConfigKeys.OZONE_HTTP_SECURITY_ENABLED_KEY)) {
          p.setValue(OzoneConsts.OZONE_HTTP_SECURITY_ENABLED_SECURE);
        } else if (p.getName().equalsIgnoreCase(
            OzoneConfigKeys.OZONE_HTTP_FILTER_INITIALIZERS_KEY)) {
          p.setValue(OzoneConsts.OZONE_HTTP_FILTER_INITIALIZERS_SECURE);
        } else if (p.getName().endsWith(OzoneConsts.HTTP_AUTH_TYPE_SUFFIX)) {
          p.setValue(OzoneConsts.KERBEROS_CONFIG_VALUE);
        }

        requiredProperties.add(p);
      }
    }

    OzoneConfiguration.XMLConfiguration generatedConfig =
        new OzoneConfiguration.XMLConfiguration(requiredProperties);

    File output = new File(path, "ozone-site.xml");
    if (output.createNewFile()) {
      JAXBContext context =
          JAXBContext.newInstance(OzoneConfiguration.XMLConfiguration.class);
      Marshaller m = context.createMarshaller();
      m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
      m.marshal(generatedConfig, output);

      out().println("ozone-site.xml has been generated at " + path);
    } else {
      out().printf("ozone-site.xml already exists at %s and " +
          "will not be overwritten%n", path);
    }

  }

  /**
   * Check if the path is valid directory.
   *
   * @return true, if path is valid directory, else return false
   */
  public static boolean isValidPath(String path) {
    try {
      return Files.isDirectory(Paths.get(path));
    } catch (InvalidPathException | NullPointerException ex) {
      return false;
    }
  }

  /**
   * Check if user has permission to write in the specified path.
   *
   * @return true, if the user has permission to write, else returns false
   */
  public static boolean canWrite(String path) {
    File file = new File(path);
    return file.canWrite();
  }

}
