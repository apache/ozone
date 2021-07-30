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
package org.apache.hadoop.hdds.cli;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.Callable;

import com.google.common.collect.Maps;
import org.apache.hadoop.conf.ConfigRedactor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Reconfigurable;
import org.apache.hadoop.conf.ReconfigurationException;
import org.apache.hadoop.conf.ReconfigurationTaskStatus;
import org.apache.hadoop.conf.ReconfigurationUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.ExecutionException;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.RunLast;

/**
 * This is a generic parent class for all the ozone related cli tools.
 */
public abstract class GenericCli extends Configured implements Callable<Void>, GenericParentCommand,
    Reconfigurable {

  @Option(names = {"--verbose"},
      description = "More verbose output. Show the stack trace of the errors.")
  private boolean verbose;

  @Option(names = {"-D", "--set"})
  private Map<String, String> configurationOverrides = new HashMap<>();

  @Option(names = {"-conf"})
  private String configurationPath;

  @Option(names = {"-start"})
  private boolean reconfigStart;

  @Option(names = {"-status"})
  private String reconfigStatus;

  private final CommandLine cmd;

  public GenericCli() {
    cmd = new CommandLine(this);
  }

  private static final Logger LOG =
      LoggerFactory.getLogger(GenericCli.class);

  // Use for testing purpose.
  private ReconfigurationUtil reconfigurationUtil = new ReconfigurationUtil();

  /** Background thread to reload configuration. */
  private Thread reconfigThread = null;
  private volatile boolean shouldRun = true;
  private Object reconfigLock = new Object();

  /**
   * The timestamp when the <code>reconfigThread</code> starts.
   */
  private long startTime = 0;

  /**
   * The timestamp when the <code>reconfigThread</code> finishes.
   */
  private long endTime = 0;

  /**
   * A map of <changed property, error message>. If error message is present,
   * it contains the messages about the error occurred when applies the particular
   * change. Otherwise, it indicates that the change has been successfully applied.
   */
  private Map<ReconfigurationUtil.PropertyChange, Optional<String>> status = null;


  public GenericCli(Class<?> type) {
    this();
    addSubcommands(getCmd(), type);
  }

  private void addSubcommands(CommandLine cli, Class<?> type) {
    ServiceLoader<SubcommandWithParent> registeredSubcommands =
        ServiceLoader.load(SubcommandWithParent.class);
    for (SubcommandWithParent subcommand : registeredSubcommands) {
      if (subcommand.getParentType().equals(type)) {
        final Command commandAnnotation =
            subcommand.getClass().getAnnotation(Command.class);
        CommandLine subcommandCommandLine = new CommandLine(subcommand);
        addSubcommands(subcommandCommandLine, subcommand.getClass());
        cli.addSubcommand(commandAnnotation.name(), subcommandCommandLine);
      }
    }
  }

  /**
   * Handle the error when subcommand is required but not set.
   */
  public static void missingSubcommand(CommandSpec spec) {
    System.err.println("Incomplete command");
    spec.commandLine().usage(System.err);
    System.exit(-1);
  }

  public void run(String[] argv) {
    try {
      execute(argv);
    } catch (ExecutionException ex) {
      printError(ex.getCause() == null ? ex : ex.getCause());
      System.exit(-1);
    }
  }

  @VisibleForTesting
  public void execute(String[] argv) {
    cmd.parseWithHandler(new RunLast(), argv);
  }

  protected void printError(Throwable error) {
    //message could be null in case of NPE. This is unexpected so we can
    //print out the stack trace.
    if (verbose || error.getMessage() == null
        || error.getMessage().length() == 0) {
      error.printStackTrace(System.err);
    } else {
      System.err.println(error.getMessage().split("\n")[0]);
    }
  }

  @Override
  public Void call() throws Exception {
    throw new MissingSubcommandException(cmd);
  }

  @Override
  public OzoneConfiguration createOzoneConfiguration() {
    OzoneConfiguration ozoneConf = new OzoneConfiguration();
    if (configurationPath != null) {
      ozoneConf.addResource(new Path(configurationPath));
    }
    if (configurationOverrides != null) {
      for (Entry<String, String> entry : configurationOverrides.entrySet()) {
        ozoneConf.set(entry.getKey(), entry.getValue());
      }
    }
    return ozoneConf;
  }

  public boolean startReconfiguration() throws IOException {
    return reconfigStart;
  }

  @VisibleForTesting
  public picocli.CommandLine getCmd() {
    return cmd;
  }

  @Override
  public boolean isVerbose() {
    return verbose;
  }

  /**
   * A background thread to apply configuration changes.
   */
  private static class ReconfigurationThread extends Thread {
    private GenericCli parent;

    ReconfigurationThread(GenericCli base) {
      this.parent = base;
    }

    // See {@link ReconfigurationServlet#applyChanges}
    public void run() {
      LOG.info("Starting reconfiguration task.");
      final Configuration oldConf = parent.getConf();
      final Configuration newConf = parent.getNewConf();
      final Collection<ReconfigurationUtil.PropertyChange> changes =
          parent.getChangedProperties(newConf, oldConf);
      Map<ReconfigurationUtil.PropertyChange, Optional<String>> results = Maps.newHashMap();
      ConfigRedactor oldRedactor = new ConfigRedactor(oldConf);
      ConfigRedactor newRedactor = new ConfigRedactor(newConf);
      for (ReconfigurationUtil.PropertyChange change : changes) {
        String errorMessage = null;
        String oldValRedacted = oldRedactor.redact(change.prop, change.oldVal);
        String newValRedacted = newRedactor.redact(change.prop, change.newVal);
        if (!parent.isPropertyReconfigurable(change.prop)) {
          LOG.info(String.format(
              "Property %s is not configurable: old value: %s, new value: %s",
              change.prop,
              oldValRedacted,
              newValRedacted));
          continue;
        }
        LOG.info("Change property: " + change.prop + " from \""
            + ((change.oldVal == null) ? "<default>" : oldValRedacted)
            + "\" to \""
            + ((change.newVal == null) ? "<default>" : newValRedacted)
            + "\".");
        try {
          String effectiveValue =
              parent.reconfigurePropertyImpl(change.prop, change.newVal);
          if (change.newVal != null) {
            oldConf.set(change.prop, effectiveValue);
          } else {
            oldConf.unset(change.prop);
          }
        } catch (ReconfigurationException e) {
          Throwable cause = e.getCause();
          errorMessage = cause == null ? e.getMessage() : cause.getMessage();
        }
        results.put(change, Optional.ofNullable(errorMessage));
      }

      synchronized (parent.reconfigLock) {
        parent.endTime = Time.now();
        parent.status = Collections.unmodifiableMap(results);
        parent.reconfigThread = null;
      }
    }
  }

  /**
   * Start a reconfiguration task to reload configuration in background.
   */
  public void startReconfigurationTask() throws IOException {
    synchronized (reconfigLock) {
      if (!shouldRun) {
        String errorMessage = "The server is stopped.";
        LOG.warn(errorMessage);
        throw new IOException(errorMessage);
      }
      if (reconfigThread != null) {
        String errorMessage = "Another reconfiguration task is running.";
        LOG.warn(errorMessage);
        throw new IOException(errorMessage);
      }
      reconfigThread = new GenericCli.ReconfigurationThread(this);
      reconfigThread.setDaemon(true);
      reconfigThread.setName("Reconfiguration Task");
      reconfigThread.start();
      startTime = Time.now();
    }
  }

  public org.apache.hadoop.conf.ReconfigurationTaskStatus getReconfigurationTaskStatus() {
    synchronized (reconfigLock) {
      if (reconfigThread != null) {
        return new org.apache.hadoop.conf.ReconfigurationTaskStatus(startTime, 0, null);
      }
      return new ReconfigurationTaskStatus(startTime, endTime, status);
    }
  }

  public void shutdownReconfigurationTask() {
    Thread tempThread;
    synchronized (reconfigLock) {
      shouldRun = false;
      if (reconfigThread == null) {
        return;
      }
      tempThread = reconfigThread;
      reconfigThread = null;
    }

    try {
      tempThread.join();
    } catch (InterruptedException e) {
    }
  }

  @VisibleForTesting
  public Collection<ReconfigurationUtil.PropertyChange> getChangedProperties(
      Configuration newConf, Configuration oldConf) {
    return reconfigurationUtil.parseChangedProperties(newConf, oldConf);
  }

  @Override
  public void reconfigureProperty(String property, String newVal) throws ReconfigurationException {
    if (isPropertyReconfigurable(property)) {
      LOG.info("changing property " + property + " to " + newVal);
      synchronized(getConf()) {
        getConf().get(property);
        String effectiveValue = reconfigurePropertyImpl(property, newVal);
        if (newVal != null) {
          getConf().set(property, effectiveValue);
        } else {
          getConf().unset(property);
        }
      }
    } else {
      throw new ReconfigurationException(property, newVal,
          getConf().get(property));
    }
  }

  @Override
  public boolean isPropertyReconfigurable(String property) {
    return getReconfigurableProperties().contains(property);
  }

  @Override
  public abstract Collection<String> getReconfigurableProperties();

  /**
   * Create a new configuration.
   */
  protected abstract Configuration getNewConf();

  protected abstract String reconfigurePropertyImpl(
      String property, String newVal) throws ReconfigurationException;
}
