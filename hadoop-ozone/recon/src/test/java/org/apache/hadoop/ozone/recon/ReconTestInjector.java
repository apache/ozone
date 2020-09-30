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

package org.apache.hadoop.ozone.recon;

import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_DATANODE_ADDRESS_KEY;
import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_PROMETHEUS_HTTP_ENDPOINT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_DB_DIR;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_OM_SNAPSHOT_DB_DIR;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SCM_DB_DIR;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.ozone.recon.persistence.AbstractReconSqlDBTest;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ContainerDBServiceProvider;
import org.apache.hadoop.ozone.recon.spi.OzoneManagerServiceProvider;
import org.apache.hadoop.ozone.recon.spi.impl.ContainerDBServiceProviderImpl;
import org.apache.hadoop.ozone.recon.spi.impl.ReconContainerDBProvider;
import org.junit.Assert;
import org.junit.rules.TemporaryFolder;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Singleton;

/**
 * Class to setup a recon test injector, with any combination of sub modules
 * that are specified. This Recon specific abstraction to Guice API has
 * been created to simplify the process of setting up a test environment for
 * unit testing.
 */
public class ReconTestInjector {

  private Injector injector;
  private OzoneManagerServiceProvider ozoneManagerServiceProvider;
  private ReconOMMetadataManager reconOMMetadataManager;
  private OzoneStorageContainerManager reconScm;
  private AbstractReconSqlDBTest reconSqlDB;
  private boolean withContainerDB = false;
  private List<Module> additionalModules = new ArrayList<>();
  private boolean withReconSqlDb = false;
  private TemporaryFolder temporaryFolder;
  private Map<Class, Class> extraInheritedBindings = new HashMap<>();
  private Map<Class, Object> extraInstanceBindings = new HashMap<>();
  private Set<Class> extraClassBindings = new HashSet<>();

  public ReconTestInjector(TemporaryFolder temporaryFolder) {
    this.temporaryFolder = temporaryFolder;
  }

  public void setWithReconSqlDb(boolean withReconSqlDb) {
    this.withReconSqlDb = withReconSqlDb;
  }

  public void setOzoneManagerServiceProvider(
      OzoneManagerServiceProvider ozoneManagerServiceProvider) {
    this.ozoneManagerServiceProvider = ozoneManagerServiceProvider;
  }

  public void setReconOMMetadataManager(
      ReconOMMetadataManager reconOMMetadataManager) {
    this.reconOMMetadataManager = reconOMMetadataManager;
  }

  public void setReconScm(OzoneStorageContainerManager reconScm) {
    this.reconScm = reconScm;
  }

  public void withContainerDB(boolean containerDbIncluded) {
    this.withContainerDB = containerDbIncluded;
  }

  public OzoneManagerServiceProvider getOzoneManagerServiceProvider() {
    return ozoneManagerServiceProvider;
  }

  public ReconOMMetadataManager getReconOMMetadataManager() {
    return reconOMMetadataManager;
  }

  public OzoneStorageContainerManager getReconScm() {
    return reconScm;
  }

  public List<Module> getAdditionalModules() {
    return additionalModules;
  }

  public Map<Class, Object> getExtraInstanceBindings() {
    return extraInstanceBindings;
  }

  public Map<Class, Class> getExtraInheritedBindings() {
    return extraInheritedBindings;
  }

  public Set<Class> getExtraClassBindings() {
    return extraClassBindings;
  }

  /**
   * Wrapper to get the bound instance.
   * @param type type
   * @param <T> type
   * @return bound instance of type T.
   */
  public <T> T getInstance(Class<T> type) {
    return injector.getInstance(type);
  }

  /**
   * The goal of the class is to discourage the use of injector to
   * create more child injectors explicitly.
   * Use this API wisely!
   * @return injector.
   */
  public Injector getInjector() {
    return injector;
  }

  void setupInjector() throws IOException {
    List<Module> modules = new ArrayList<>();

    modules.add(new AbstractModule() {
      @Override
      protected void configure() {
        try {
          bind(OzoneConfiguration.class).toInstance(
              getTestOzoneConfiguration(temporaryFolder.newFolder()));

          if (reconOMMetadataManager != null) {
            bind(ReconOMMetadataManager.class)
                .toInstance(reconOMMetadataManager);
          }

          if (ozoneManagerServiceProvider != null) {
            bind(OzoneManagerServiceProvider.class)
                .toInstance(ozoneManagerServiceProvider);
          }

          if (reconScm != null) {
            bind(OzoneStorageContainerManager.class).toInstance(reconScm);
          }

          if (withContainerDB) {
            bind(ContainerDBServiceProvider.class)
                .to(ContainerDBServiceProviderImpl.class).in(Singleton.class);
            bind(DBStore.class).toProvider(ReconContainerDBProvider.class).
                in(Singleton.class);
          }

          for (Map.Entry<Class, Object> entry :
              extraInstanceBindings.entrySet()) {
            bind(entry.getKey()).toInstance(entry.getValue());
          }

          for (Map.Entry<Class, Class> entry :
              extraInheritedBindings.entrySet()) {
            bind(entry.getKey()).to(entry.getValue()).in(Singleton.class);
          }

          for (Class type : extraClassBindings) {
            bind(type).in(Singleton.class);
          }
        } catch (IOException e) {
          Assert.fail();
        }
      }
    });

    if (CollectionUtils.isNotEmpty(additionalModules)) {
      modules.addAll(additionalModules);
    }

    if (withReconSqlDb) {
      reconSqlDB = new AbstractReconSqlDBTest();
      modules.addAll(reconSqlDB.getReconSqlDBModules());
    }

    injector = Guice.createInjector(modules);
    if (reconSqlDB != null) {
      reconSqlDB.createSchema(injector);
    }
  }

  public OzoneConfiguration getTestOzoneConfiguration(
      File dir) {
    OzoneConfiguration configuration = new OzoneConfiguration();
    configuration.set(OZONE_RECON_OM_SNAPSHOT_DB_DIR, dir.getAbsolutePath());
    configuration.set(OZONE_RECON_DB_DIR, dir.getAbsolutePath());
    configuration.set(OZONE_RECON_SCM_DB_DIR, dir.getAbsolutePath());
    configuration.set(OZONE_RECON_DATANODE_ADDRESS_KEY,
        "0.0.0.0:0");
    configuration.set(OZONE_RECON_PROMETHEUS_HTTP_ENDPOINT,
        "http://localhost:6666");
    return configuration;
  }


  /**
   * Builder for Recon Test Injector.
   */
  public static class Builder {
    private ReconTestInjector reconTestInjector;

    public Builder(TemporaryFolder temporaryFolder) {
      reconTestInjector = new ReconTestInjector(temporaryFolder);
    }

    /**
     * Use if you need the Recon SQL DB instance.
     */
    public Builder withReconSqlDb() {
      reconTestInjector.setWithReconSqlDb(true);
      return this;
    }

    /**
     * Pass in your Ozone manager service provider implementation, maybe with
     * mocked behavior.
     * @param ozoneManagerServiceProvider instance
     */
    public Builder withOmServiceProvider(
        OzoneManagerServiceProvider ozoneManagerServiceProvider) {
      reconTestInjector.setOzoneManagerServiceProvider(
          ozoneManagerServiceProvider);
      return this;
    }

    /**
     * Pass in your ReconOMMetadataManager implementation, maybe with
     * mocked behavior.
     * @param reconOm instance
     */
    public Builder withReconOm(ReconOMMetadataManager reconOm) {
      reconTestInjector.setReconOMMetadataManager(reconOm);
      return this;
    }

    /**
     * Pass in your Recon SCM implementation.
     * @param reconScm instance
     * @return Builder.
     */
    public Builder withReconScm(OzoneStorageContainerManager reconScm) {
      reconTestInjector.setReconScm(reconScm);
      return this;
    }

    /**
     * Use if you need the ReconContainerDB. Bound to default implementation.
     * @return Builder.
     */
    public Builder withContainerDB() {
      reconTestInjector.withContainerDB(true);
      return this;
    }

    /**
     * Add binding of the type bind(A.class).toInstance(AImpl).
     * @param type class type
     * @param instance instance
     * @return Builder.
     */
    public Builder addBinding(Class type, Object instance) {
      reconTestInjector.getExtraInstanceBindings().put(type, instance);
      return this;
    }

    /**
     * Add binding of the type bind(A.class).
     * @param type class type
     * @return Builder.
     */
    public Builder addBinding(Class type) {
      reconTestInjector.getExtraClassBindings().add(type);
      return this;
    }

    /**
     * Add binding of the type bind(A.class).to(B.class) where B extends A.
     * @param type class type
     * @return Builder.
     */
    public Builder addBinding(Class type, Class inheritedType) {
      reconTestInjector.getExtraInheritedBindings().put(type, inheritedType);
      return this;
    }

    /**
     * If you really need to pass in more injector modules for extending the
     * set of classes bound, use this.
     * @param module external module.
     * @return Builder.
     */
    public Builder addModule(Module module) {
      reconTestInjector.getAdditionalModules().add(module);
      return this;
    }

    /**
     * Build the whole graph of classes.
     * @return ReconTestInjector
     * @throws IOException on error.
     */
    public ReconTestInjector build() throws IOException {
      reconTestInjector.setupInjector();
      return reconTestInjector;
    }
  }
}
