package org.apache.hadoop.hdds.upgrade;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMHeartbeatRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMHeartbeatResponseProto;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.proxy.SCMContainerLocationFailoverProxyProvider;
import org.apache.hadoop.hdds.scm.server.SCMConfigurator;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.scm.server.upgrade.SCMUpgradeFinalizationContext;
import org.apache.hadoop.ozone.upgrade.InjectedUpgradeFinalizationExecutor;
import org.apache.hadoop.ozone.upgrade.InjectedUpgradeFinalizationExecutor.UpgradeTestInjectionPoints;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import static org.apache.hadoop.hdds.scm.ha.SCMContext.INVALID_TERM;

public class TestScmHAFinalization {
  private static final String clientID = UUID.randomUUID().toString();
  
  List<StorageContainerManager> scms;
  StorageContainerLocationProtocol scmClient;
  // Used to unpause finalization at the halting point.
  CountDownLatch unpauseSignal = new CountDownLatch(1);

  @BeforeEach
  @ParameterizedTest
  @EnumSource(UpgradeTestInjectionPoints.class)
  public void init(UpgradeTestInjectionPoints haltingPoint) throws Exception {
    OzoneConfiguration conf = buildConfiguration();

    InjectedUpgradeFinalizationExecutor<SCMUpgradeFinalizationContext> executor =
        new InjectedUpgradeFinalizationExecutor<>();
    executor.configureTestInjectionFunction(haltingPoint, () -> {
        unpauseSignal.await();
        return false;
    });

    SCMConfigurator configurator = new SCMConfigurator();
    configurator.setUpgradeFinalizationExecutor(executor);

    scms = Arrays.asList(
        HddsTestUtils.getScmSimple(conf, configurator),
        HddsTestUtils.getScmSimple(conf, configurator),
        HddsTestUtils.getScmSimple(conf, configurator)
    );

    scmClient = new
        StorageContainerLocationProtocolClientSideTranslatorPB(
            new SCMContainerLocationFailoverProxyProvider(conf, null));
    
    // TODO: Wait for SCM leader.
  }

  @Test
  public void testRestart() throws Exception {
    // SCM should remain in safemode until we have registered all the 
    // datanodes.
    Assertions.assertTrue(scmClient.inSafeMode());
    try {
      scmClient.finalizeScmUpgrade(clientID);
      Assertions.fail("SCM should not be able to finalize in safemode.");
    } catch (Exception e) {
      // TODO: test exception type.
    }
    
    // Register datanodes and wait for SCM to leave safemode.
    exitSafemode();
    scmClient.finalizeScmUpgrade(clientID);
    
    // finalization executor has now paused at the specified point.
    // Restart SCMs.
    scms.forEach(StorageContainerManager::stop);
    for (StorageContainerManager scm : scms) {
      scm.start();
    }
    exitSafemode();

    
  }

  @Test
  public void testLeaderChange() {

  }

  @Test
  public void testSnapshotFinalization() {

  }

  private static OzoneConfiguration buildConfiguration() {
    // TODO
    return null;
  }

  private void unpauseFinalization() {
    unpauseSignal.countDown();
  }
  
  private void exitSafemode() {
    
  }
  
  private void processScmCommands(SCMHeartbeatResponseProto responseWithCommands) {
    processPipelienCommands();
    processFinalizeCommands();
  }

  private SCMHeartbeatResponseProto sendHeartbeat(
      SCMHeartbeatRequestProto heartbeatProto) throws Exception {
    return sendHeartbeat(heartbeatProto, scms);
  }

  private SCMHeartbeatResponseProto sendHeartbeat(SCMHeartbeatRequestProto heartbeatProto,
      List<StorageContainerManager> scmsToSendTo) throws Exception {

    SCMHeartbeatResponseProto leaderResponseProto = null;
    long maxTerm = INVALID_TERM;

    for (StorageContainerManager scm: scmsToSendTo) {
      SCMHeartbeatResponseProto responseProto = scm.getDatanodeProtocolServer().sendHeartbeat(heartbeatProto);
      if (!responseProto.getCommandsList().isEmpty() && responseProto.getCommands(0).hasTerm()) {
        long term = responseProto.getCommands(0).getTerm();
        if (term >= maxTerm) {
          leaderResponseProto = responseProto;
          maxTerm = term;
        }
      }
    }

    return leaderResponseProto;
  }
}
