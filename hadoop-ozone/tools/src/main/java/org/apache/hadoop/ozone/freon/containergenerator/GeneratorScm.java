package org.apache.hadoop.ozone.freon.containergenerator;

import java.util.concurrent.Callable;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.hdds.utils.db.Table;

import com.codahale.metrics.Timer;
import static org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition.CONTAINERS;
import picocli.CommandLine.Command;

@Command(name = "crscm",
    description = "Offline container metadata generator for Storage Conainer "
        + "Manager",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)
public class GeneratorScm extends BaseGenerator {

  private DBStore scmDb;

  private Table<ContainerID, ContainerInfo> containerStore;

  private Timer timer;

  @Override
  public Void call() throws Exception {
    init();

    ConfigurationSource config = createOzoneConfiguration();

    scmDb = DBStoreBuilder.createDBStore(config, new SCMDBDefinition());

    containerStore = CONTAINERS.getTable(scmDb);

    timer = getMetrics().timer("scm-generator");
    runTests(this::writeScmData);
    scmDb.close();
    return null;
  }


  private void writeScmData(long index) throws Exception {
    timer.time((Callable<Void>) () -> {
      long containerId = getContainerIdOffset() + index;
      // SCM
      ContainerInfo containerInfo =
          new ContainerInfo.Builder()
              .setContainerID(containerId)
              .setState(LifeCycleState.CLOSED)
              .setReplicationFactor(ReplicationFactor.THREE)
              .setReplicationType(ReplicationType.STAND_ALONE)
              .setOwner(getUserId())
              .build();

      containerStore.put(new ContainerID(containerId), containerInfo);
      return null;
    });

  }

}
