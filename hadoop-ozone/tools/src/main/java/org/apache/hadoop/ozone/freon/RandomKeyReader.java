package org.apache.hadoop.ozone.freon;

import com.codahale.metrics.Timer;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import picocli.CommandLine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Reads keys from random volume/bucket pairs.
 */
@CommandLine.Command(name = "random-key-reader",
        aliases = "rkr",
        description = "Read keys from random volume/buckets",
        versionProvider = HddsVersionProvider.class,
        mixinStandardHelpOptions = true,
        showDefaultValues = true)
public class RandomKeyReader extends BaseFreonGenerator
        implements Callable<Void> {
  @CommandLine.Option(
          names = "--om-service-id",
          description = "OM Service ID"
  )
  private String omServiceID = null;
  private OzoneClient[] ozoneClients;
  private int clientCount;
  private Timer timer;
  private AtomicInteger readKeyCount = new AtomicInteger();
  private ArrayList<Pair<OzoneVolume, OzoneBucket>> volBuckPairs;

  @Override
  public Void call() throws Exception {
    init();
    setupClients();
    gatherVolumeBucketInfo();
    runTests(this::readRandomKeys);
    teardownClients();
    report();
    return null;
  }

  public void init() {
    super.init();
    timer = getMetrics().timer("key-read");
  }

  private void setupClients() throws Exception {
    OzoneConfiguration ozoneConfiguration = createOzoneConfiguration();
    clientCount = getThreadNo();
    ozoneClients = new OzoneClient[clientCount];
    for (int i = 0; i < clientCount; i++) {
      ozoneClients[i] = createOzoneClient(omServiceID, ozoneConfiguration);
    }
  }

  private void gatherVolumeBucketInfo() throws IOException {
    if (ozoneClients.length == 0) {
      throw new IllegalStateException("No clients available for gathering volume-bucket information.");
    }

    OzoneClient client = ozoneClients[0];
    Iterator<? extends OzoneVolume> volumes = client.getObjectStore().listVolumes("");

    if (!volumes.hasNext()) {
      throw new RuntimeException("There is no volume.");
    }

    volBuckPairs = new ArrayList<>();
    volumes.forEachRemaining(vol ->
            vol.listBuckets("").forEachRemaining(buck -> {
              Pair<OzoneVolume, OzoneBucket> pair = Pair.of(vol, buck);
              volBuckPairs.add(pair);
            })
    );

    if (volBuckPairs.isEmpty()) {
      throw new RuntimeException("No bucket exists in any volume. Create a bucket.");
    }
  }

  private void teardownClients() {
    for (int i = 0; i < clientCount; i++) {
      try {
        if (ozoneClients[i] != null) {
          ozoneClients[i].close();
        }
      } catch (IOException e) {
        System.err.println("Error while closing client " + i + ": " + e.getMessage());
      }
    }
  }

  private void report() {
    String report = "-- Result ----------------------------------------------------------------------\n" +
            "The number of keys read: " + readKeyCount.get();
    System.out.println(report);
  }

  public void readRandomKeys(long count) {
    timer.time(() -> {
      try {
        int randomPairIndex = new Random().nextInt(volBuckPairs.size());
        Pair<OzoneVolume, OzoneBucket> pair = volBuckPairs.get(randomPairIndex);
        Iterator<? extends OzoneKey> keyIterator = pair.getRight().listKeys("", "");

        while (keyIterator.hasNext()) {
          String keyName = keyIterator.next().getName();
          try (OzoneInputStream stream = pair.getRight().readKey(keyName)) {
            IOUtils.consume(stream);
            readKeyCount.incrementAndGet();
          }
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }
}
