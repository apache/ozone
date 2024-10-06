package org.apache.hadoop.ozone.freon;

import com.codahale.metrics.Timer;
import kotlin.Pair;
import org.apache.commons.io.IOUtils;
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
 * Reads keys from random volume/bucket pairs
 */
@CommandLine.Command(name = "random-key-reader",
        aliases = "rkr",
        description = "Read keys from random volume/buckets",
        mixinStandardHelpOptions = true)
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

  public Void call() throws Exception {
    init();
    OzoneConfiguration ozoneConfiguration = createOzoneConfiguration();
    clientCount = getThreadNo();
    ozoneClients = new OzoneClient[clientCount];
    for (int i = 0; i < clientCount; i++) {
      ozoneClients[i] = createOzoneClient(omServiceID, ozoneConfiguration);
    }
    timer = getMetrics().timer("key-read");

    runTests(this::readRandomKeys);
    for (int i = 0; i < clientCount; i++) {
      if (ozoneClients[i] != null) {
        ozoneClients[i].close();
      }
    }
    String report = "The number of keys read: "
            + readKeyCount.get();
    System.out.println(report);
    return null;
  }

  // Get volume list,
  public void readRandomKeys(long count) throws IOException {
    int clientIndex = (int) (count % clientCount);
    OzoneClient c = ozoneClients[clientIndex];
    timer.time(() -> {
      Iterator<? extends OzoneVolume> vols = null;
      try {
        vols = c.getObjectStore().listVolumes("");
        if (!vols.hasNext()) {
          throw new RuntimeException("There is no volume.");
        }
        ArrayList<Pair<OzoneVolume, OzoneBucket>> volBuckPairs = new ArrayList<>();
        vols.forEachRemaining(vol ->
                vol.listBuckets("").forEachRemaining(buck -> {
                  Pair<OzoneVolume, OzoneBucket> p = new Pair<>(vol, buck);
                  volBuckPairs.add(p);
                })
        );
        if (volBuckPairs.isEmpty()) {
          throw new RuntimeException("No bucket exists in any volume. Create a bucket ");
        }
        int randomPairInd = new Random().nextInt(volBuckPairs.size());
        Pair<OzoneVolume, OzoneBucket> pair = volBuckPairs.get(randomPairInd);
        Iterator<? extends OzoneKey> keyItereator = pair.getSecond().listKeys("", "");

        while (keyItereator.hasNext()) {
          String keyName = keyItereator.next().getName();
          OzoneInputStream stream = null;
          try {
            stream = pair.getSecond().readKey(keyName);
            IOUtils.consume(stream);
            readKeyCount.incrementAndGet();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }
}
