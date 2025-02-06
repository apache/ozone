/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.DBColumnFamilyDefinition;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.hdds.utils.db.LongCodec;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.hdds.utils.db.StringCodec;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.hdds.utils.db.managed.ManagedCompactRangeOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedDBOptions;
//import org.rocksdb.CompactRangeOptions;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Tests OM failover protocols using a Mock Failover provider and a Mock OM
 * Protocol.
 */
public class TestOMFailovers {

  private static Codec<ContainerInfo> containerInfoCodec = new Codec<ContainerInfo>() {
    @Override
    public Class<ContainerInfo> getTypeClass() {
      return ContainerInfo.class;
    }

    @Override
    public byte[] toPersistedFormat(ContainerInfo object) throws IOException {
      return mapper.writeValueAsBytes(object);
    }

    @Override
    public ContainerInfo fromPersistedFormat(byte[] rawData) throws IOException {
      return mapper.readValue(rawData, ContainerInfo.class);
    }

    @Override
    public ContainerInfo copyObject(ContainerInfo object) {
      try {
        return fromPersistedFormat(toPersistedFormat(object));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  };
  private ManagedDBOptions dbOptions;
  private OzoneConfiguration conf = new OzoneConfiguration();
  private static final String DIRTREEDBNAME = "om.db";
  private static final ObjectMapper mapper = new ObjectMapper();
  private static final DBColumnFamilyDefinition<Long, ContainerInfo> DIRTREE_COLUMN_FAMILY
      = new DBColumnFamilyDefinition<>("metaTable", LongCodec.get(), containerInfoCodec);
  private static AtomicLong counter = new AtomicLong();
  private static AtomicLong prevCounter = new AtomicLong();

  public void testseparateDB() throws Exception {
    DBStore dirTreeDbStore = null;
    dirTreeDbStore = openDb(new File("/Users/sbalachandran/Documents/outputs"));
    if (dirTreeDbStore == null) {
      return;
    }
    List<ContainerProtos.ContainerDataProto.State> states =
        Arrays.asList(ContainerProtos.ContainerDataProto.State.OPEN, ContainerProtos.ContainerDataProto.State.CLOSING,
            ContainerProtos.ContainerDataProto.State.UNHEALTHY, ContainerProtos.ContainerDataProto.State.DELETED,
            ContainerProtos.ContainerDataProto.State.RECOVERING,
            ContainerProtos.ContainerDataProto.State.QUASI_CLOSED,
            ContainerProtos.ContainerDataProto.State.CLOSED);
    Map<ContainerProtos.ContainerDataProto.State, Integer> map = new HashMap<>();

    for (int i = 0; i < states.size(); i++) {
      map.put(states.get(i), i);
    }

    Table<Long, ContainerInfo> dirTreeTable = dirTreeDbStore.getTable(DIRTREE_COLUMN_FAMILY.getName(), Long.class,
        ContainerInfo.class);

    PrintWriter mismatchFile = new PrintWriter(new BufferedWriter(new FileWriter("/Users/sbalachandran/Documents/ozone/hadoop-ozone/integration-test/src/main/outputs/samefileMismatch.txt")));
    PrintWriter bcsmismatch = new PrintWriter(new BufferedWriter(new FileWriter("/Users/sbalachandran/Documents/ozone/hadoop-ozone/integration-test/src/main/outputs/bcsidmismatch.txt")));
    PrintWriter multipleOpen = new PrintWriter(new BufferedWriter(new FileWriter("/Users/sbalachandran/Documents/ozone/hadoop-ozone/integration-test/src/main/outputs/multipleopen.txt")));

    TableIterator<Long, ? extends Table.KeyValue<Long, ContainerInfo>> iterator = dirTreeTable.iterator();
    ThreadPoolExecutor pool = new ThreadPoolExecutor(30, 100, 1, TimeUnit.MINUTES, new ArrayBlockingQueue<>(2000));
    Queue<Future<?>> futures = new LinkedList<>();
    while (iterator.hasNext()) {
      if (futures.size() == 1000) {
        futures.poll().get();
      }
      Table.KeyValue<Long, ContainerInfo> next = iterator.next();
      ContainerInfo containerInfo = next.getValue();
      Long key = next.getKey();
      futures.add(pool.submit(() -> {
        System.out.println("Checking container " + key);
        long bcsId = -2;
        TreeMap<Long, Integer> containerStates = new TreeMap<>();
        for (Map.Entry<String, DNInfoState> entry: containerInfo.getDnInfo().entrySet()) {
          if (entry.getValue().getIsInOpen() > 1) {
            multipleOpen.println(key + "|" + entry.getKey() + "|" + readableOutput(entry.getValue()));
          }
          containerStates.compute(entry.getValue().getBcsId(),
              (k, v) -> v == null ?  map.get(entry.getValue().getState()) :
                  Math.max(map.get(entry.getValue().getState()), v));
        }
        if (containerStates.size() > 1) {
          long largestBcsId = containerStates.lastKey();
          long largestStateId = containerStates.values().stream().max(Integer::compareTo).get();
          if (containerStates.get(largestBcsId) < largestStateId) {
            bcsmismatch.println(key + "|" + containerStates.entrySet().stream().map(e -> e.getKey() + ":" + states.get(e.getValue()))
                .collect(Collectors.joining(",")));
          }
        }
      }));
    }
    while (futures.size() > 0) {
      futures.poll().get();
    }
    bcsmismatch.close();
    multipleOpen.close();
    mismatchFile.close();
    pool.shutdown();
  }

  public void erase(String dn) throws Exception {
    DBStore dirTreeDbStore = null;
    dirTreeDbStore = openDb(new File("/Users/sbalachandran/Documents/outputs"));
    if (dirTreeDbStore == null) {
      return;
    }
    Table<Long, ContainerInfo> dirTreeTable = dirTreeDbStore.getTable(DIRTREE_COLUMN_FAMILY.getName(), Long.class,
        ContainerInfo.class);

    TableIterator<Long, ? extends Table.KeyValue<Long, ContainerInfo>> iterator = dirTreeTable.iterator();
    while (iterator.hasNext()) {
      Table.KeyValue<Long, ContainerInfo> next = iterator.next();
      ContainerInfo containerInfo = next.getValue();
      if (containerInfo.getDnInfo().containsKey(dn)) {
        containerInfo.getDnInfo().remove(dn);
        dirTreeTable.put(next.getKey(), next.getValue());
      }
    }
  }

  public void run(File file, Table<Long, ContainerInfo> dirTreeTable, ConcurrentHashMap lock,
                  RDBStore dirTreeDbStore, PrintWriter errorFiles) throws IOException {
    BufferedReader reader = new BufferedReader(new FileReader(file));
    String line = reader.readLine();
    String dn = file.getName().split("-")[2];
    System.out.println(dn);
    long cnt =0;
    while (line != null) {
      // read next line
      line = reader.readLine();
      if (null == line)
        continue;
      cnt+=1;
      String[] split = line.split("\\|");
      if (split.length >= 7) {
        try {
          String time = split[0];
          long id = Long.parseLong(split[2].split("=")[1].split("\\s+")[0]);
          int replicaIndex = Integer.parseInt(split[3].split("=")[1].split("\\s+")[0]);
          long bcsId = Long.parseLong(split[4].split("=")[1].split("\\s+")[0]);
          ContainerProtos.ContainerDataProto.State state =
              ContainerProtos.ContainerDataProto.State.valueOf(split[5].split("=")[1].split("\\s+")[0]);
          String reason = String.join(" ", split[6].split("\\s+"));
          if (lock != null) {
            lock.compute(id, (k1, v1) -> {
              try {
                ContainerInfo containerInfo = dirTreeTable.get(id);
                if (containerInfo == null) {
                  containerInfo = new ContainerInfo();
                }

                if (state == ContainerProtos.ContainerDataProto.State.DELETED) {
                  containerInfo.getDnInfo().compute(dn, (k, v) -> null);
                }

                DNInfoState dnInfoState =
                    containerInfo.getDnInfo().compute(dn, (k, v) -> v == null ? new DNInfoState() : v);
                dnInfoState.setBcsId(bcsId);
                dnInfoState.setState(state);
                dnInfoState.setReplicaIndex(replicaIndex);
                if (state == ContainerProtos.ContainerDataProto.State.CLOSED) {
                  dnInfoState.setClosedBcsId(bcsId);
                }

                if (state == ContainerProtos.ContainerDataProto.State.OPEN) {
                  dnInfoState.setIsInOpen(dnInfoState.getIsInOpen() + 1);
                }

                if (state == ContainerProtos.ContainerDataProto.State.QUASI_CLOSED) {
                  dnInfoState.setIsQuasiClosed(dnInfoState.getIsQuasiClosed() + 1);
                }

                dnInfoState.setTransition(dnInfoState.getTransition() + state + " -> ");
                if (!reason.isEmpty()) {
                  dnInfoState.setStrError(dnInfoState.getStrError() + time + "\t" + reason + "\n");
                }
                dirTreeTable.put(id, containerInfo);
              } catch (IOException e) {
                throw new UncheckedIOException(e);
              }
              return null;
            });
          }
        } catch (Exception e) {
          System.out.println("Skipping Line : " + file.getAbsolutePath() + "\t" + line);
          errorFiles.println("Skipping Line : " + file.getAbsolutePath() + "\t" + line);
          break;
        }
      }
    }
    System.out.println(dn + "\t" + counter.addAndGet(cnt));
    reader.close();
    lock.compute("lock", (k,v) -> {
      try {
        dirTreeDbStore.flushLog(true);
        dirTreeDbStore.flushDB();
        try (ManagedCompactRangeOptions options = new ManagedCompactRangeOptions()) {
//          options.setBottommostLevelCompaction(CompactRangeOptions.BottommostLevelCompaction.kForce);
          ((RDBStore)dirTreeDbStore).compactDB();
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return null;
    });

  }

  String readableOutput(DNInfoState state) {
    String sep = ",";
    String val = state.getIsInOpen() + sep + state.getState() + sep + state.getBcsId() + sep + state.getIsQuasiClosed()
        + sep + state.getTransition() + sep + state.getStrError().replace("\n", "<-->");
    return val;
  }

  public void test123() throws Exception {
    BufferedReader reader;

    RDBStore dirTreeDbStore = null;
    dirTreeDbStore = openDb(new File("/Users/sbalachandran/Documents/outputs"));
    if (dirTreeDbStore == null) {
      return;
    }
    Table<Long, ContainerInfo> dirTreeTable = dirTreeDbStore.getTable(DIRTREE_COLUMN_FAMILY.getName(), Long.class,
        ContainerInfo.class);
    ExecutorService pool = new ThreadPoolExecutor(30, 100, 1, TimeUnit.MINUTES, new ArrayBlockingQueue<>(1000));
    ConcurrentHashMap<Long, ContainerInfo> lock = new ConcurrentHashMap<>();
    List<Future<Exception>> futures = new ArrayList<>();
    try (PrintWriter errorFiles = new PrintWriter(new BufferedWriter(new FileWriter("/Users/sbalachandran/Documents" +
        "/ozone/hadoop-ozone/integration-test/src/main/outputs/error.txt")))) {
      for (File file :
          Objects.requireNonNull(Arrays.stream(new File("/Users/sbalachandran/Documents/code/dummyrocks/dn_container_log")
              .listFiles(pathname -> pathname.getName().startsWith("dn-container.log"))).sorted()
              .sorted(Comparator.comparing(File::getName)).collect(Collectors.toList()))) {
        RDBStore finalDirTreeDbStore = dirTreeDbStore;
        futures.add(pool.submit(() -> {
          try {
            run(file, dirTreeTable, null, finalDirTreeDbStore, errorFiles);
          } catch (Exception e) {
            System.out.println(file.getAbsolutePath() + "\t" + e);
            e.printStackTrace();
            errorFiles.println(file.getAbsolutePath() + "\t" + e.toString());
          }
          return null;
        }));
      }
      for (Future<Exception> future : futures) {
        Exception e = future.get();
        if ( e != null) {
          throw new RuntimeException(e);
        }
      }
    } finally {
      if (dirTreeDbStore != null) {
        dirTreeDbStore.close();
        dbOptions.close();
        pool.shutdown();
      }
    }
  }

  private RDBStore openDb(File omPath) {
    omPath.mkdirs();
    File dirTreeDbPath = new File(omPath, DIRTREEDBNAME);
    System.err.println("Creating database of dir tree path at " + dirTreeDbPath);
    try {
      // Delete the DB from the last run if it exists.
      //if (dirTreeDbPath.exists()) {
      //  FileUtils.deleteDirectory(dirTreeDbPath);
      //}
      // ConfigurationSource conf = new OzoneConfiguration();
      DBStoreBuilder dbStoreBuilder = DBStoreBuilder.newBuilder(conf);
      dbStoreBuilder.setName(dirTreeDbPath.getName());
      dbStoreBuilder.setPath(dirTreeDbPath.getParentFile().toPath());
      dbStoreBuilder.addTable(DIRTREE_COLUMN_FAMILY.getName());
      dbStoreBuilder.addCodec(DIRTREE_COLUMN_FAMILY.getKeyType(), DIRTREE_COLUMN_FAMILY.getKeyCodec());
      dbStoreBuilder.addCodec(DIRTREE_COLUMN_FAMILY.getValueType(), DIRTREE_COLUMN_FAMILY.getValueCodec());
      dbOptions = new ManagedDBOptions();
      dbOptions.setWalSizeLimitMB(100);
      dbOptions.setKeepLogFileNum(10);
      dbOptions.setMaxTotalWalSize(1000000000);
      dbOptions.setCreateIfMissing(true);
      dbOptions.setCreateMissingColumnFamilies(true);
      dbStoreBuilder.setDBOptions(dbOptions);
      return (RDBStore) dbStoreBuilder.build();
    } catch (IOException e) {
      System.err.println("Error creating omdirtree.db " + e);
      return null;
    }
  }
  
  public static class ContainerInfo {
    private Map<String, DNInfoState> dnInfo = new HashMap();
    private int status = 0;

    public Map<String, DNInfoState> getDnInfo() {
      return dnInfo;
    }

    public void setDnInfo(Map<String, DNInfoState> dnInfo) {
      this.dnInfo = dnInfo;
    }

    public int getStatus() {
      return status;
    }

    public void setStatus(int status) {
      this.status = status;
    }
  }
  
  public static class DNInfoState {
    private int errorType = 0;
    private String strError = "";
    private long bcsId = -1;
    private ContainerProtos.ContainerDataProto.State state;
    private int replicaIndex = 0;
    private int isInOpen = 0;
    private int isQuasiClosed = 0;
    private long closedBcsId = -1;
    private String transition = "";

    public int getErrorType() {
      return errorType;
    }

    public void setErrorType(int errorType) {
      this.errorType = errorType;
    }

    public String getStrError() {
      return strError;
    }

    public void setStrError(String strError) {
      this.strError = strError;
    }

    public long getBcsId() {
      return bcsId;
    }

    public void setBcsId(long bcsId) {
      this.bcsId = bcsId;
    }

    public void setReplicaIndex(int replicaIndex) {
      this.replicaIndex = replicaIndex;
    }

    public ContainerProtos.ContainerDataProto.State getState() {
      return state;
    }

    public void setState(ContainerProtos.ContainerDataProto.State state) {
      this.state = state;
    }

    public int getIsInOpen() {
      return isInOpen;
    }

    public void setIsInOpen(int isInOpen) {
      this.isInOpen = isInOpen;
    }

    public int getIsQuasiClosed() {
      return isQuasiClosed;
    }

    public void setIsQuasiClosed(int isQuasiClosed) {
      this.isQuasiClosed = isQuasiClosed;
    }

    public long getClosedBcsId() {
      return closedBcsId;
    }

    public void setClosedBcsId(long closedBcsId) {
      this.closedBcsId = closedBcsId;
    }

    public String getTransition() {
      return transition;
    }

    public void setTransition(String transition) {
      this.transition = transition;
    }
  }

  public static void main(String[] args) throws Exception {
    new TestOMFailovers().testseparateDB();
  }
}
