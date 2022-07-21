package org.apache.hadoop.ozone.freon;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.UniformReservoir;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.om.helpers.ServiceInfo;
import org.apache.hadoop.ozone.util.ShutdownHookManager;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.VersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.hadoop.ozone.conf.OzoneServiceConfig.DEFAULT_SHUTDOWN_HOOK_PRIORITY;

@CommandLine.Command(name = "om-rpc-load",
        aliases = "orpcl",
        description = "Generate random RPC request to the OM with or without layload.",
        versionProvider = HddsVersionProvider.class,
        mixinStandardHelpOptions = true,
        showDefaultValues = true)
public class OmRPCLoadGenerator extends BaseFreonGenerator
        implements Callable<Void> {

    @CommandLine.ParentCommand
    private Freon freon;

    enum FreonOps {
        op1,
        op2,
        op3,
        op4
    }

    private static final Logger LOG =
            LoggerFactory.getLogger(OmRPCLoadGenerator.class);
    private volatile Throwable exception;

    @CommandLine.Option(names = {"--threads"},
            description = "Specifies numbers of thread to generate RPC request",
            defaultValue = "1")
    private int numOfThreads = 1;

    @CommandLine.Option(names = {"--payload"},
            description = "Specifies the size of payload in bytes in each RPC request",
            defaultValue = "1024")
    private int payloadSize = 1024;

    private long startTime;
    private int threadPoolSize;
    private OzoneClient ozoneClient;
    private ExecutorService executor;
    private AtomicLong totalBytesWritten;
    private OzoneConfiguration ozoneConfiguration;
    private ArrayList<Histogram> histograms = new ArrayList<>();


    OmRPCLoadGenerator(OzoneConfiguration ozoneConfiguration) {
        this.ozoneConfiguration = ozoneConfiguration;
    }

    public void init(OzoneConfiguration configuration) throws IOException {
        totalBytesWritten = new AtomicLong();
        ozoneClient = OzoneClientFactory.getRpcClient(configuration);
//        objectStore = ozoneClient.getObjectStore();
//        for (RandomKeyGenerator.FreonOps ops : RandomKeyGenerator.FreonOps.values()) {
//            histograms.add(ops.ordinal(), new Histogram(new UniformReservoir()));
//        }
        if (freon != null) {
            freon.startHttpServer();
        }
    }

    @Override
    public Void call() throws Exception {

        if (ozoneConfiguration == null) {
            ozoneConfiguration = freon.createOzoneConfiguration();
        }
        init(ozoneConfiguration);


        LOG.info("Number of Threads: {}", numOfThreads);
        threadPoolSize = numOfThreads;
        executor = Executors.newFixedThreadPool(threadPoolSize);
        addShutdownHook();

        LOG.info("RPC request payload size: {} bytes", payloadSize);
        startTime = System.nanoTime();

        //TODO:: create new API specifically for RPC performance evaluation in OM
        for (int i = 0; i < numOfThreads; i++) {
            executor.execute(new RPCRequester());
        }


        ozoneClient.close();
        return null;
    }

    private void addShutdownHook() {
        ShutdownHookManager.get().addShutdownHook(() -> {
            printStats(System.out);
            if (freon != null) {
                freon.stopHttpServer();
            }
        }, DEFAULT_SHUTDOWN_HOOK_PRIORITY);
    }

    private void printStats(PrintStream out) {
        long execTime = System.nanoTime() - startTime;
        String prettyExecTime = DurationFormatUtils
                .formatDuration(TimeUnit.NANOSECONDS.toMillis(execTime),
                        DURATION_FORMAT);

        long averageRPCRoundTripResponseTime =
                TimeUnit.NANOSECONDS.toMillis(rpcResponseTime.get())
                        / threadPoolSize;
        String prettyAverageRPCRoundTripResponseTime = DurationFormatUtils
                .formatDuration(averageRPCRoundTripResponseTime, DURATION_FORMAT);

        out.println();
        out.println("***************************************************");
        out.println("Status: " + (exception != null ? "Failed" : "Success"));
        out.println("Git Base Revision: " + VersionInfo.getRevision());
        out.println("Number of thread created: " + threadPoolSize);
        out.println("Size of payload in each RPC request created: " + payloadSize);
        out.println(
                "Average Time spent in RPC round trip between client and OM: " + prettyAverageRPCRoundTripResponseTime);
        out.println("Total Execution time: " + prettyExecTime);
        out.println("***************************************************");

//        if (jsonDir != null) {
//            String[][] quantileTime =
//                    new String[OmRPCLoadGenerator.FreonOps.values().length][QUANTILES + 1];
//            String[] deviations = new String[OmRPCLoadGenerator.FreonOps.values().length];
//            String[] means = new String[OmRPCLoadGenerator.FreonOps.values().length];
//            for (OmRPCLoadGenerator.FreonOps ops : OmRPCLoadGenerator.FreonOps.values()) {
//                Snapshot snapshot = histograms.get(ops.ordinal()).getSnapshot();
//                for (int i = 0; i <= QUANTILES; i++) {
//                    quantileTime[ops.ordinal()][i] = DurationFormatUtils.formatDuration(
//                            TimeUnit.NANOSECONDS
//                                    .toMillis((long) snapshot.getValue((1.0 / QUANTILES) * i)),
//                            DURATION_FORMAT);
//                }
//                deviations[ops.ordinal()] = DurationFormatUtils.formatDuration(
//                        TimeUnit.NANOSECONDS.toMillis((long) snapshot.getStdDev()),
//                        DURATION_FORMAT);
//                means[ops.ordinal()] = DurationFormatUtils.formatDuration(
//                        TimeUnit.NANOSECONDS.toMillis((long) snapshot.getMean()),
//                        DURATION_FORMAT);
//            }
//
//            OmRPCLoadGenerator.FreonJobInfo jobInfo = new OmRPCLoadGenerator.FreonJobInfo().setExecTime(execTime)
//                    .setGitBaseRevision(VersionInfo.getRevision())
//                    .setMeanRPCResponseTime(means[OmRPCLoadGenerator.FreonOps.GET_RPC_RESPONSE_TIME.ordinal()])
//                    .setDeviationVolumeCreateTime(
//                            deviations[OmRPCLoadGenerator.FreonOps.GET_RPC_RESPONSE_TIME.ordinal()])
//                    .setTenQuantileVolumeCreateTime(
//                            quantileTime[OmRPCLoadGenerator.FreonOps.GET_RPC_RESPONSE_TIME.ordinal()])
//            String jsonName =
//                    new SimpleDateFormat("yyyyMMddHHmmss").format(Time.now()) + ".json";
//            String jsonPath = jsonDir + "/" + jsonName;
//            try (FileOutputStream os = new FileOutputStream(jsonPath)) {
//                ObjectMapper mapper = new ObjectMapper();
//                mapper.setVisibility(PropertyAccessor.FIELD,
//                        JsonAutoDetect.Visibility.ANY);
//                ObjectWriter writer = mapper.writerWithDefaultPrettyPrinter();
//                writer.writeValue(os, jobInfo);
//            } catch (FileNotFoundException e) {
//                out.println("Json File could not be created for the path: " + jsonPath);
//                out.println(e);
//            } catch (IOException e) {
//                out.println("Json object could not be created");
//                out.println(e);
//            }
//        }
    }

    private class RPCRequester implements Runnable {
        // Send a RPC request to OM to measure the time of round trip between OM client and OM
        @Override
        public void run() {
            OzoneManagerVersion omv =
                    OzoneManagerVersion
                            .fromProtoValue(s.getProtobuf().getOMVersion());
        }
    }


}


