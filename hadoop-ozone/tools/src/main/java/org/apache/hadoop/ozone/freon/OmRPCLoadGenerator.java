package org.apache.hadoop.ozone.freon;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.UniformReservoir;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.om.helpers.ServiceInfo;
import org.apache.hadoop.ozone.om.helpers.ServiceInfoEx;
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
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.hadoop.ozone.conf.OzoneServiceConfig.DEFAULT_SHUTDOWN_HOOK_PRIORITY;

@CommandLine.Command(name = "om-rpc-load",
        aliases = "omrpcl",
        description = "Generate random RPC request to the OM with or without layload.",
        versionProvider = HddsVersionProvider.class,
        mixinStandardHelpOptions = true,
        showDefaultValues = true)
public class OmRPCLoadGenerator extends BaseFreonGenerator
        implements Callable<Void> {

    private static final Logger LOG =
            LoggerFactory.getLogger(OmRPCLoadGenerator.class);

    @CommandLine.Option(names = {"--payload"},
            description = "Specifies the size of payload in bytes in each RPC request",
            defaultValue = "1024")
    private int payloadSize = 1024;

    @CommandLine.Option(names = {"--empty-req"},
            description = "Specifies whether the payload of request is empty or not",
            defaultValue = "False")
    private boolean isEmptyReq = false;

    @CommandLine.Option(names = {"--empty-resp"},
            description = "Specifies whether the payload of response is empty or not",
            defaultValue = "False")
    private boolean isEmptyResp = false;

//    OmRPCLoadGenerator(OzoneConfiguration ozoneConfiguration) {
//        this.ozoneConfiguration = ozoneConfiguration;
//    }

    @Override
    public Void call() throws Exception {

        init();
        OzoneConfiguration configuration = createOzoneConfiguration();
//        fileSystem = FileSystem.get(URI.create(rootPath), configuration);
//        contentGenerator = new ContentGenerator(fileSizeInBytes, bufferSize);
//        timer = getMetrics().timer("file-create");

//        if (ozoneConfiguration == null) {
//            ozoneConfiguration = freon.createOzoneConfiguration();
//        }
//        init(ozoneConfiguration);
//
//        LOG.info("Number of Threads: {}", numOfThreads);
//        threadPoolSize = numOfThreads;
//        executor = Executors.newFixedThreadPool(threadPoolSize);
//        addShutdownHook();
//
//        LOG.info("RPC request payload size: {} bytes", payloadSize);
//        startTime = System.nanoTime();
//
        int numOfThread = getThreadNo();
        for (int i = 0; i < numOfThread; i++) {
            runTests(this::sendRPCReq);
        }
        return null;
    }

    private void sendRPCReq(long l) throws Exception {
        OzoneConfiguration configuration = createOzoneConfiguration();
        OzoneClient client = createOzoneClient(null, configuration);
        ServiceInfoEx serviceInfo = client.getProxy().getOzoneManagerClient().getServiceInfo();

        LOG.info("###########################################");
        LOG.info("###########################################");
        LOG.info(serviceInfo.toString());
        LOG.info("###########################################");
        LOG.info("###########################################");
    }

    // Send a RPC request to OM to measure the time of round trip between OM client and OM
//        public void sendRPCReq(long l) {
//            OzoneManagerVersion omv =
//                    OzoneManagerVersion
//                            .fromProtoValue(s.getProtobuf().getOMVersion());
//
//            createOmClient()
//        }

}


