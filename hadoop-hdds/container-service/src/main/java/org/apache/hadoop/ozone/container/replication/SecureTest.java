package org.apache.hadoop.ozone.container.replication;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CompletableFuture;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;

public class SecureTest {

  public static void main(String[] args) throws Exception {

    OzoneConfiguration config = new OzoneConfiguration();
    final SecurityConfig securityConfig = new SecurityConfig(config);
    final GrpcReplicationClient grpcReplicationClient =
        new GrpcReplicationClient("192.168.192.3", 9859, Paths.get("/tmp/test"),
            securityConfig, null);
    final CompletableFuture<Path> download = grpcReplicationClient.download(1L);
    System.out.println(download.get());

  }

}
