package org.apache.hadoop.hdds.server.http;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.eclipse.jetty.server.ServerConnector;
import org.junit.Test;

import java.net.URI;

import static org.junit.Assert.assertEquals;

public class TestHttpServer2 {

  @Test
  public void testIdleTimeout() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    URI uri = URI.create("https://example.com/");

    HttpServer2 srv = new HttpServer2.Builder()
            .setConf(conf)
            .setName("test")
            .addEndpoint(uri)
            .build();
    for (ServerConnector server : srv.getListeners()) {
      // Check default value in ozone-default.xml
      assertEquals(60000, server.getIdleTimeout());
    }
  }
}
