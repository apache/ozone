package org.apache.hadoop.hdds.utils;

import org.junit.jupiter.api.Test;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.HttpHeaders;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestHttpServletUtils {

  @Test
  public void testParseHeaders() throws Exception {
    HashMap<String, HttpServletUtils.ResponseFormat> verifyMap = new HashMap<>();
    verifyMap.put("text/plain", HttpServletUtils.ResponseFormat.XML);
    verifyMap.put(null, HttpServletUtils.ResponseFormat.XML);
    verifyMap.put("text/xml", HttpServletUtils.ResponseFormat.XML);
    verifyMap.put("application/xml", HttpServletUtils.ResponseFormat.XML);
    verifyMap.put("application/json", HttpServletUtils.ResponseFormat.JSON);

    HttpServletRequest request = mock(HttpServletRequest.class);
    for (Map.Entry<String, HttpServletUtils.ResponseFormat> entry : verifyMap.entrySet()) {
      HttpServletUtils.ResponseFormat contenTypeActual = entry.getValue();
      when(request.getHeader(HttpHeaders.ACCEPT))
          .thenReturn(entry.getKey());
      assertEquals(contenTypeActual,
          HttpServletUtils.parseAcceptHeader(request));
    }
  }
}
