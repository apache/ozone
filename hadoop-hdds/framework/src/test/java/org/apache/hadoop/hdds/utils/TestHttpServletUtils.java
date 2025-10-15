package org.apache.hadoop.hdds.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.annotation.Nullable;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.stream.Stream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.HttpHeaders;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestHttpServletUtils {

  public static Stream<Arguments> provideParseHeadersTestCases() {
    return Stream.of(
        Arguments.of("text/plain", HttpServletUtils.ResponseFormat.XML, null),
        // if content type is null, an IllegalArgumentException is expected to be thrown.
        Arguments.of(null, null,  IllegalArgumentException.class),
        Arguments.of("text/xml", HttpServletUtils.ResponseFormat.XML, null),
        Arguments.of("application/xml", HttpServletUtils.ResponseFormat.XML,null),
        Arguments.of("application/json", HttpServletUtils.ResponseFormat.JSON, null)
    );
  }

  @ParameterizedTest
  @MethodSource("provideParseHeadersTestCases")
  public void testParseHeaders(@Nullable String contentType, HttpServletUtils.ResponseFormat expectResponseFormat, Class<? extends Throwable> expectedException) {
    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getHeader(HttpHeaders.ACCEPT))
        .thenReturn(contentType);
    if (expectedException == null) {
      assertEquals(expectResponseFormat,
          HttpServletUtils.parseAcceptHeader(request));
    } else {
      assertThrows(expectedException, () -> HttpServletUtils.parseAcceptHeader(request));
    }
  }

  @Test
  public void testWriteErrorResponse_JSON() throws Exception {
    StringWriter sw = new StringWriter();
    HttpServletResponse response = mock(HttpServletResponse.class);
    when(response.getWriter()).thenReturn(new PrintWriter(sw));
    HttpServletUtils.writeErrorResponse(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "example error",
        HttpServletUtils.ResponseFormat.JSON, response);
    assertEquals("{\"error\":\"example error\"}", sw.toString());
  }

  @Test
  public void testWriteErrorResponse_XML() throws Exception {
    StringWriter sw = new StringWriter();
    HttpServletResponse response = mock(HttpServletResponse.class);
    when(response.getWriter()).thenReturn(new PrintWriter(sw));
    HttpServletUtils.writeErrorResponse(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "example error",
        HttpServletUtils.ResponseFormat.XML, response);
    assertEquals("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><error>example error</error>",
        sw.toString());
  }
}
