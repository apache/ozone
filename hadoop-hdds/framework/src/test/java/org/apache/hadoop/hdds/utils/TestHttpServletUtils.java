package org.apache.hadoop.hdds.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
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

  public static Stream<Arguments> provideGetResponseFormatTestCases() {
    return Stream.of(
        Arguments.of("text/plain", HttpServletUtils.ResponseFormat.XML),
        Arguments.of(null, HttpServletUtils.ResponseFormat.UNSPECIFIED),
        Arguments.of("text/xml", HttpServletUtils.ResponseFormat.XML),
        Arguments.of("application/xml", HttpServletUtils.ResponseFormat.XML),
        Arguments.of("application/json", HttpServletUtils.ResponseFormat.JSON)
    );
  }

  @ParameterizedTest
  @MethodSource("provideGetResponseFormatTestCases")
  public void testGetResponseFormat(@Nullable String contentType,
                                    HttpServletUtils.ResponseFormat expectResponseFormat) {
    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getHeader(HttpHeaders.ACCEPT))
        .thenReturn(contentType);
    assertEquals(expectResponseFormat,
        HttpServletUtils.getResponseFormat(request));
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
