package org.apache.hadoop.hdds.utils;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.server.JsonUtils;
import org.apache.hadoop.util.XMLUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.IOException;
import java.io.Serializable;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;

public class HttpServletUtils implements Serializable {
    /**
     * Parse the Accept header to determine response format.
     *
     * @param request the HTTP servlet request
     * @return {@link ResponseFormat#JSON} if Accept header contains "application/json",
     * otherwise {@link ResponseFormat#XML} (default for backwards compatibility)
     * @see HttpHeaders#ACCEPT
     */
    @VisibleForTesting
    public static ResponseFormat parseAcceptHeader(HttpServletRequest request) {
        String format = request.getHeader(HttpHeaders.ACCEPT);
        MediaType mediaType = MediaType.valueOf(format);
        return format != null && format.contains(ResponseFormat.JSON.getValue()) ?
                ResponseFormat.JSON : ResponseFormat.XML;
    }

    /**
     * Guts of the servlet - extracted for easy testing.
     */
    public static void writeResponse(OzoneConfiguration conf,
                                     Writer out, ResponseFormat format, String propertyName)
            throws IOException, IllegalArgumentException {
        switch (format) {
            case JSON:
                OzoneConfiguration.dumpConfiguration(conf, propertyName, out);
                break;
            case XML:
            default:
                conf.writeXml(propertyName, out);
                break;
        }
    }

    /**
     * Write error response according to the specified format.
     *
     * @param errorMessage the error message
     * @param format       the response format
     * @param out          the writer
     */
    public static void writeErrorResponse(String errorMessage, ResponseFormat format, Writer out)
            throws IOException {
        switch (format) {
            case JSON:
                Map<String, String> errorMap = new HashMap<String, String>();
                errorMap.put("error", errorMessage);
                out.write(JsonUtils.toJsonString(errorMap));
                break;
            case XML:
            default:
                writeXmlError(errorMessage, out);
                break;
        }
    }

    public static void writeXmlError(String errorMessage, Writer out) throws IOException {
        try {
            DocumentBuilderFactory factory = XMLUtils.newSecureDocumentBuilderFactory();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.newDocument();

            Element root = doc.createElement("error");
            root.setTextContent(errorMessage);
            doc.appendChild(root);

            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer = transformerFactory.newTransformer();
            transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
            transformer.setOutputProperty(OutputKeys.STANDALONE, "no");

            DOMSource source = new DOMSource(doc);
            StreamResult result = new StreamResult(out);
            transformer.transform(source, result);
        } catch (ParserConfigurationException | TransformerException e) {
            throw new IOException("Failed to write XML error response", e);
        }
    }

    public enum ResponseFormat {
        JSON("json"),
        XML("xml");
        private final String value;

        ResponseFormat(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        @Override
        public String toString() {
            return value;
        }
    }
}
