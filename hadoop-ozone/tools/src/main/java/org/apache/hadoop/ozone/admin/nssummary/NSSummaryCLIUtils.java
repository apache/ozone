package org.apache.hadoop.ozone.admin.nssummary;

import com.google.gson.Gson;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import picocli.CommandLine.Help.Ansi;

import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static java.net.HttpURLConnection.HTTP_CREATED;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.apache.hadoop.ozone.recon.ReconUtils.getFileSizeUpperBound;

public class NSSummaryCLIUtils {

  private static CloseableHttpClient httpClient = HttpClientBuilder
      .create()
      .build();

  public static String makeHttpCall(String url, String path) throws Exception {
    return makeHttpCall(url, path, false, false);
  }

  public static String makeHttpCall(String url, String path,
                                        boolean listFile, boolean withReplica)
      throws Exception {
    List parameterList = new ArrayList();
    parameterList.add(new BasicNameValuePair("path", path));

    if (listFile) {
      parameterList.add(new BasicNameValuePair("files", "true"));
    }
    if (withReplica) {
      parameterList.add(new BasicNameValuePair("replica", "true"));
    }

    HttpGet httpGet = new HttpGet(url);
    URI uri = new URIBuilder(httpGet.getURI())
        .addParameters(parameterList)
        .build();
    httpGet.setURI(uri);

    try {
      HttpResponse response = httpClient.execute(httpGet);
      int errorCode = response.getStatusLine().getStatusCode();
      HttpEntity entity = response.getEntity();

      if ((errorCode == HTTP_OK) || (errorCode == HTTP_CREATED)) {
        return EntityUtils.toString(entity);
      }

      if (entity != null) {
        throw new IOException("Recon is being initialized..." +
            "\nPlease wait a moment.");
      } else {
        throw new IOException("Unexpected null in http payload," +
            " while processing request");
      }
    } catch (ConnectException ex) {
      System.err.println("Connection Refused. Please make sure the " +
          "Recon Server has been started.");
      return null;
    }
  }

  public static HashMap<String, Object> getResponseMap(String response) {
    return new Gson().fromJson(response, HashMap.class);
  }

  public static void printNewLines(int cnt) {
    for (int i = 0; i < cnt; ++i) {
      System.out.println();
    }
  }

  public static void printSpaces(int cnt) {
    for (int i = 0; i < cnt; ++i) {
      System.out.print(" ");
    }
  }

  public static void printEmptyPathRequest() {
    System.err.println("The path parameter is empty.\n" +
        "If you mean the root path, use / instead.");
  }

  public static void printPathNotFound() {
    System.err.println("Path not found in the system.\n" +
        "Did you remove any protocol prefix before the path?");
  }

  public static void printTypeNA(String requestType) {
    String markUp = "@|underline " + requestType + "|@";
    System.err.println("Path found in the system.\nBut the entity type " +
        "is not applicable to the " + Ansi.AUTO.string(markUp) + " request");
  }

  public static void printKVSeparator() {
    System.out.print(" : ");
  }

  public static void printWithUnderline(String str, boolean newLine) {
    String markupStr = "@|underline " + str + "|@";
    if (newLine) {
      System.out.println(Ansi.AUTO.string(markupStr));
    } else {
      System.out.print(Ansi.AUTO.string(markupStr));
    }
  }
}
