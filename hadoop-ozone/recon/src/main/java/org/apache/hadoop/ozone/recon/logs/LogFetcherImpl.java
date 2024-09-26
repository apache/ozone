package org.apache.hadoop.ozone.recon.logs;


/**
 * This class will be used to implement the API call actions to /log endpoint
 */
public class LogFetcherImpl implements LogFetcher{

  @Override
  public String getLogs(String timestamp, int direction, int lines) {
    //TODO: Implement the log fetcher
    return null;
  }

  @Override
  public String getLogs(long offset, int direction, int lines) {
    //TODO: Implement the log fetcher
    return null;
  }
}
