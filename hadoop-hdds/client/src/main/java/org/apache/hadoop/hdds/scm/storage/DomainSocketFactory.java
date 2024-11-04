/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.storage;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.net.unix.DomainSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

/**
 *  A factory to help create DomainSocket.
 */
public final class DomainSocketFactory implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(
      DomainSocketFactory.class);

  /**
   *  Domain socket path state.
   */
  public enum PathState {
    NOT_CONFIGURED(false),
    DISABLED(false),
    VALID(true);

    PathState(boolean usableForShortCircuit) {
      this.usableForShortCircuit = usableForShortCircuit;
    }

    public boolean getUsableForShortCircuit() {
      return usableForShortCircuit;
    }
    private final boolean usableForShortCircuit;
  }

  /**
   *  Domain socket path.
   */
  public static class PathInfo {
    private static final PathInfo NOT_CONFIGURED = new PathInfo("", PathState.NOT_CONFIGURED);
    private static final PathInfo DISABLED = new PathInfo("", PathState.DISABLED);
    private static final PathInfo VALID = new PathInfo("", PathState.VALID);

    private final String path;
    private final PathState state;

    PathInfo(String path, PathState state) {
      this.path = path;
      this.state = state;
    }

    public String getPath() {
      return path;
    }

    public PathState getPathState() {
      return state;
    }

    @Override
    public String toString() {
      return "PathInfo{path=" + path + ", state=" + state + "}";
    }
  }

  public static final String FEATURE = "short-circuit reads";
  public static final String FEATURE_FLAG = "SC";
  private static boolean nativeLibraryLoaded = false;
  private static String nativeLibraryLoadFailureReason;
  private long pathExpireMills;
  private final ConcurrentHashMap<String, PathInfo> pathMap;
  private Timer timer;
  private boolean isEnabled = false;
  private String domainSocketPath;

  static {
    // Try to load native hadoop library and set fallback flag appropriately
    if (SystemUtils.IS_OS_WINDOWS) {
      nativeLibraryLoadFailureReason = "UNIX Domain sockets are not available on Windows.";
    } else {
      LOG.info("Trying to load the custom-built native-hadoop library...");
      try {
        System.loadLibrary("hadoop");
        LOG.info("Loaded the native-hadoop library");
        nativeLibraryLoaded = true;
      } catch (Throwable t) {
        // Ignore failure to continue
        LOG.info("Failed to load native-hadoop with error: " + t);
        LOG.info("java.library.path=" + System.getProperty("java.library.path"));
        nativeLibraryLoadFailureReason = "libhadoop cannot be loaded.";
      }

      if (!nativeLibraryLoaded) {
        LOG.warn("Unable to load native-hadoop library for your platform... " +
            "using builtin-java classes where applicable");
      }
    }
  }

  private static volatile DomainSocketFactory instance = null;

  public static DomainSocketFactory getInstance(ConfigurationSource conf) {
    if (instance == null) {
      synchronized (DomainSocketFactory.class) {
        if (instance == null) {
          instance = new DomainSocketFactory(conf);
        }
      }
    }
    return instance;
  }

  private DomainSocketFactory(ConfigurationSource conf) {
    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    boolean shortCircuitEnabled = clientConfig.isShortCircuitEnabled();
    PathInfo pathInfo;
    long startTime = System.nanoTime();
    if (!shortCircuitEnabled) {
      LOG.info(FEATURE + " is disabled.");
      pathInfo = PathInfo.NOT_CONFIGURED;
    } else {
      domainSocketPath = conf.get(OzoneClientConfig.OZONE_DOMAIN_SOCKET_PATH,
          OzoneClientConfig.OZONE_DOMAIN_SOCKET_PATH_DEFAULT);
      if (domainSocketPath.isEmpty()) {
        throw new IllegalArgumentException(FEATURE + " is enabled but "
            + OzoneClientConfig.OZONE_DOMAIN_SOCKET_PATH + " is not set.");
      } else if (!nativeLibraryLoaded) {
        LOG.warn(FEATURE + " cannot be used because " + nativeLibraryLoadFailureReason);
        pathInfo = PathInfo.DISABLED;
      } else {
        pathInfo = PathInfo.VALID;
        isEnabled = true;
        timer = new Timer(DomainSocketFactory.class.getSimpleName() + "-Timer");
        LOG.info(FEATURE + " is enabled within {} ns.", System.nanoTime() - startTime);
      }
    }
    pathExpireMills = clientConfig.getShortCircuitReadDisableInterval() * 1000;
    pathMap = new ConcurrentHashMap<>();
    pathMap.put(domainSocketPath, pathInfo);
  }

  public boolean isServiceEnabled() {
    return isEnabled;
  }

  public boolean isServiceReady() {
    if (isEnabled) {
      PathInfo status = pathMap.get(domainSocketPath);
      return status.getPathState() == PathState.VALID;
    } else {
      return false;
    }
  }

  /**
   * Get information about a domain socket path. Caller must make sure that addr is a local address.
   *
   * @param addr         The local inet address to use.
   * @return             Information about the socket path.
   */
  public PathInfo getPathInfo(InetSocketAddress addr) {
    if (!isEnabled) {
      return PathInfo.NOT_CONFIGURED;
    }

    if (!isServiceReady()) {
      return PathInfo.DISABLED;
    }

    String escapedPath = DomainSocket.getEffectivePath(domainSocketPath, addr.getPort());
    PathInfo status = pathMap.get(escapedPath);
    if (status == null) {
      PathInfo pathInfo = new PathInfo(escapedPath, PathState.VALID);
      pathMap.putIfAbsent(escapedPath, pathInfo);
      return pathInfo;
    } else {
      return status;
    }
  }

  /**
   * Create DomainSocket for addr. Caller must make sure that addr is a local address.
   */
  public DomainSocket createSocket(int readTimeoutMs, int writeTimeoutMs, InetSocketAddress addr) throws IOException {
    if (!isEnabled || !isServiceReady()) {
      return null;
    }
    boolean success = false;
    DomainSocket sock = null;
    String escapedPath = null;
    long startTime = System.nanoTime();
    try {
      escapedPath = DomainSocket.getEffectivePath(domainSocketPath, addr.getPort());
      sock = DomainSocket.connect(escapedPath);
      sock.setAttribute(DomainSocket.RECEIVE_TIMEOUT, readTimeoutMs);
      sock.setAttribute(DomainSocket.SEND_TIMEOUT, writeTimeoutMs);
      success = true;
      LOG.info("{} is created within {} ns", sock, System.nanoTime() - startTime);
    } catch (IOException e) {
      LOG.error("Failed to create DomainSocket", e);
      throw e;
    } finally {
      if (!success) {
        if (sock != null) {
          IOUtils.closeQuietly(sock);
        }
        if (escapedPath != null) {
          pathMap.put(escapedPath, PathInfo.DISABLED);
          LOG.error("{} is disabled for {} ms due to current failure", escapedPath, pathExpireMills);
          schedulePathEnable(escapedPath, pathExpireMills);
        }
        sock = null;
      }
    }
    return sock;
  }

  public void disableShortCircuit() {
    pathMap.put(domainSocketPath, PathInfo.DISABLED);
    schedulePathEnable(domainSocketPath, pathExpireMills);
  }

  private void schedulePathEnable(String path, long delayMills) {
    timer.schedule(new TimerTask() {
      @Override
      public void run() {
        pathMap.put(path, PathInfo.VALID);
      }
    }, delayMills);
  }

  @VisibleForTesting
  public void clearPathMap() {
    pathMap.clear();
  }

  public long getPathExpireMills() {
    return pathExpireMills;
  }

  @Override
  public void close() {
    if (timer != null) {
      timer.cancel();
    }
  }
}
