package org.apache.hadoop.fs.ozone;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DelegateToFileSystem;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.ozone.OzoneConsts;

/**
 * ozone implementation of AbstractFileSystem.
 * This impl delegates to the OzoneFileSystem
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class OzFs extends DelegateToFileSystem {

  public OzFs(URI theUri, Configuration conf)
      throws IOException, URISyntaxException {
    super(theUri, new OzoneFileSystem(), conf,
        OzoneConsts.OZONE_URI_SCHEME, false);
  }
}
