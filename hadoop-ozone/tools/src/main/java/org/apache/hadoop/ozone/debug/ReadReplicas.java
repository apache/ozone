package org.apache.hadoop.ozone.debug;

import com.google.gson.*;
import org.apache.hadoop.hdds.cli.SubcommandWithParent;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientException;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.common.OzoneChecksumException;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import org.apache.hadoop.ozone.shell.keys.KeyHandler;
import org.kohsuke.MetaInfServices;
import picocli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * Class that for a given key downloads every blocks every replica and creates
 * a manifest file with information about the replicas.
 */
@CommandLine.Command(name = "read-replicas",
    description = "Reads a given keys every blocks every replica.")
@MetaInfServices(SubcommandWithParent.class)
public class ReadReplicas extends KeyHandler implements SubcommandWithParent {

  private ClientProtocol clientProtocol;

  @Override
  public Class<?> getParentType() {
    return OzoneDebug.class;
  }

  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
      throws IOException, OzoneClientException {

    clientProtocol = client.getObjectStore().getClientProxy();

    address.ensureKeyAddress();
    String volumeName = address.getVolumeName();
    String bucketName = address.getBucketName();
    String keyName = address.getKeyName();

    OzoneKeyDetails keyInfoDetails
        = clientProtocol.getKeyDetails(volumeName, bucketName, keyName);

    Map<OmKeyLocationInfo, Map<DatanodeDetails, OzoneInputStream>> replicas
        = clientProtocol.getKeysEveryReplicas(volumeName, bucketName, keyName);


    String fileSuffix
        = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
    String directoryName = volumeName + "_" + bucketName + "_" + keyName +
        "_" + fileSuffix;
    System.out.println("Creating directory : " + directoryName);
    File dir = new File("/opt/hadoop/" + directoryName);
    if (!dir.exists()){
      if(dir.mkdir()) {
        System.out.println("Successfully created!");
      } else {
        System.out.println("Something went wrong.");
      }
    }

    JsonObject result = new JsonObject();
    result.addProperty("filename",
        volumeName + "/" + bucketName + "/" + keyName);
    result.addProperty("datasize", keyInfoDetails.getDataSize());

    JsonArray blocks = new JsonArray();
    int blockIndex = 0;

    for (Map.Entry<OmKeyLocationInfo, Map<DatanodeDetails, OzoneInputStream>>
        block : replicas.entrySet()) {
      JsonObject blockJson = new JsonObject();
      JsonArray replicasJson = new JsonArray();

      blockIndex += 1;
      blockJson.addProperty("block index", blockIndex);
      blockJson.addProperty("container id", block.getKey().getContainerID());
      blockJson.addProperty("local id", block.getKey().getLocalID());
      blockJson.addProperty("length", block.getKey().getLength());
      blockJson.addProperty("offset", block.getKey().getOffset());

      for (Map.Entry<DatanodeDetails, OzoneInputStream>
          replica : block.getValue().entrySet()) {
        JsonObject replicaJson = new JsonObject();

        replicaJson.addProperty("hostname",
            replica.getKey().getHostName());
        replicaJson.addProperty("uuid",
            replica.getKey().getUuidString());

        OzoneInputStream is = replica.getValue();
        String fileName = keyName + "_block" + blockIndex + "_" +
            replica.getKey().getHostName();
        System.out.println("Writing : " + fileName);
        File replicaFile
            = new File("/opt/hadoop/" + directoryName + "/" + fileName);

        try {
          Files.copy(is, replicaFile.toPath(),
              StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
          replicaJson.addProperty("exception", e.getMessage());
        } finally {
          is.close();
        }

        replicasJson.add(replicaJson);
      }
      blockJson.add("replicas", replicasJson);
      blocks.add(blockJson);
    }
    result.add("blocks", blocks);
    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    String prettyJson = gson.toJson(result);

    String manifestFileName = keyName + "_manifest";
    System.out.println("Writing manifest file : " + manifestFileName);
    File manifestFile
        = new File("/opt/hadoop/" + directoryName + "/" + manifestFileName);
    Files.write(manifestFile.toPath(), prettyJson.getBytes());
  }
}
