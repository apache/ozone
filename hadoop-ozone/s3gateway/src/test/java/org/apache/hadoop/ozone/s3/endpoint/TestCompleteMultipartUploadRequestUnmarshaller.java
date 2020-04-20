package org.apache.hadoop.ozone.s3.endpoint;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Class tests Unmarshall logic of {@link CompleteMultipartUploadRequest}.
 */
public class TestCompleteMultipartUploadRequestUnmarshaller {

  private static String part1 = UUID.randomUUID().toString();
  private static String part2 = UUID.randomUUID().toString();
  @Test
  public void fromStreamWithNamespace() throws IOException {
    //GIVEN
    ByteArrayInputStream inputBody =
        new ByteArrayInputStream(
            ("<CompleteMultipartUpload xmlns=\"http://s3.amazonaws" +
                ".com/doc/2006-03-01/\">" +
                "<Part><ETag>" + part1 + "</ETag><PartNumber>1" +
                "</PartNumber></Part><Part><ETag>" + part2 +
                "</ETag><PartNumber>2</PartNumber></Part>" +
                "</CompleteMultipartUpload>")
                .getBytes(UTF_8));

    //WHEN
    CompleteMultipartUploadRequest completeMultipartUploadRequest =
        unmarshall(inputBody);

    //THEN
    checkContent(completeMultipartUploadRequest);
  }

  @Test
  public void fromStreamWithoutNamespace() throws IOException {
    //GIVEN
    ByteArrayInputStream inputBody =
        new ByteArrayInputStream(
            ("<CompleteMultipartUpload>" +
                "<Part><ETag>" + part1 + "</ETag><PartNumber>1</PartNumber" +
                "></Part><Part><ETag>" + part2 + "</ETag><PartNumber>2" +
                "</PartNumber></Part></CompleteMultipartUpload>")
                .getBytes(UTF_8));

    //WHEN
    CompleteMultipartUploadRequest completeMultipartUploadRequest =
        unmarshall(inputBody);

    //THEN
    checkContent(completeMultipartUploadRequest);
  }

  private void checkContent(CompleteMultipartUploadRequest completeMultipartUploadRequest) {
    Assert.assertEquals(2, completeMultipartUploadRequest.getPartList().size());

    List<CompleteMultipartUploadRequest.Part> parts =
        completeMultipartUploadRequest.getPartList();

    Assert.assertEquals(part1,
        parts.get(0).geteTag());
    Assert.assertEquals(part2,
        completeMultipartUploadRequest.getPartList().get(1).geteTag());
  }

  private CompleteMultipartUploadRequest unmarshall(
      ByteArrayInputStream inputBody) throws IOException {
    return new CompleteMultipartUploadRequestUnmarshaller()
        .readFrom(null, null, null, null, null, inputBody);
  }
}
