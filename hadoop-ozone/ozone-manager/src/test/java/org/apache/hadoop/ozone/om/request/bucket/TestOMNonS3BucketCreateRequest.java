package org.apache.hadoop.ozone.om.request.bucket;

import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;

import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class TestOMNonS3BucketCreateRequest extends TestOMBucketCreateRequest{
    private String bucketName;
    private boolean strictS3;
    private boolean expectBucketCreated;

    @Parameterized.Parameters
    public static Collection createBucketNamesAndStrictS3() {
        return Arrays.asList(new Object[][] {
            { "bucket_underscore", false, true},
            { "_bucket___multi_underscore_", false, true},
            { "bucket", true, true},
            { "bucket_", true, false},
        });
    }

    public TestOMNonS3BucketCreateRequest(String bucketName, boolean strictS3, boolean expectBucketCreated) {
        this.bucketName = bucketName;
        this.strictS3 = strictS3;
        this.expectBucketCreated = expectBucketCreated;
    }

    @Test
    public void testCreateBucketWithOMNamespaceS3NotStrict()
        throws Exception {
        String volumeName = UUID.randomUUID().toString();
        when(ozoneManager.isStrictS3()).thenReturn(strictS3);
        OMBucketCreateRequest omBucketCreateRequest;
        if (expectBucketCreated) {
            omBucketCreateRequest= doPreExecute(volumeName, bucketName);
            doValidateAndUpdateCache(volumeName, bucketName,
            omBucketCreateRequest.getOmRequest());
        } else {
            Throwable e = assertThrows(OMException.class, () ->
                doPreExecute(volumeName, bucketName));
            assertEquals(e.getMessage(),"Invalid bucket name: bucket_");
        }
    }

}
