---
title: Access Ozone object store with Amazon Boto3 client
linktitle: Ozone with Boto3 Client
summary: How to access Ozone object store with Boto3 client?
---
<!---
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

This recipe shows how Ozone object store can be accessed from Boto3 client. Following apis were verified:

 - Create bucket
 - List bucket
 - Head bucket
 - Delete bucket
 - Upload file
 - Download file
 - Delete objects(keys)
 - Head object
 - Multipart upload


## Requirements

You will need a higher version of Python3 for your Boto3 client as Boto3 installation requirement indicates at here:
https://boto3.amazonaws.com/v1/documentation/api/latest/index.html

## Obtain resource to Ozone
You may reference Amazon Boto3 documentation regarding the creation of 's3' resources at here:
https://boto3.amazonaws.com/v1/documentation/api/latest/guide/resources.html

    s3 = boto3.resource('s3',
                        endpoint_url='http://localhost:9878',
                        aws_access_key_id='testuser/scm@EXAMPLE.COM',
                        aws_secret_access_key='c261b6ecabf7d37d5f9ded654b1c724adac9bd9f13e247a235e567e8296d2999'
    )    
    'endpoint_url' is pointing to Ozone s3 endpoint.


## Obtain client to Ozone via session
You may reference Amazon Boto3 documentation regarding session at here:
https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html

    Create a session
        session = boto3.session.Session()

    Obtain s3 client to Ozone via session:

        s3_client = session.client(
            service_name='s3',
            aws_access_key_id='testuser/scm@EXAMPLE.COM',
            aws_secret_access_key='c261b6ecabf7d37d5f9ded654b1c724adac9bd9f13e247a235e567e8296d2999',
            endpoint_url='http://localhost:9878',
        )
        'endpoint_url' is pointing to Ozone s3 endpoint.

    In our code sample below, we're demonstrating the usage of both s3 and s3_client.

There are multiple ways to configure Boto3 client credentials if you're connecting to a secured cluster. In these cases, 
the above lines of passing 'aws_access_key_id' and 'aws_secret_access_key' when creating Ozone s3 client shall be skipped.

Please refer to Boto3 documentation for details at here:
https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html


### Create a bucket
    response = s3_client.create_bucket(Bucket='bucket1')
    print(response)

This will create a bucket 'bucket1' in Ozone volume 's3v'.

### List buckets
    response = s3_client.list_buckets()
    print('Existing buckets:')
    for bucket in response['Buckets']:
        print(f'  {bucket["Name"]}')

This will list all buckets in Ozone volume 's3v'.

### Head a bucket
    response = s3_client.head_bucket(Bucket='bucket1')
    print(response)

This will head bucket 'bucket1' in Ozone volume 's3v'.

### Delete a bucket
    response = s3_client.delete_bucket(Bucket='bucket1')
    print(response)

This will delete the bucket 'bucket1' from Ozone volume 's3v'.

### Upload a file
    response = s3.Bucket('bucket1').upload_file('./README.md','README.md')
    print(response)

This will upload 'README.md' to Ozone creates a key 'README.md' in volume 's3v'.

### Download a file
    response = s3.Bucket('bucket1').download_file('README.md', 'download.md')
    print(response)

This will download 'README.md' from Ozone volume 's3v' to local and create a file with name 'download.md'.

### Head an object
    response = s3_client.head_object(Bucket='bucket1', Key='README.md')
    print(response)

This will head object 'README.md' from Ozone volume 's3v' in the bucket 'bucket1'.

### Delete Objects
    response = s3_client.delete_objects(
        Bucket='bucket1',
        Delete={
            'Objects': [
                {
                    'Key': 'README4.md',
                },
                {
                    'Key': 'README3.md',
                },
            ],
            'Quiet': False,
        },
    )

This will delete objects 'README3.md' and 'README4.md' from Ozone volume 's3v' in bucket 'bucket1'.

### Multipart upload
    response = s3_client.create_multipart_upload(Bucket='bucket1', Key='key1')
    print(response)
    uid=response['UploadId']
    print(uid)

    response = s3_client.upload_part_copy(
        Bucket='bucket1',
        CopySource='/bucket1/maven.gz',
        Key='key1',
        PartNumber=1,
        UploadId=str(uid)
    )
    print(response)
    etag1=response.get('CopyPartResult').get('ETag')
    print(etag1)

    response = s3_client.upload_part_copy(
        Bucket='bucket1',
        CopySource='/bucket1/maven1.gz',
        Key='key1',
        PartNumber=2,
        UploadId=str(uid)
    )
    print(response)
    etag2=response.get('CopyPartResult').get('ETag')
    print(etag2)

    response = s3_client.complete_multipart_upload(
        Bucket='bucket1',
        Key='key1',
        MultipartUpload={
            'Parts': [
                {
                    'ETag': str(etag1),
                    'PartNumber': 1,
                },
                {
                    'ETag': str(etag2),
                    'PartNumber': 2,
                },
            ],
        },
        UploadId=str(uid),
    )
    print(response)

This will use 'maven.gz' and 'maven1.gz' as copy source from Ozone volume 's3v' to create a new object 'key1'
in Ozone volume 's3v'. Please note 'ETag's is required and important for the call.
