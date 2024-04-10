---
title: 使用 Boto3 客户端访问 Ozone 对象存储
linktitle: Boto3
summary: 如何使用 Boto3 客户端访问 Ozone 对象存储？
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

这个指南展示了如何从 Boto3 客户端访问 Ozone 对象存储。以下 API 已经过验证：

- Create bucket
- List bucket
- Head bucket
- Delete bucket
- Upload file
- Download file
- Delete objects(keys)
- Head object
- Multipart upload


## 要求

您将需要较高版本的 Python3 来运行 Boto3 客户端，请参考 Boto3 的安装需求:

https://boto3.amazonaws.com/v1/documentation/api/latest/index.html

## 获取对 Ozone 的资源访问
您可以参考 Amazon Boto3 文档，关于创建 `s3` 资源的内容在此处：
https://boto3.amazonaws.com/v1/documentation/api/latest/guide/resources.html

    s3 = boto3.resource('s3',
                        endpoint_url='http://localhost:9878',
                        aws_access_key_id='testuser/scm@EXAMPLE.COM',
                        aws_secret_access_key='c261b6ecabf7d37d5f9ded654b1c724adac9bd9f13e247a235e567e8296d2999'
    )    
    'endpoint_url' is pointing to Ozone s3 endpoint.


## 通过 session 获取对 Ozone 的客户端访问
您可以参考 Amazon Boto3 文档，关于 session 的内容在此处：
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

如果您连接到一个安全的集群，有多种方式配置 Boto3 客户端凭证。在这些情况下，创建 Ozone s3 客户端时传递 `aws_access_key_id` 和 `aws_secret_access_key` 的上述步骤应该被跳过。

请参考 Boto3 文档以获取详细信息，在此处：
https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html


### 创建桶
    response = s3_client.create_bucket(Bucket='bucket1')
    print(response)

这将在一个名为 `s3v` 的卷中创建一个名为 `bucket1` 的桶

### 列出所有桶
    response = s3_client.list_buckets()
    print('Existing buckets:')
    for bucket in response['Buckets']:
        print(f'  {bucket["Name"]}')

这将列出 Ozone `s3v` 卷中的所有桶

### 查看桶信息
    response = s3_client.head_bucket(Bucket='bucket1')
    print(response)

这将在 Ozone 卷 `s3v` 中查看桶 `bucket1` 的信息。

### 删除桶
    response = s3_client.delete_bucket(Bucket='bucket1')
    print(response)

这将从 Ozone 卷 `s3v` 中删除一个桶 `bucket1`。

### 上传文件
    response = s3.Bucket('bucket1').upload_file('./README.md','README.md')
    print(response)

这将从向 Ozone 卷 `s3v` 和桶 `bucket1` 中上传 `README.md` 文件并创建一个 `README.md` 键。

### 下载文件
    response = s3.Bucket('bucket1').download_file('README.md', 'download.md')
    print(response)

这将从从 Ozone 卷 `s3v` 和桶 `bucket1` 中下载 `README.md` 并创建一个 `README.md` 文件到本地。

### 查看对象信息
    response = s3_client.head_object(Bucket='bucket1', Key='README.md')
    print(response)

这将查看一个位于 Ozone 卷 `s3v` 和桶 `bucket1` 中的 `README.md` 文件的信息。

### 删除多个对象
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

这将从 Ozone 卷 `s3v` 和桶 `bucket1` 中删除多个对象 `README3.md` 和 `README4.md`

### 分片上传
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

这将使用来自 Ozone 卷 `s3v` 的 `maven.gz` 和 `maven1.gz` 作为复制源，以创建 Ozone 卷 `s3v` 中的新对象 `key1`。请注意，`ETag` 是必需的且对于使用分片上传 API 非常重要。
