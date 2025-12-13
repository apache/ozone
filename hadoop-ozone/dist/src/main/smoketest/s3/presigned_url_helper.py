# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import hashlib


def generate_presigned_put_object_url(
    aws_access_key_id=None,
    aws_secret_access_key=None,
    bucket_name=None,
    object_key=None,
    region_name='us-east-1',
    expiration=3600,
    content_type=None,
    endpoint_url=None,
):
  """
  Generate a presigned URL for PUT Object. This function creates the S3 client internally.
  """
  try:
    import boto3

    client_args = {
      'service_name': 's3',
      'region_name': region_name,
    }

    if aws_access_key_id and aws_secret_access_key:
      client_args['aws_access_key_id'] = aws_access_key_id
      client_args['aws_secret_access_key'] = aws_secret_access_key

    if endpoint_url:
      client_args['endpoint_url'] = endpoint_url

    s3_client = boto3.client(**client_args)

    params = {
      'Bucket': bucket_name,
      'Key': object_key,
    }

    if content_type:
      params['ContentType'] = content_type

    presigned_url = s3_client.generate_presigned_url(
      ClientMethod='put_object',
      Params=params,
      ExpiresIn=expiration
    )

    return presigned_url

  except Exception as e:
    raise Exception(f"Failed to generate presigned URL: {str(e)}")


def compute_sha256_file(path):
  """Compute SHA256 hex digest for the entire file content at path."""
  with open(path, 'rb') as f:
    return hashlib.sha256(f.read()).hexdigest()
