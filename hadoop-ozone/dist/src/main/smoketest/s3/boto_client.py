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

import os
import sys
import random
import string
import logging
import json
import unittest
import boto3
import tempfile
from botocore.client import Config
from botocore.exceptions import ClientError
import os.path
from os import path

class TestBotoClient(unittest.TestCase):

    s3 = None
    s3_client = None
    setup_done = False
    target_bucket = None
    ozone_endpoint_url = None
    random_bucket_name = 'ozone-test-' + ''.join(
        random.choice(string.ascii_lowercase) for i in range(10))

    def setUp(self):
        if TestBotoClient.setup_done:
            return

        TestBotoClient.ozone_endpoint_url = sys.argv[1]
        TestBotoClient.target_bucket = sys.argv[2]
        TestBotoClient.setup_done = True

        TestBotoClient.s3 = boto3.resource('s3',
            endpoint_url=self.ozone_endpoint_url
        )

        TestBotoClient.s3_client = boto3.session.Session().client(
            service_name='s3',
            endpoint_url=self.ozone_endpoint_url
        )

        try:
            response = self.s3_client.create_bucket(
                Bucket=self.random_bucket_name)
            print(response)

            response = self.s3_client.upload_file("README.md", str(self.target_bucket), "README3.md")
            print(response)

            response = self.s3.Bucket(str(self.target_bucket)).upload_file('README.md','README4.md')
            print(response)

            self.s3.Bucket(str(self.target_bucket)).upload_file('README.md','README10.md')
            print(response)
        except ClientError as e:
            logging.error(e)
            print(e)

        with tempfile.TemporaryDirectory() as temp_dir:
            local_path = temp_dir + '/multiUpload.gz'
            f = open(local_path, "wb")
            f.seek(10485760)
            f.write(b"\0")
            f.close()
            self.s3.Bucket(str(self.target_bucket)).upload_file(local_path, 'multiUpload.1.gz')
            self.s3.Bucket(str(self.target_bucket)).upload_file(local_path, 'multiUpload.2.gz')

    def test_create_bucket(self):
        self.assertTrue(self.s3_client is not None)
        try:
           letters = string.ascii_lowercase
           bucket_name = ''.join(random.choice(letters) for i in range(10))
           response = self.s3_client.create_bucket(Bucket='bucket-' + str(bucket_name))
           print(response)
           self.assertTrue(str(bucket_name) in response.get('Location'))
           self.assertTrue(response.get('ResponseMetadata').get('HTTPStatusCode') == 200)
        except ClientError as e:
            print(e)
            logging.error(e)
            return False
        return True

    def test_list_bucket(self):
        self.assertTrue(self.s3_client is not None)
        try:
           response = self.s3_client.list_buckets()
           self.assertTrue(response.get('ResponseMetadata').get('HTTPStatusCode') == 200)
           print(response)           
        except ClientError as e:
            print(e)
            logging.error(e)
            return False
        return True

    def test_head_bucket(self):
        self.assertTrue(self.s3_client is not None)
        try:
           response = self.s3_client.head_bucket(Bucket=self.target_bucket)
           self.assertTrue(response.get('ResponseMetadata').get('HTTPStatusCode') == 200)
           print(response)
        except ClientError as e:
            print(e)
            logging.error(e)
            return False
        return True

    def test_bucket_delete(self):
        self.assertTrue(self.s3_client is not None)
        try:
           response = self.s3_client.delete_bucket(
               Bucket=self.random_bucket_name)
           self.assertTrue(response.get('ResponseMetadata').get('HTTPStatusCode') == 204)
           print(response)
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def test_upload_file(self):
        self.assertTrue(self.s3 is not None)
        try:
           self.s3.Bucket(str(self.target_bucket)).upload_file('./README.md','README1.md')
           response = self.s3_client.head_object(Bucket=str(self.target_bucket), Key='README1.md')
           self.assertTrue(response.get('ResponseMetadata').get('HTTPStatusCode') == 200)
           print(response)
        except ClientError as e:
            print(e)
            logging.error(e)
            return False
        return True

    def test_download_file(self):
        self.assertTrue(self.s3 is not None)
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                local_path = temp_dir + '/download.md'
                self.s3.Bucket(str(self.target_bucket)).download_file('README10.md', local_path)
                self.assertTrue(path.exists(local_path))
        except ClientError as e:
            print(e)
            logging.error(e)
            return False
        return True

    def test_delete_objects(self):
        self.assertTrue(self.s3_client is not None)
        try:
            response = self.s3_client.delete_objects(
                Bucket=str(self.target_bucket),
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
            self.assertTrue(response.get('ResponseMetadata').get('HTTPStatusCode') == 200)
            print(response)
        except ClientError as e:
            print(e)
            logging.error(e)
            return False
        return True

    def test_head_object(self):
        self.assertTrue(self.s3_client is not None)
        try:
           response = self.s3_client.head_object(Bucket=str(self.target_bucket), Key='README10.md')
           self.assertTrue(response.get('ResponseMetadata').get('HTTPStatusCode') == 200)
           print(response)
        except ClientError as e:
            print(e)
            logging.error(e)
            return False
        return True

    def test_multi_uploads(self):
        self.assertTrue(self.s3_client is not None)
        try:
            lts = string.ascii_lowercase
            key_name = ''.join(random.choice(lts) for i in range(10))
            response = self.s3_client.create_multipart_upload(Bucket=str(self.target_bucket), Key=str(key_name))
            print(response)
            uid=response['UploadId']

            copy1 = self.target_bucket + "/multiUpload.1.gz"
            response = self.s3_client.upload_part_copy(
                Bucket=str(self.target_bucket),
                CopySource=str(copy1),
                Key=str(key_name),
                PartNumber=1,
                UploadId=str(uid)
            )
            etag1=response.get('CopyPartResult').get('ETag')

            copy2 = self.target_bucket + "/multiUpload.2.gz"
            response = self.s3_client.upload_part_copy(
                Bucket=str(self.target_bucket),
                CopySource=str(copy2),
                Key=str(key_name),
                PartNumber=2,
                UploadId=str(uid)
            )
            etag2=response.get('CopyPartResult').get('ETag')
     
            response = self.s3_client.complete_multipart_upload(
                Bucket=str(self.target_bucket),
                Key=str(key_name),
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
            self.assertTrue(response.get('ResponseMetadata').get('HTTPStatusCode') == 200)
            print(response)
        except ClientError as e:
            print(e)
            logging.error(e)
            return False
        return True

if __name__ == '__main__':
    #unittest.main()
    suite = unittest.TestSuite()

    suite.addTest(TestBotoClient('test_create_bucket'))
    suite.addTest(TestBotoClient('test_list_bucket'))
    suite.addTest(TestBotoClient('test_head_bucket'))
    suite.addTest(TestBotoClient('test_bucket_delete'))
    suite.addTest(TestBotoClient('test_upload_file'))
    suite.addTest(TestBotoClient('test_download_file'))
    suite.addTest(TestBotoClient('test_delete_objects'))
    suite.addTest(TestBotoClient('test_head_object'))
    suite.addTest(TestBotoClient('test_multi_uploads'))

    result = unittest.TextTestRunner(verbosity=2).run(suite)
 
    if result.wasSuccessful():
        print("Boto3 Client Test PASSED!")
        exit(0)
    else:
        print("Boto3 Client Test FAILED!")
        exit(1)
