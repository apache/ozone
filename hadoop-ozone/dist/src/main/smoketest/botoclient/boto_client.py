import os
import logging
import json
import unittest
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

class TestBotoClient(unittest.TestCase):

    s3 = None
    s3_client = None
    setup_done = False
 
    def setUp(self):
        if TestBotoClient.setup_done:
            return

        TestBotoClient.setup_done = True

        TestBotoClient.s3 = boto3.resource('s3',
            endpoint_url='http://s3g:9878')

        TestBotoClient.s3_client = boto3.session.Session().client(
            service_name='s3',
            endpoint_url='http://s3g:9878')

        self.s3_client.create_bucket(Bucket='bucket1')
        self.s3_client.create_bucket(Bucket='bucket10')
 
        try:
            response = self.s3_client.upload_file('README.md', 'bucket1', 'README3.md')
            print(response)

            response = self.s3.Bucket('bucket1').upload_file('README.md','README4.md')
            print(response)

            self.s3.Bucket('bucket1').upload_file('README.md','README10.md')
            print(response)
        except ClientError as e:
            logging.error(e)
            print(e)

        f = open('multiUpload.gz',"wb")
        f.seek(10485760)
        f.write(b"\0")
        f.close()
        self.s3.Bucket('bucket1').upload_file('./multiUpload.gz','multiUpload.1.gz')
        self.s3.Bucket('bucket1').upload_file('./multiUpload.gz','multiUpload.2.gz')

    def test_create_bucket(self):
        try:
           self.assertTrue(self.s3_client is not None)
           response = self.s3_client.create_bucket(Bucket='bucket3')
           print(response)
        except ClientError as e:
            print(e)
            logging.error(e)
            return False
        return True

    def test_list_bucket(self):
        try:
           self.assertTrue(self.s3_client is not None)
           response = self.s3_client.list_buckets()
           print(response)
        except ClientError as e:
            print(e)
            logging.error(e)
            return False
        return True 

    def test_head_bucket(self):
        self.assertTrue(self.s3_client is not None)
        try:
           response = self.s3_client.head_bucket(Bucket='bucket1')
           print(response)
        except ClientError as e:
            print(e)
            logging.error(e)
            return False
        return True

    def test_bucket_delete(self):
        self.assertTrue(self.s3_client is not None)
        try:
           response = self.s3_client.delete_bucket(Bucket='bucket10')
           print(response)
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def test_upload_file(self):
        self.assertTrue(self.s3 is not None)
        try:
           response = self.s3.Bucket('bucket1').upload_file('./README.md','README1.md')
           print(response)
        except ClientError as e:
            print(e)
            logging.error(e)
            return False
        return True

    def test_download_file(self):
        self.assertTrue(self.s3 is not None)
        try:
           response = self.s3.Bucket('bucket1').download_file('README10.md', 'download.md')
           print(response)
        except ClientError as e:
            print(e)
            logging.error(e)
            return False
        return True

    def test_delete_objects(self):
        self.assertTrue(self.s3_client is not None)
        try:
            response = self.s3_client.delete_objects(
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
            print(response)
        except ClientError as e:
            print(e)
            logging.error(e)
            return False
        return True

    def test_head_object(self):
        self.assertTrue(self.s3_client is not None)
        try:
           response = self.s3_client.head_object(Bucket='bucket1', Key='README10.md')
           print(response)
        except ClientError as e:
            print(e)
            logging.error(e)
            return False
        return True

    def test_multi_uploads(self):
        self.assertTrue(self.s3_client is not None)
        try:
            response = self.s3_client.create_multipart_upload(Bucket='bucket1', Key='key1')
            print(response)
            uid=response['UploadId']

            response = self.s3_client.upload_part_copy(
                Bucket='bucket1',
                CopySource='/bucket1/multiUpload.1.gz',
                Key='key1',
                PartNumber=1,
                UploadId=str(uid)
            )
            etag1=response.get('CopyPartResult').get('ETag')

            response = self.s3_client.upload_part_copy(
                Bucket='bucket1',
                CopySource='/bucket1/multiUpload.2.gz',
                Key='key1',
                PartNumber=2,
                UploadId=str(uid)
            )
            etag2=response.get('CopyPartResult').get('ETag')
     
            response = self.s3_client.complete_multipart_upload(
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
