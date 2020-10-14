import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import xml.etree.ElementTree as ET
from itertools import izip_longest
from random import randrange

# Ozone doesn't care access key and secret key in non-SafeMode
AWS_ACCESS_KEY_ID = '12345678'
AWS_SECRET_ACCESS_KEY = '12345678'

# Ozone S3 gateway endpoints
root = ET.parse('ozone_idex_config.xml').getroot()
endpoints = root.findall('property/entry')
SERVER_LIST = []
for endpoint in endpoints:
    SERVER_LIST.append(endpoint.get('ozone.s3g.endpoint'))
#endpoint = 'http://localhost:9878'

# Ozone S3 bucket name
bucket_name = 'idex_bucket'

def round_robin(cur = [0]):
    length = len(SERVER_LIST)
    ret = SERVER_LIST[cur[0] % length]
    cur[0] = (cur[0] + 1) % length
    return ret

def s3_resource():
    session = boto3.session.Session(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    endpoint = round_robin([randrange(len(SERVER_LIST))])
    s3 = session.resource(service_name='s3', endpoint_url=endpoint)
    print("current endpoint: " + endpoint)
    return s3

class OzoneS3:
    @staticmethod
    def upload_file(file_path):
        try:
            s3_resource().Bucket(bucket_name).upload_file(file_path, file_path)
        except Exception as e:
            print("retry one time. exception: " + str(e))
            s3_resource().Bucket(bucket_name).upload_file(file_path, file_path)


    @staticmethod
    def upload_stream(key, byte_stream):
        try:
            s3_resource().Object(bucket_name, key).put(Body=byte_stream)
        except Exception as e:
            s3_resource().Object(bucket_name, key).put(Body=byte_stream)

    @staticmethod
    def download_file(key, output_file_path):
        try:
            s3_resource().Bucket(bucket_name).download_file(key, output_file_path)
        except Exception as e:
            s3_resource().Bucket(bucket_name).download_file(key, output_file_path)

    @staticmethod
    def download_stream(key):
        try:
            obj = s3_resource().Object(bucket_name, key)
            return obj.get()['Body'].read()
        except Exception as e:
            obj = s3_resource().Object(bucket_name, key)
            return obj.get()['Body'].read()

    @staticmethod
    def iter_n(iterable, n, fillvalue=None):
        args = [iter(iterable)] * n
        return izip_longest(*args, fillvalue=fillvalue)

    @staticmethod
    def download_stream_iter(key):
        try:
            obj = s3_resource().Object(bucket_name, key)
            return obj.get()['Body']
        except Exception as e:
            obj = s3_resource().Object(bucket_name, key)
            return obj.get()['Body']

    @staticmethod
    def delete(key):
        try:
            s3_resource().Object(bucket_name, key).delete()
        except Exception as e:
            s3_resource().Object(bucket_name, key).delete()

