Python interface for ozone thru s3 gateway.

In order to use this OzoneS3.py, make sure python3 is installed in your env. Then put s3 gateway endpoint in ozone_client_confg.xml.
If you have multiple s3 gateway endpoint, put separate entries in ozone_client_config.xml. OzoneS3.py will randomly pick one.

Bucket is hardcoded in OzoneS3.py. In terms of s3 gateway of Ozone, please refer to https://hadoop.apache.org/ozone/docs/0.4.0-alpha/s3.html .
