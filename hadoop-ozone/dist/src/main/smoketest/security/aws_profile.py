# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Writes AWS CLI profiles directly, same result as a series of `aws configure set` calls,
without starting the AWS CLI for each setting."""

import configparser
import os


def _update(path, section, values):
    parser = configparser.RawConfigParser()
    parser.read(path)
    if not parser.has_section(section):
        parser.add_section(section)
    for key, value in values.items():
        parser.set(section, key, value)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    # Same as AWS CLI: new files are readable only by the owner
    with os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600), "w") as f:
        parser.write(f)


def write_aws_profile(profile, access_key, secret_key, session_token="", region="us-east-1"):
    aws_dir = os.path.expanduser("~/.aws")
    config_file = os.environ.get("AWS_CONFIG_FILE", os.path.join(aws_dir, "config"))
    credentials_file = os.environ.get("AWS_SHARED_CREDENTIALS_FILE", os.path.join(aws_dir, "credentials"))
    config_section = profile if profile == "default" else "profile " + profile
    _update(config_file, config_section, {"s3": "\nsignature_version = s3v4", "region": region})
    credentials = {"aws_access_key_id": access_key, "aws_secret_access_key": secret_key}
    if session_token:
        credentials["aws_session_token"] = session_token
    _update(credentials_file, profile, credentials)
