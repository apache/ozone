#!/usr/bin/env python3
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

"""Verify a non-owner Kerberos caller receives NFS ACCESS from Ozone ACLs."""

import argparse
import errno
from pathlib import Path
import unittest
import uuid

from test_ganesha_write import require_nfs41_krb5p_mount


class GaneshaAuthorizationContract(unittest.TestCase):
    mount = None

    def test_non_owner_cannot_create_in_export_root(self):
        path = self.mount / ("acl-denial-" + uuid.uuid4().hex)
        with self.assertRaises(PermissionError) as denial:
            path.mkdir()
        self.assertEqual(errno.EACCES, denial.exception.errno)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mount", type=Path, required=True)
    arguments = parser.parse_args()
    GaneshaAuthorizationContract.mount = require_nfs41_krb5p_mount(arguments.mount)
    unittest.main(argv=[__file__], verbosity=2)
