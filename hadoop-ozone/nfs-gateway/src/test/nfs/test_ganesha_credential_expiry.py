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

"""Verify expired Kerberos credentials are rejected by a mounted NFS export."""

import argparse
from datetime import datetime, timedelta
import errno
import os
from pathlib import Path
import subprocess
import time
import unittest
import uuid

from test_ganesha_write import require_nfs41_krb5p_mount


class GaneshaCredentialExpiryContract(unittest.TestCase):
    mount = None
    ticket_cache = None
    expiry_timeout_seconds = 180

    def ticket_is_valid(self):
        return subprocess.run(
            ["klist", "-s", "-c", self.ticket_cache],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        ).returncode == 0

    def nfs_service_ticket_expiry(self):
        result = subprocess.run(
            ["klist", "-c", self.ticket_cache],
            check=True,
            capture_output=True,
            text=True,
        )
        for line in result.stdout.splitlines():
            fields = line.split()
            if len(fields) >= 5 and fields[-1].startswith("nfs/"):
                return datetime.strptime(" ".join(fields[2:4]), "%m/%d/%y %H:%M:%S")
        self.fail("Kerberos cache has no NFS service ticket; mount the export before expiry")

    def test_expired_ticket_cannot_start_new_operation(self):
        allowed = self.mount / ("credential-valid-" + uuid.uuid4().hex)
        denied = self.mount / ("credential-expired-" + uuid.uuid4().hex)
        self.assertTrue(self.ticket_is_valid(), "a valid Kerberos ticket is required before the test")
        service_ticket_expiry = self.nfs_service_ticket_expiry()

        allowed.mkdir()
        allowed.rmdir()

        # The GSS context can outlive its TGT until the cached NFS service ticket expires.
        wait_seconds = (service_ticket_expiry + timedelta(seconds=10) - datetime.now()).total_seconds()
        self.assertLessEqual(wait_seconds, self.expiry_timeout_seconds,
                             "ticket expiry exceeds the configured timeout")
        if wait_seconds > 0:
            time.sleep(wait_seconds)
        self.assertFalse(self.ticket_is_valid(), "the Kerberos ticket did not expire before the timeout")

        try:
            denied.mkdir()
        except OSError as error:
            self.assertIn(error.errno, (errno.EACCES, getattr(errno, "EKEYEXPIRED", -1)))
        else:
            denied.rmdir()
            self.fail("NFS accepted a namespace operation after the Kerberos ticket expired")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mount", type=Path, required=True)
    parser.add_argument("--ticket-cache", default=os.environ.get("KRB5CCNAME", "FILE:/tmp/krb5cc_0"))
    parser.add_argument("--expiry-timeout-seconds", type=int, default=180)
    arguments = parser.parse_args()
    GaneshaCredentialExpiryContract.mount = require_nfs41_krb5p_mount(arguments.mount)
    GaneshaCredentialExpiryContract.ticket_cache = arguments.ticket_cache
    GaneshaCredentialExpiryContract.expiry_timeout_seconds = arguments.expiry_timeout_seconds
    unittest.main(argv=[__file__], verbosity=2)
