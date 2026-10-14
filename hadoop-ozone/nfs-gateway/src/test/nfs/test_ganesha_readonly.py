# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Run the read-only Kerberos contract against a mounted FSAL_OZONE export."""

import argparse
import errno
import os
from pathlib import Path
import re
import unittest


class GaneshaReadOnlyContract(unittest.TestCase):
    mount = None
    expected = b"native READ interop"

    def test_directory_listing(self):
        self.assertIn("child", {entry.name for entry in self.mount.iterdir()})

    def test_ranged_read_and_eof(self):
        path = self.mount / "child"
        with path.open("rb") as stream:
            self.assertEqual(self.expected, stream.read())
        fd = os.open(path, os.O_RDONLY)
        try:
            self.assertEqual(b"READ i", os.pread(fd, 6, 7))
            self.assertEqual(b"", os.pread(fd, 1, len(self.expected)))
        finally:
            os.close(fd)

    def test_export_rejects_writes(self):
        with self.assertRaises(OSError) as failure:
            os.open(self.mount / "child", os.O_WRONLY)
        self.assertIn(failure.exception.errno, (errno.EACCES, errno.EROFS))


def require_nfs41_krb5p_mount(path):
    mount = path.resolve(strict=True)
    with open("/proc/mounts", encoding="utf-8") as mounts:
        for line in mounts:
            fields = line.split()
            name = re.sub(r"\\([0-7]{3})", lambda match: chr(int(match.group(1), 8)), fields[1])
            options = fields[3].split(",")
            if name == str(mount) and fields[2] in ("nfs", "nfs4") and "vers=4.1" in options:
                if "sec=krb5p" in options:
                    return mount
                raise ValueError("contract requires an NFSv4.1 sec=krb5p mount: {}".format(mount))
    raise ValueError("contract requires an existing Linux NFSv4.1 mount: {}".format(mount))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mount", type=Path, required=True)
    arguments = parser.parse_args()
    GaneshaReadOnlyContract.mount = require_nfs41_krb5p_mount(arguments.mount)
    unittest.main(argv=[__file__], verbosity=2)
