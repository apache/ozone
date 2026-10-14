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

"""Exercise Ganesha CREATE, WRITE, COMMIT, close, and REMOVE over a mounted export."""

import argparse
import os
from pathlib import Path
import re
import unittest
import uuid


class GaneshaWriteContract(unittest.TestCase):
    mount = None

    def test_create_write_commit_close_and_remove(self):
        path = self.mount / ("write-contract-" + uuid.uuid4().hex)
        renamed = path.with_name(path.name + "-renamed")

        def cleanup():
            for candidate in (path, renamed):
                try:
                    candidate.unlink()
                except FileNotFoundError:
                    pass

        self.addCleanup(cleanup)
        data = bytes(range(256)) * 16
        with path.open("xb") as stream:
            self.assertEqual(len(data), stream.write(data))
            stream.flush()
            self.assertEqual(len(data), path.stat().st_size)
            os.fsync(stream.fileno())
        self.assertEqual(len(data), path.stat().st_size)
        self.assertEqual(data, path.read_bytes())
        identity = path.stat().st_ino
        path.rename(renamed)
        self.assertEqual(identity, renamed.stat().st_ino)
        renamed.unlink()

    def test_create_and_remove_directory(self):
        directory = self.mount / ("new-directory-" + uuid.uuid4().hex)

        def cleanup():
            try:
                directory.rmdir()
            except FileNotFoundError:
                pass

        self.addCleanup(cleanup)
        directory.mkdir()
        directory.rmdir()

    def test_create_and_remove_utf8_directory(self):
        directory = self.mount / ("dir-café-" + uuid.uuid4().hex)
        child = directory / "文件"

        def cleanup():
            for candidate in (child, directory):
                try:
                    candidate.rmdir()
                except FileNotFoundError:
                    pass

        self.addCleanup(cleanup)
        directory.mkdir()
        self.assertTrue(directory.is_dir())
        child.mkdir()
        self.assertEqual({"文件"}, {entry.name for entry in directory.iterdir()})
        child.rmdir()
        directory.rmdir()


def require_nfs41_krb5p_mount(path):
    mount = Path(os.path.abspath(path))
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
    GaneshaWriteContract.mount = require_nfs41_krb5p_mount(arguments.mount)
    unittest.main(argv=[__file__], verbosity=2)
