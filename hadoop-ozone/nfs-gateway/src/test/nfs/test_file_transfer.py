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

"""Run the common file-transfer contract against an existing Linux NFSv4.1 mount."""

import argparse
import errno
import os
from pathlib import Path
import re
import tempfile
import unittest


class FileTransferContract(unittest.TestCase):
    mount = None

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="ozone-nfs-contract-", dir=self.mount)
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)

    def test_upload_ranged_reads_and_eof(self):
        data = bytes(range(256)) * 1024
        path = self.root / "upload"
        with path.open("xb") as stream:
            for offset in range(0, len(data), 16384):
                stream.write(data[offset:offset + 16384])
            stream.flush()
            os.fsync(stream.fileno())
        self.assertEqual(len(data), path.stat().st_size)
        with path.open("rb") as stream:
            for offset, length in [(0, 7), (16380, 19), (65533, 4096), (len(data) - 11, 30)]:
                stream.seek(offset)
                self.assertEqual(data[offset:offset + length], stream.read(length))
            stream.seek(len(data))
            self.assertEqual(b"", stream.read(1))
        self.assertEqual(data, path.read_bytes())

    def test_empty_file(self):
        path = self.root / "empty"
        with path.open("xb"):
            pass
        self.assertEqual(0, path.stat().st_size)
        self.assertEqual(b"", path.read_bytes())

    def test_directory_listing(self):
        names = {"file-{:04d}".format(index) for index in range(300)}
        for name in names:
            with (self.root / name).open("xb"):
                pass
        entries = list(self.root.iterdir())
        self.assertEqual(len(names), len(entries))
        self.assertEqual(names, {entry.name for entry in entries})

    def test_file_rename_preserves_identity_and_data(self):
        source = self.root / "source"
        source.write_bytes(b"rename contract")
        identity = source.stat().st_ino
        destination = self.root / "destination"
        source.rename(destination)
        self.assertFalse(source.exists())
        self.assertEqual(identity, destination.stat().st_ino)
        self.assertEqual(b"rename contract", destination.read_bytes())

    def test_parent_rename_preserves_descendant(self):
        source = self.root / "source"
        (source / "child").mkdir(parents=True)
        file = source / "child" / "file"
        file.write_bytes(b"descendant")
        identity = file.stat().st_ino
        destination = self.root / "destination"
        source.rename(destination)
        moved = destination / "child" / "file"
        self.assertFalse(source.exists())
        self.assertEqual(identity, moved.stat().st_ino)
        self.assertEqual(b"descendant", moved.read_bytes())

    def test_remove_and_rmdir(self):
        directory = self.root / "directory"
        directory.mkdir()
        file = directory / "file"
        with file.open("xb"):
            pass
        with self.assertRaises(OSError) as failure:
            directory.rmdir()
        self.assertEqual(errno.ENOTEMPTY, failure.exception.errno)
        file.unlink()
        self.assertFalse(file.exists())
        directory.rmdir()
        self.assertFalse(directory.exists())

    def test_utf8_and_space_names(self):
        path = self.root / "transfer café 文件"
        path.write_bytes(b"named file")
        self.assertEqual({path.name}, {entry.name for entry in self.root.iterdir()})
        self.assertEqual(b"named file", path.read_bytes())


def require_nfs41_mount(path):
    mount = path.resolve(strict=True)
    with open("/proc/mounts", encoding="utf-8") as mounts:
        for line in mounts:
            fields = line.split()
            name = re.sub(r"\\([0-7]{3})", lambda match: chr(int(match.group(1), 8)), fields[1])
            if name == str(mount) and fields[2] in ("nfs", "nfs4") and "vers=4.1" in fields[3].split(","):
                return mount
    raise ValueError("contract requires an existing Linux NFSv4.1 mount: {}".format(mount))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mount", type=Path, required=True)
    arguments = parser.parse_args()
    FileTransferContract.mount = require_nfs41_mount(arguments.mount)
    unittest.main(argv=[__file__], verbosity=2)
