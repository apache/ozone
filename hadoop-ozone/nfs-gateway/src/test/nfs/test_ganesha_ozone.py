# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0
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

"""Verify mounted Ganesha writes and renames against canonical OzoneFS data."""

import argparse
import errno
import os
from pathlib import Path
import re
import subprocess
import tempfile
import unittest
import uuid


class GaneshaOzoneContract(unittest.TestCase):
    mount = None
    ozone_uri = None
    isolated_mount = None
    isolated_ozone_uri = None
    ozone_cli = None
    read_keys = ()

    def ozone_path(self, name):
        return self.ozone_uri.rstrip("/") + "/" + name

    def read_ozone_file(self, name):
        return self.read_ozone_file_at(self.ozone_path(name))

    def ozone_file_exists(self, name):
        return self.ozone_file_exists_at(self.ozone_path(name))

    def test_cp_upload_is_published_to_ozonefs(self):
        relative_name = "ganesha-cp-contract-" + uuid.uuid4().hex
        path = self.mount / relative_name
        expected = bytes(range(256)) * 1024

        def cleanup():
            try:
                path.unlink()
            except FileNotFoundError:
                pass

        self.addCleanup(cleanup)

        with tempfile.TemporaryDirectory(prefix="ozone-nfs-cp-") as temporary_directory:
            source = Path(temporary_directory) / "upload"
            source.write_bytes(expected)
            subprocess.run(["cp", "--", str(source), str(path)], check=True, timeout=60)

        self.assertEqual(expected, path.read_bytes())
        self.assertEqual(expected, self.read_ozone_file(relative_name))

    def test_multimegabyte_upload_is_published_byte_exactly(self):
        relative_name = "ganesha-large-upload-contract-" + uuid.uuid4().hex
        path = self.mount / relative_name
        chunk_size = 1024 * 1024
        expected = b"".join(bytes([index + 1]) * chunk_size for index in range(16))

        def cleanup():
            try:
                path.unlink()
            except FileNotFoundError:
                pass

        self.addCleanup(cleanup)

        descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        try:
            for offset in range(0, len(expected), chunk_size):
                view = memoryview(expected)[offset:offset + chunk_size]
                while view:
                    written = os.write(descriptor, view)
                    if written <= 0:
                        self.fail("NFS write made no progress")
                    view = view[written:]
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

        self.assertEqual(expected, path.read_bytes())
        self.assertEqual(expected, self.read_ozone_file(relative_name))

    def test_chmod_is_rejected_without_changing_file_data(self):
        relative_name = "ganesha-chmod-contract-" + uuid.uuid4().hex
        path = self.mount / relative_name
        expected = b"Ozone ACLs remain authoritative"
        path.write_bytes(expected)

        def cleanup():
            try:
                path.unlink()
            except FileNotFoundError:
                pass

        self.addCleanup(cleanup)

        with self.assertRaises(OSError) as failure:
            os.chmod(path, 0o600)

        self.assertIn(failure.exception.errno,
                      (errno.EOPNOTSUPP, errno.ENOTSUP, errno.EPERM))
        self.assertEqual(expected, path.read_bytes())
        self.assertEqual(expected, self.read_ozone_file(relative_name))

    def test_recreated_path_has_new_handle_and_old_handle_is_stale(self):
        with open("/proc/mounts", encoding="utf-8") as mounts:
            mount_options = next(
                fields[3].split(",") for line in mounts
                if (fields := line.split())[1] == str(self.mount))
        if not all(option in mount_options for option in
                   ("acregmin=0", "acregmax=0", "acdirmin=0", "acdirmax=0")):
            self.skipTest("stale-handle contract requires an actimeo=0 NFS mount")

        relative_name = "ganesha-stale-contract-" + uuid.uuid4().hex
        path = self.mount / relative_name
        ozone_path = self.ozone_path(relative_name)
        old_data = b"old object identity"
        replacement = b"replacement object identity"

        old_handle = None

        def cleanup():
            if old_handle is not None:
                os.close(old_handle)
            subprocess.run(
                [self.ozone_cli, "fs", "-rm", "-skipTrash", ozone_path],
                capture_output=True, timeout=60)

        self.addCleanup(cleanup)

        descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
        try:
            self.assertEqual(len(old_data), os.write(descriptor, old_data))
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        old_handle = os.open(path, os.O_RDONLY)
        old_file_id = os.fstat(old_handle).st_ino

        with tempfile.TemporaryDirectory(prefix="ozone-nfs-stale-") as temporary_directory:
            source = Path(temporary_directory) / "replacement"
            source.write_bytes(replacement)
            subprocess.run(
                [self.ozone_cli, "fs", "-rm", "-skipTrash", ozone_path],
                check=True, capture_output=True, timeout=60)
            subprocess.run(
                [self.ozone_cli, "fs", "-put", str(source), ozone_path],
                check=True, capture_output=True, timeout=60)

        self.assertNotEqual(old_file_id, path.stat().st_ino)
        with self.assertRaises(OSError) as failure:
            os.pread(old_handle, len(old_data), 0)
        self.assertEqual(errno.ESTALE, failure.exception.errno)
        self.assertEqual(replacement, path.read_bytes())

    def test_exports_do_not_expose_each_others_keys(self):
        if self.isolated_mount is None or self.isolated_ozone_uri is None:
            self.skipTest("no second Ozone export was configured")

        relative_name = "ganesha-export-isolation-" + uuid.uuid4().hex
        primary_path = self.mount / relative_name
        isolated_path = self.isolated_mount / relative_name
        primary_data = b"primary Ozone export"
        isolated_data = b"isolated Ozone export"
        primary_uri = self.ozone_path(relative_name)
        isolated_uri = self.isolated_ozone_uri.rstrip("/") + "/" + relative_name

        def cleanup():
            for path in (primary_path, isolated_path):
                try:
                    path.unlink()
                except FileNotFoundError:
                    pass

        self.addCleanup(cleanup)

        primary_path.write_bytes(primary_data)
        self.assertFalse(isolated_path.exists())
        self.assertFalse(self.ozone_file_exists_at(isolated_uri))

        isolated_path.write_bytes(isolated_data)
        self.assertEqual(primary_data, primary_path.read_bytes())
        self.assertEqual(isolated_data, isolated_path.read_bytes())
        self.assertNotEqual(primary_path.stat().st_ino, isolated_path.stat().st_ino)
        self.assertEqual(primary_data, self.read_ozone_file_at(primary_uri))
        self.assertEqual(isolated_data, self.read_ozone_file_at(isolated_uri))

    def read_ozone_file_at(self, uri):
        return subprocess.run(
            [self.ozone_cli, "fs", "-cat", uri], check=True, capture_output=True, timeout=60,
        ).stdout

    def ozone_file_exists_at(self, uri):
        result = subprocess.run(
            [self.ozone_cli, "fs", "-test", "-e", uri],
            capture_output=True, timeout=60,
        )
        if result.returncode not in (0, 1):
            self.fail("OzoneFS existence check failed: {}".format(result.stderr.decode(errors="replace")))
        return result.returncode == 0

    def test_stable_write_and_rename_are_published_to_ozonefs(self):
        relative_name = "ganesha-ozone-contract-" + uuid.uuid4().hex
        path = self.mount / relative_name
        renamed_name = relative_name + "-renamed"
        renamed_path = self.mount / renamed_name
        expected = b"0123WXYZ89abcdef--OzoneFS publication contract"

        def cleanup():
            for candidate in (path, renamed_path):
                try:
                    candidate.unlink()
                except FileNotFoundError:
                    pass

        self.addCleanup(cleanup)

        descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
        try:
            self.assertEqual(len(expected) - 8, os.pwrite(descriptor, expected[8:], 8))
            self.assertEqual(8, os.pwrite(descriptor, expected[:8], 0))
            self.assertEqual(4, os.pwrite(descriptor, b"WXYZ", 4))
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

        self.assertEqual(expected, path.read_bytes())
        self.assertEqual(expected, self.read_ozone_file(relative_name))
        self.assertTrue(self.ozone_file_exists(relative_name))

        inode = path.stat().st_ino
        path.rename(renamed_path)
        self.assertEqual(inode, renamed_path.stat().st_ino)
        self.assertFalse(self.ozone_file_exists(relative_name))
        self.assertTrue(self.ozone_file_exists(renamed_name))
        self.assertEqual(expected, self.read_ozone_file(renamed_name))

        renamed_path.unlink()
        self.assertFalse(self.ozone_file_exists(renamed_name))

    def test_configured_ozone_files_read_through_ganesha(self):
        if not self.read_keys:
            self.skipTest("no Ozone keys were configured for mounted readback")

        for key in self.read_keys:
            with self.subTest(key=key):
                relative_path = Path(key)
                if relative_path.is_absolute() or ".." in relative_path.parts:
                    self.fail("mounted read key must be a relative path without '..': {}".format(key))
                mounted_path = self.mount / relative_path
                self.assertEqual(self.read_ozone_file(key), mounted_path.read_bytes())


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
    parser.add_argument("--isolated-mount", type=Path,
                        help="Optional second NFS mount for a distinct Ozone export")
    parser.add_argument("--ozone-uri", required=True,
                        help="OzoneFS bucket root, for example ofs://om-host/volume/bucket")
    parser.add_argument("--isolated-ozone-uri",
                        help="OzoneFS bucket root corresponding to --isolated-mount")
    parser.add_argument("--ozone-cli", default="ozone", help="Path to the Ozone CLI executable")
    parser.add_argument("--read-key", action="append", default=[],
                        help="Ozone key relative to the export root to compare through the NFS mount; repeatable")
    arguments = parser.parse_args()
    GaneshaOzoneContract.mount = require_nfs41_krb5p_mount(arguments.mount)
    if (arguments.isolated_mount is None) != (arguments.isolated_ozone_uri is None):
        parser.error("--isolated-mount and --isolated-ozone-uri must be used together")
    if arguments.isolated_mount is not None:
        GaneshaOzoneContract.isolated_mount = require_nfs41_krb5p_mount(arguments.isolated_mount)
        GaneshaOzoneContract.isolated_ozone_uri = arguments.isolated_ozone_uri
    GaneshaOzoneContract.ozone_uri = arguments.ozone_uri
    GaneshaOzoneContract.ozone_cli = arguments.ozone_cli
    GaneshaOzoneContract.read_keys = arguments.read_key
    unittest.main(argv=[__file__], verbosity=2)
