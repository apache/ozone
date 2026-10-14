# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Check the Ganesha patches against the pinned V5.7 source tree."""

import argparse
from pathlib import Path
import subprocess
import tarfile
import tempfile
import unittest
from io import BytesIO


class GaneshaPatchContract(unittest.TestCase):
    def test_patches_apply_to_pinned_source(self):
        revision = subprocess.check_output(
            ["git", "-C", str(OPTIONS.ganesha_source), "rev-parse", "HEAD"], text=True
        ).strip()
        self.assertEqual("3113f970511289ae8928078cf392d17c0df81b38", revision)
        ntirpc_source = Path(OPTIONS.ganesha_source) / "src/libntirpc"
        ntirpc_revision = subprocess.check_output(
            ["git", "-C", str(ntirpc_source), "rev-parse", "HEAD"], text=True
        ).strip()
        self.assertEqual("bf7fd0259c33ce95f5f2bf22817a8912f7fe5188", ntirpc_revision)
        patch_dir = Path(__file__).parents[2] / "main/ganesha/patches"
        patches = [
            patch_dir / "0001-expose-rpcsec-gss-client-context.patch",
            patch_dir / "0003-capture-rpcsec-gss-context-per-nfs4-compound.patch",
            patch_dir / "0002-add-ozone-fsal-build-target.patch",
        ]
        archive = subprocess.run(
            ["git", "-C", str(OPTIONS.ganesha_source), "archive", "HEAD"],
            check=True,
            stdout=subprocess.PIPE,
        ).stdout
        with tempfile.TemporaryDirectory() as temporary_directory:
            with tarfile.open(fileobj=BytesIO(archive)) as source:
                source.extractall(temporary_directory)
            ntirpc_file = Path(temporary_directory) / "src/libntirpc/src/svc_auth_gss.c"
            ntirpc_file.parent.mkdir(parents=True, exist_ok=True)
            ntirpc_auth = subprocess.check_output(
                ["git", "-C", str(ntirpc_source), "show", "HEAD:src/svc_auth_gss.c"]
            )
            ntirpc_file.write_bytes(ntirpc_auth)
            for patch in patches:
                subprocess.run(
                    ["git", "-C", temporary_directory, "apply", "--check", str(patch)],
                    check=True,
                )
                subprocess.run(
                    ["git", "-C", temporary_directory, "apply", str(patch)],
                    check=True,
                )
            patched_auth = ntirpc_file.read_text()
            self.assertIn("gss_OID mech, name_type;", patched_auth)
            self.assertIn(
                "gss_display_name(&min_stat, gd->client_name, &gd->cname,\n"
                "\t\t\t\t     &name_type);",
                patched_auth,
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--ganesha-source", required=True)
    OPTIONS, remaining = parser.parse_known_args()
    unittest.main(argv=["test_ganesha_patch.py"] + remaining)
