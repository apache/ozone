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

"""Verify NFSv4 open state across Ganesha restarts or bidirectional instance takeover."""

import argparse
import os
from pathlib import Path
import shlex
import subprocess
import unittest
import uuid

from test_ganesha_write import require_nfs41_krb5p_mount


class GaneshaReclaimContract(unittest.TestCase):
    mount = None
    restart_commands = []
    restart_timeout_seconds = 90

    def test_open_state_survives_server_restart(self):
        path = self.mount / ("reclaim-contract-" + uuid.uuid4().hex)
        initial = b"stable data before Ganesha restart"
        def cleanup():
            try:
                path.unlink()
            except FileNotFoundError:
                pass

        self.addCleanup(cleanup)
        with path.open("x+b", buffering=0) as stream:
            self.assertEqual(len(initial), stream.write(initial))
            stream.flush()
            os.fsync(stream.fileno())

            expected = initial
            for index, command in enumerate(self.restart_commands, start=1):
                subprocess.run(command, check=True, timeout=self.restart_timeout_seconds)

                stream.seek(0)
                self.assertEqual(expected, stream.read())
                appended = f"; write after reclaim {index}".encode()
                stream.seek(0, 2)
                self.assertEqual(len(appended), stream.write(appended))
                stream.flush()
                os.fsync(stream.fileno())
                expected += appended

        self.assertEqual(expected, path.read_bytes())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mount", type=Path, required=True)
    parser.add_argument(
        "--restart-command",
        action="append",
        required=True,
        help="Ganesha restart/takeover command; repeat for each direction, split into arguments without a shell")
    parser.add_argument("--restart-timeout-seconds", type=int, default=90)
    arguments = parser.parse_args()
    GaneshaReclaimContract.mount = require_nfs41_krb5p_mount(arguments.mount)
    GaneshaReclaimContract.restart_commands = [shlex.split(command) for command in arguments.restart_command]
    GaneshaReclaimContract.restart_timeout_seconds = arguments.restart_timeout_seconds
    if any(not command for command in GaneshaReclaimContract.restart_commands):
        parser.error("--restart-command must not be empty")
    unittest.main(argv=[__file__], verbosity=2)
