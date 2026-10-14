# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Unit tests for the mounted Ganesha reclaim runner."""

from pathlib import Path
import tempfile
import unittest
from unittest.mock import call, patch

import test_ganesha_reclaim


class GaneshaReclaimRunnerTest(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.contract = test_ganesha_reclaim.GaneshaReclaimContract(
            "test_open_state_survives_server_restart")
        self.addCleanup(self.contract.doCleanups)
        self.contract.mount = Path(self.temporary_directory.name)
        self.contract.restart_commands = [["promote-standby"], ["failback-primary"]]
        self.contract.restart_timeout_seconds = 30

    def test_open_file_is_read_and_written_after_each_takeover(self):
        with patch("test_ganesha_reclaim.subprocess.run") as run:
            self.contract.test_open_state_survives_server_restart()

        self.assertEqual([
            call(["promote-standby"], check=True, timeout=30),
            call(["failback-primary"], check=True, timeout=30),
        ], run.call_args_list)

if __name__ == "__main__":
    unittest.main(verbosity=2)
