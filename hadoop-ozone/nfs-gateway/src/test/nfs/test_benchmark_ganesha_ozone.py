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

"""Unit tests for the mounted benchmark's server RSS sampling."""

import unittest
from unittest.mock import mock_open, patch

import benchmark_ganesha_ozone


class ProcessRssSamplerTest(unittest.TestCase):
    def test_reads_resident_memory_from_proc_status(self):
        status = "Name:\tjava\nVmSize:\t1000 kB\nVmRSS:\t1536 kB\n"
        with patch("builtins.open", mock_open(read_data=status)):
            self.assertEqual(1.5, benchmark_ganesha_ozone.process_rss_mib(42))

    def test_samples_peak_rss_for_each_named_process(self):
        with patch("benchmark_ganesha_ozone.process_rss_mib", return_value=64.0):
            sampler = benchmark_ganesha_ozone.ProcessRssSampler(
                [("ganesha", 42), ("om", 43)], interval_seconds=0.001)
            sampler.start()
            sampler.thread.join(timeout=0.02)
            sampler.stop()

        results = sampler.results()
        self.assertEqual(64.0, results["ganesha"]["peak_rss_mib"])
        self.assertEqual(64.0, results["om"]["peak_rss_mib"])
        self.assertGreaterEqual(results["ganesha"]["samples"], 2)
        self.assertGreaterEqual(results["om"]["samples"], 2)

    def test_empty_process_list_returns_empty_results(self):
        self.assertEqual({}, benchmark_ganesha_ozone.ProcessRssSampler([]).results())


if __name__ == "__main__":
    unittest.main()
