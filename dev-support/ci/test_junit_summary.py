#!/usr/bin/env python3
#
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
"""Tests for junit_summary.py. Run: cd dev-support/ci && python3 -m unittest test_junit_summary -v"""

import os
import tempfile
import unittest

import junit_summary

SAMPLE_XML = """<?xml version="1.0" encoding="UTF-8"?>
<testsuite name="org.apache.hadoop.ozone.TestExample" tests="4" failures="1" errors="0" skipped="1" time="12.5">
  <testcase name="testPasses" classname="org.apache.hadoop.ozone.TestExample" time="1.5"/>
  <testcase name="testFails" classname="org.apache.hadoop.ozone.TestExample" time="2.0">
    <failure message="expected: &lt;1&gt; but was: |2|" type="AssertionError">stack trace here</failure>
  </testcase>
  <testcase name="testFlaky" classname="org.apache.hadoop.ozone.TestExample" time="3.0">
    <flakyFailure message="Connection refused" type="IOException">stack</flakyFailure>
  </testcase>
  <testcase name="testSkipped" classname="org.apache.hadoop.ozone.TestExample" time="0.0">
    <skipped message="disabled"/>
  </testcase>
</testsuite>
"""


class TestParseReport(unittest.TestCase):

  def write_report(self, tmp, relpath, content=SAMPLE_XML):
    path = os.path.join(tmp, relpath)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
      f.write(content)
    return path

  def test_classification(self):
    with tempfile.TemporaryDirectory() as tmp:
      path = self.write_report(tmp, "hadoop-hdds/common/target/surefire-reports/TEST-x.xml")
      cases = junit_summary.parse_report(path)
    by_name = {c.test_name: c for c in cases}
    self.assertEqual(len(cases), 4)
    self.assertEqual(by_name["testPasses"].status, "passed")
    self.assertEqual(by_name["testFails"].status, "failed")
    self.assertEqual(by_name["testFlaky"].status, "flaky")
    self.assertEqual(by_name["testSkipped"].status, "skipped")
    self.assertEqual(by_name["testFails"].class_name, "org.apache.hadoop.ozone.TestExample")
    self.assertAlmostEqual(by_name["testFlaky"].time, 3.0)

  def test_message_is_collapsed_raw_text(self):
    with tempfile.TemporaryDirectory() as tmp:
      path = self.write_report(tmp, "m/target/surefire-reports/TEST-x.xml")
      cases = junit_summary.parse_report(path)
    failed = next(c for c in cases if c.status == "failed")
    self.assertEqual(failed.message, "expected: <1> but was: |2|")
    flaky = next(c for c in cases if c.status == "flaky")
    self.assertEqual(flaky.message, "Connection refused")

  def test_module_name_from_path(self):
    # normal surefire layout: module is the dir above target/
    self.assertEqual(junit_summary.module_name("hadoop-hdds/common/target/surefire-reports/TEST-a.xml"), "common")
    # layout after _mvn_unit_report.sh moves a failed test's XML under target/<check>/<module-path>/
    self.assertEqual(junit_summary.module_name("target/unit/hadoop-hdds/common/TEST-a.xml"), "common")
    self.assertEqual(junit_summary.module_name("TEST-a.xml"), "-")


if __name__ == "__main__":
  unittest.main()
