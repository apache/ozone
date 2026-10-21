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

import contextlib
import io
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

  def test_message_keeps_junit5_tail_when_custom_message_floods(self):
    # assertEquals(expected, actual, message) renders as "<custom message> ==> expected: ... but was: ...";
    # some tests embed whole logs as the custom message, which would push the tail past the truncation limit
    flood = "log line&#10;" * 100
    xml = ('<testsuite><testcase name="t" classname="C" time="1">'
           '<failure message="client log:&#10;%s ==> expected: &lt;1&gt; but was: &lt;2&gt;"/>'
           '</testcase></testsuite>' % flood)
    with tempfile.TemporaryDirectory() as tmp:
      path = self.write_report(tmp, "m/target/surefire-reports/TEST-x.xml", content=xml)
      cases = junit_summary.parse_report(path)
    self.assertEqual(cases[0].message, "expected: <1> but was: <2>")

  def test_message_junit5_tail_edge_cases(self):
    fromstring = junit_summary.ET.fromstring
    # short custom messages are meaningful and kept in full
    elem = fromstring('<failure message="wrong count ==&gt; expected: &lt;1&gt; but was: &lt;2&gt;"/>')
    self.assertEqual(junit_summary.clean_message(elem), "wrong count ==> expected: <1> but was: <2>")
    # assertion values containing " ==> " must not corrupt the tail
    long_context = "x" * 400
    elem = fromstring('<failure message="%s ==&gt; expected: &lt;a ==&gt; b&gt; but was: &lt;c&gt;"/>' % long_context)
    self.assertEqual(junit_summary.clean_message(elem), "expected: <a ==> b> but was: <c>")
    # long messages with " ==> " but no assertion framing fall back to head truncation
    elem = fromstring('<failure message="pipeline A ==&gt; B failed %s"/>' % long_context)
    self.assertTrue(junit_summary.clean_message(elem).startswith("pipeline A ==> B failed"))

  def test_module_name_from_path(self):
    # normal surefire layout: module is the dir above target/
    self.assertEqual(junit_summary.module_name("hadoop-hdds/common/target/surefire-reports/TEST-a.xml"), "common")
    # layout after _mvn_unit_report.sh moves a failed test's XML under target/<check>/<module-path>/
    self.assertEqual(junit_summary.module_name("target/unit/hadoop-hdds/common/TEST-a.xml"), "common")
    self.assertEqual(junit_summary.module_name("TEST-a.xml"), "-")


class TestRender(unittest.TestCase):

  def make_cases(self):
    tc = junit_summary.TestCase
    return [
        tc("common", "org.X", "ok", 1.0, "passed"),
        tc("common", "org.X", "bad", 2.0, "failed", "boom"),
        tc("common", "org.X", "shaky", 3.0, "flaky", "flap"),
        tc("common", "org.X", "skip", 0.0, "skipped"),
    ]

  def test_format_time(self):
    self.assertEqual(junit_summary.format_time(59), "59s")
    self.assertEqual(junit_summary.format_time(779), "12m59s")
    self.assertEqual(junit_summary.format_time(3725), "1h2m5s")

  def test_render_counts_list(self):
    md = junit_summary.render_summary(self.make_cases())
    self.assertIn("## Test Summary", md)
    self.assertIn("4 tests run in 6s (total test time):", md)
    self.assertIn("- ✅ 1 PASSED\n- ❌ 1 FAILED\n- ⚠️ 1 FLAKY\n- 🙈 1 SKIPPED", md)

  def test_render_counts_omit_zero(self):
    cases = [c for c in self.make_cases() if c.status == "passed"]
    md = junit_summary.render_summary(cases)
    self.assertIn("- ✅ 1 PASSED", md)
    self.assertNotIn("FAILED", md)
    self.assertNotIn("FLAKY", md)
    self.assertNotIn("SKIPPED", md)

  def test_render_tables(self):
    md = junit_summary.render_summary(self.make_cases())
    self.assertIn("❌ FAILED (1)", md)
    self.assertIn("|common|org.X.bad|boom|2s|", md)
    self.assertIn("⚠️ FLAKY (1)", md)
    self.assertIn("|common|org.X.shaky|flap|3s|", md)
    self.assertIn("🙈 SKIPPED (1)", md)

  def test_render_escapes_every_cell(self):
    # parameterized test names can contain pipes and HTML-significant chars
    cases = [junit_summary.TestCase("common", "org.X", "test[a|b]<c>", 1.0, "failed", "got |x| <y>")]
    md = junit_summary.render_summary(cases)
    self.assertIn("|common|org.X.test[a\\|b]&lt;c&gt;|got \\|x\\| &lt;y&gt;|1s|", md)

  def test_render_report_url(self):
    os.environ["JUNIT_REPORT_URL"] = "https://example.com/artifact"
    try:
      md = junit_summary.render_summary(self.make_cases())
    finally:
      del os.environ["JUNIT_REPORT_URL"]
    self.assertIn("[Download test artifacts](https://example.com/artifact)", md)


class TestMain(unittest.TestCase):

  write_report = TestParseReport.write_report

  def run_main(self, argv):
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
      code = junit_summary.main(argv)
    return code, out.getvalue()

  def test_find_reports_dedupes_both_trees(self):
    with tempfile.TemporaryDirectory() as tmp:
      self.write_report(tmp, "hadoop-hdds/common/target/surefire-reports/TEST-a.xml")
      self.write_report(tmp, "target/unit/surefire-reports/TEST-b.xml")
      reports = junit_summary.find_reports(tmp)
    self.assertEqual(len(reports), 2)

  def test_find_reports_skips_iteration_dirs(self):
    with tempfile.TemporaryDirectory() as tmp:
      self.write_report(tmp, "target/unit/TEST-a.xml")
      self.write_report(tmp, "target/unit/iteration2/TEST-a.xml")
      reports = junit_summary.find_reports(tmp)
    self.assertEqual(len(reports), 1)

  def test_main_no_reports_is_silent_noop(self):
    with tempfile.TemporaryDirectory() as tmp:
      code, out = self.run_main(["--path", tmp])
    self.assertEqual(code, 0)
    self.assertEqual(out, "")

  def test_main_prints_summary(self):
    with tempfile.TemporaryDirectory() as tmp:
      self.write_report(tmp, "m/target/surefire-reports/TEST-a.xml")
      code, out = self.run_main(["--path", tmp])
    self.assertEqual(code, 0)
    self.assertIn("## Test Summary", out)
    self.assertIn("4 tests run", out)

  def test_main_survives_malformed_xml(self):
    with tempfile.TemporaryDirectory() as tmp:
      self.write_report(tmp, "m/target/surefire-reports/TEST-a.xml")
      self.write_report(tmp, "m/target/surefire-reports/TEST-bad.xml", content="<not-xml")
      with contextlib.redirect_stderr(io.StringIO()):
        code, out = self.run_main(["--path", tmp])
    self.assertEqual(code, 0)
    self.assertIn("4 tests run", out)

  def test_main_survives_non_parse_errors(self):
    # a ValueError (bad time attribute), not an ET.ParseError, must not break the exit-0 guarantee
    bad_time = '<testsuite><testcase name="t" classname="C" time="not-a-number"/></testsuite>'
    with tempfile.TemporaryDirectory() as tmp:
      self.write_report(tmp, "m/target/surefire-reports/TEST-a.xml")
      self.write_report(tmp, "m/target/surefire-reports/TEST-badtime.xml", content=bad_time)
      with contextlib.redirect_stderr(io.StringIO()):
        code, out = self.run_main(["--path", tmp])
    self.assertEqual(code, 0)
    self.assertIn("4 tests run", out)

  def test_main_bad_args_still_exit_zero(self):
    with contextlib.redirect_stderr(io.StringIO()):
      code = junit_summary.main(["--no-such-flag"])
    self.assertEqual(code, 0)


if __name__ == "__main__":
  unittest.main()
