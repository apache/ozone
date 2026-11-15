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
"""Tests for coverage_summary.py. Run: cd dev-support/ci && python3 -m unittest test_coverage_summary -v"""
import contextlib
import io
import os
import tempfile
import unittest
from unittest import mock

import coverage_summary

SAMPLE_XML = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<!DOCTYPE report PUBLIC "-//JACOCO//DTD Report 1.1//EN" "report.dtd">
<report name="JaCoCo Coverage Report">
  <sessioninfo id="localhost-6cb2dcd8" start="1794287847378" dump="1794287848330"/>
  <package name="org/apache/hadoop/ozone/recon/persistence">
    <class name="org/apache/hadoop/ozone/recon/persistence/ContainerHistory" sourcefilename="ContainerHistory.java">
      <method name="getLastBcsId" desc="()J" line="55">
        <counter type="INSTRUCTION" missed="0" covered="3"/>
        <counter type="LINE" missed="0" covered="1"/>
        <counter type="COMPLEXITY" missed="0" covered="1"/>
        <counter type="METHOD" missed="0" covered="1"/>
      </method>
      <counter type="INSTRUCTION" missed="0" covered="82"/>
      <counter type="LINE" missed="0" covered="34"/>
      <counter type="COMPLEXITY" missed="0" covered="17"/>
      <counter type="METHOD" missed="0" covered="17"/>
      <counter type="CLASS" missed="0" covered="1"/>
    </class>
    <sourcefile name="ContainerHistory.java">
      <line nr="39" mi="0" ci="2" mb="0" cb="0"/>
      <line nr="40" mi="0" ci="3" mb="0" cb="0"/>
      <line nr="41" mi="0" ci="3" mb="0" cb="0"/>
      <counter type="INSTRUCTION" missed="0" covered="82"/>
      <counter type="LINE" missed="0" covered="34"/>
      <counter type="COMPLEXITY" missed="0" covered="17"/>
      <counter type="METHOD" missed="0" covered="17"/>
      <counter type="CLASS" missed="0" covered="1"/>
    </sourcefile>
    <counter type="INSTRUCTION" missed="285" covered="1036"/>
    <counter type="BRANCH" missed="34" covered="54"/>
    <counter type="LINE" missed="75" covered="286"/>
    <counter type="COMPLEXITY" missed="31" covered="84"/>
    <counter type="METHOD" missed="3" covered="68"/>
    <counter type="CLASS" missed="0" covered="11"/>
  </package>
  <package name="org/apache/ozone/hdfs/web">
    <counter type="INSTRUCTION" missed="0" covered="11"/>
    <counter type="LINE" missed="0" covered="2"/>
    <counter type="COMPLEXITY" missed="0" covered="1"/>
    <counter type="METHOD" missed="0" covered="1"/>
    <counter type="CLASS" missed="0" covered="1"/>
  </package>
  <counter type="INSTRUCTION" missed="115391" covered="603867"/>
  <counter type="BRANCH" missed="14856" covered="40462"/>
  <counter type="LINE" missed="27270" covered="148570"/>
  <counter type="COMPLEXITY" missed="16488" covered="44829"/>
  <counter type="METHOD" missed="4369" covered="28809"/>
  <counter type="CLASS" missed="210" covered="3505"/>
</report>
"""


def write_report(tmp, content=SAMPLE_XML, name="all.xml"):
  path = os.path.join(tmp, name)
  with open(path, "w", encoding="utf-8") as f:
    f.write(content)
  return path


def parse_sample():
  with tempfile.TemporaryDirectory() as tmp:
    return coverage_summary.parse_report(write_report(tmp))


class TestPercent(unittest.TestCase):
  def test_percent(self):
    self.assertEqual(coverage_summary.percent(2, 3), "66.7%")
    self.assertEqual(coverage_summary.percent(0, 0), "n/a")


class TestParseReport(unittest.TestCase):
  def test_overall_counters(self):
    overall, _ = parse_sample()
    lines = overall.counter(coverage_summary.JACOCO_LINE)
    self.assertEqual((lines.missed, lines.covered, lines.total), (27270, 148570, 175840))
    self.assertEqual(overall.counter(coverage_summary.JACOCO_CLASS).missed, 210)
    self.assertEqual(overall.counter("NO_SUCH_TYPE").total, 0)

  def test_packages_counters(self):
    _, packages = parse_sample()
    by_name = {p.name: p for p in packages}
    self.assertEqual(sorted(by_name),
                     ["org.apache.hadoop.ozone.recon.persistence", "org.apache.ozone.hdfs.web"])
    package_persistence = by_name["org.apache.hadoop.ozone.recon.persistence"]
    self.assertEqual(package_persistence.counter(coverage_summary.JACOCO_LINE).missed, 75)
    self.assertEqual(package_persistence.missed, 75 + 34)
    package_web = by_name["org.apache.ozone.hdfs.web"]
    self.assertEqual(package_web.counter(coverage_summary.JACOCO_BRANCH).total, 0)
    self.assertEqual(package_web.missed, 0)

  def test_combined_uses_sonar_formula(self):
    _, packages = parse_sample()
    by_name = {p.name: p for p in packages}
    # (286 + 54) / (361 + 88)
    self.assertEqual(by_name["org.apache.hadoop.ozone.recon.persistence"].combined, "75.7%")
    # no branches: falls back to line coverage, 2 / 2
    self.assertEqual(by_name["org.apache.ozone.hdfs.web"].combined, "100.0%")


class TestRenderSummary(unittest.TestCase):
  def render(self):
    return coverage_summary.render_summary(*parse_sample())

  def table_rows(self, text):
    return [line for line in text.splitlines() if line.startswith("|") and not line.startswith("|---")]

  def test_overall_section(self):
    out = self.render()
    self.assertTrue(out.startswith("## Coverage Summary\n"))
    # (covered_lines + covered_branches)/(total_lines + total_branches)
    self.assertIn("**Overall coverage: 81.8%**", out)
    rows = self.table_rows(out)
    self.assertEqual(rows[:3], ["|Counter|Coverage|Missed|Total|",
                                "|Lines|84.5%|27270|175840|",
                                "|Branches|73.1%|14856|55318|"])
    self.assertIn("\nUntested: 4369 methods, 210 classes\n", out)

  def test_report_url(self):
    with mock.patch.dict(os.environ, {"COVERAGE_REPORT_URL": "https://example.com/artifact"}):
      self.assertIn("📦 Full HTML report: [download artifact](https://example.com/artifact)", self.render())
    with mock.patch.dict(os.environ, {}, clear=True):
      self.assertNotIn("📦 Full HTML report: [download artifact]", self.render())

  def test_package_table_sorted_by_missed(self):
    out = self.render()
    self.assertIn("<details><summary><b>Coverage by package (2)</b></summary>\n\n|Package|", out)
    rows = self.table_rows(out)[3:]
    # most untested code first: persistence misses 75 lines + 34 branches, web misses 0 lines
    self.assertEqual(rows, ["|Package|Coverage|Lines|Missed lines|Branches|Missed branches|",
                            "|org.apache.hadoop.ozone.recon.persistence|75.7%|79.2%|75|61.4%|34|",
                            "|org.apache.ozone.hdfs.web|100.0%|100.0%|0|n/a|0|"])
    self.assertTrue(out.endswith("|\n\n</details>\n\n"))

  def test_no_packages(self):
    overall, _ = parse_sample()
    out = coverage_summary.render_summary(overall, [])
    self.assertNotIn("<details>", out)
    self.assertTrue(out.endswith("\nUntested: 4369 methods, 210 classes\n\n"))


class TestMain(unittest.TestCase):
  def run_main(self, argv):
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
      code = coverage_summary.main(argv)
    return code, out.getvalue()

  def test_prints_summary(self):
    with tempfile.TemporaryDirectory() as tmp:
      code, out = self.run_main(["--report", write_report(tmp)])
    self.assertEqual(code, 0)
    self.assertIn("|Lines|84.5%|27270|175840|", out)


if __name__ == "__main__":
  unittest.main()
