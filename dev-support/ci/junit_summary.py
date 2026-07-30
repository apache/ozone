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
"""Parse Maven Surefire/Failsafe JUnit XML reports and print a Markdown test summary.

Adapted from Apache Kafka's .github/scripts/junit.py (summary format), reworked for Maven Surefire XML.

Intended for GitHub Actions: junit_summary.py [--path DIR] >> "$GITHUB_STEP_SUMMARY"
Optional env: JUNIT_REPORT_URL (link to the archived test report artifact).
Prints nothing and exits 0 when no reports are found. Always exits 0.
"""

import argparse
import dataclasses
import html
import os
import sys
import xml.etree.ElementTree as ET
from glob import glob

PASSED = "✅ PASSED"
FAILED = "❌ FAILED"
FLAKY = "⚠️ FLAKY"
SKIPPED = "🙈 SKIPPED"

FAIL_TAGS = frozenset(("failure", "error"))
FLAKY_TAGS = frozenset(("flakyFailure", "flakyError"))
MESSAGE_LIMIT = 300


@dataclasses.dataclass
class TestCase:
  module: str
  class_name: str
  test_name: str
  time: float
  status: str  # passed | failed | flaky | skipped
  message: str = ""


def module_name(xml_path):
  # Works for both layouts: <module>/target/surefire-reports/TEST-*.xml (normal run)
  # and target/<check>/<module-path>/TEST-*.xml (failed tests moved by _mvn_unit_report.sh).
  parts = os.path.normpath(xml_path).split(os.sep)[:-1]
  while parts and parts[-1] in ("surefire-reports", "failsafe-reports", "target"):
    parts.pop()
  return parts[-1] if parts else "-"


def clean_message(elem):
  message = " ".join((elem.get("message") or (elem.text or "")).split())
  if len(message) > MESSAGE_LIMIT:
    # JUnit 5 appends "==> expected: ... but was: ..." after the custom message; when a test embeds
    # a whole log as the custom message, drop it and keep the tail with the actual failure
    idx = message.find(" ==> expected:")
    if idx != -1:
      message = message[idx + len(" ==> "):]
  if len(message) > MESSAGE_LIMIT:
    message = message[:MESSAGE_LIMIT] + "..."
  return message


def parse_report(xml_path):
  module = module_name(xml_path)
  cases = []
  for testcase in ET.parse(xml_path).getroot().iter("testcase"):
    failure = next((c for c in testcase if c.tag in FAIL_TAGS), None)
    flaky = next((c for c in testcase if c.tag in FLAKY_TAGS), None)
    skipped = next((c for c in testcase if c.tag == "skipped"), None)
    status, message = "passed", ""
    if failure is not None:
      status, message = "failed", clean_message(failure)
    elif flaky is not None:
      status, message = "flaky", clean_message(flaky)
    elif skipped is not None:
      status = "skipped"
    cases.append(TestCase(module, testcase.get("classname") or "", testcase.get("name") or "",
                          float(testcase.get("time") or 0), status, message))
  return cases


def format_time(seconds):
  minutes, secs = divmod(int(seconds), 60)
  hours, minutes = divmod(minutes, 60)
  if hours:
    return "%dh%dm%ds" % (hours, minutes, secs)
  if minutes:
    return "%dm%ds" % (minutes, secs)
  return "%ds" % secs


def cell(text):
  return html.escape(" ".join(str(text).split())).replace("|", "\\|")


def render_table(title, header, rows):
  lines = ["<details><summary><b>%s (%d)</b></summary>" % (title, len(rows)), ""]
  lines.append("|" + "|".join(header) + "|")
  lines.append("|" + "|".join("---" for _ in header) + "|")
  lines.extend("|" + "|".join(cell(value) for value in row) + "|" for row in rows)
  lines.extend(["", "</details>", ""])
  return lines


def render_summary(cases):
  def select(status):
    return [c for c in cases if c.status == status]

  def full_name(case):
    return "%s.%s" % (case.class_name, case.test_name)

  passed, failed, flaky, skipped = select("passed"), select("failed"), select("flaky"), select("skipped")
  lines = ["## Test Summary", ""]
  # sum of per-test times, not wall clock (parallel forks make wall clock much shorter)
  lines.append("%d tests run in %s (total test time):" % (len(cases), format_time(sum(c.time for c in cases))))
  for group, label in ((passed, PASSED), (failed, FAILED), (flaky, FLAKY), (skipped, SKIPPED)):
    if group:
      emoji, word = label.split(" ", 1)
      lines.append("- %s %d %s" % (emoji, len(group), word))
  lines.append("")
  report_url = os.environ.get("JUNIT_REPORT_URL")
  if report_url:
    lines.extend(["[Download test artifacts](%s)" % report_url, ""])
  if failed:
    lines.extend(render_table(FAILED, ["Module", "Test", "Message", "Time"],
                              [[c.module, full_name(c), c.message, format_time(c.time)] for c in failed]))
  if flaky:
    lines.extend(render_table(FLAKY, ["Module", "Test", "Message", "Time"],
                              [[c.module, full_name(c), c.message, format_time(c.time)] for c in flaky]))
  if skipped:
    lines.extend(render_table(SKIPPED, ["Module", "Test"],
                              [[c.module, full_name(c)] for c in skipped]))
  return "\n".join(lines) + "\n"


REPORT_PATTERNS = (
    os.path.join("**", "surefire-reports", "TEST-*.xml"),
    os.path.join("**", "failsafe-reports", "TEST-*.xml"),
    os.path.join("target", "*", "**", "TEST-*.xml"),
)


def find_reports(base):
  found = set()
  for pattern in REPORT_PATTERNS:
    found.update(glob(os.path.join(base, pattern), recursive=True))
  # junit.sh keeps per-iteration copies under target/<check>/iterationN/ when ITERATIONS>1;
  # exclude them like _mvn_unit_report.sh does, so reruns are not counted multiple times.
  return sorted(path for path in found if "/iteration" not in path.replace(os.sep, "/"))


def main(argv=None):
  parser = argparse.ArgumentParser(description="Print a Markdown summary of JUnit XML test reports.")
  parser.add_argument("--path", default=".", help="directory to scan for TEST-*.xml reports")
  try:
    args = parser.parse_args(argv)
  except SystemExit:
    return 0
  # This script only decorates the step summary; it must NEVER fail the check, so exit 0 no matter what.
  try:
    cases = []
    for report in find_reports(args.path):
      try:
        cases.extend(parse_report(report))
      except Exception as e:
        print("Skipping unreadable report %s: %s" % (report, e), file=sys.stderr)
    if cases:
      print(render_summary(cases), end="")
  except Exception as e:
    print("junit_summary failed: %s" % e, file=sys.stderr)
  return 0


if __name__ == "__main__":
  sys.exit(main())
