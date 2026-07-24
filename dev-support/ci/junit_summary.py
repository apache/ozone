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

Intended for GitHub Actions: junit_summary.py [--path DIR] [--quarantine] >> "$GITHUB_STEP_SUMMARY"
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

PASSED = "PASSED ✅"
FAILED = "FAILED ❌"
FLAKY = "FLAKY ⚠️"
SKIPPED = "SKIPPED 🙈"
QUARANTINED = "QUARANTINED 😷"

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
  message = elem.get("message") or (elem.text or "")
  message = " ".join(message.split())
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
