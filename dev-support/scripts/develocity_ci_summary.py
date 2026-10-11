#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.  The ASF
# licenses this file to You under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
# License for the specific language governing permissions and limitations
# under the License.

"""Summarize Apache Ozone Main Develocity build scans for CI tuning."""

from __future__ import annotations

import argparse
import configparser
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional

DEVELOCITY_HOST = "https://develocity.apache.org"
DEFAULT_CREDS = os.path.expanduser("~/.asf-jira-pat")
ROOT_PROJECT = "Apache Ozone Main"


def load_access_key(creds_path: str) -> str:
  key = os.environ.get("DEVELOCITY_ACCESS_KEY", "").strip()
  if key:
    return key

  if not os.path.isfile(creds_path):
    raise SystemExit(
        f"No DEVELOCITY_ACCESS_KEY in env and creds file missing: {creds_path}")

  parser = configparser.ConfigParser()
  parser.read(creds_path)
  if parser.has_section("develocity"):
    key = parser.get("develocity", "DEVELOCITY_ACCESS_KEY", fallback="").strip()
    if key:
      return key

  raise SystemExit(
      "Add a Develocity access key to ~/.asf-jira-pat:\n"
      "  [develocity]\n"
      "  DEVELOCITY_ACCESS_KEY=<key from develocity.apache.org My settings>\n"
      "Or export DEVELOCITY_ACCESS_KEY.")


def api_get(path: str, access_key: str, params: Optional[Dict[str, str]] = None) -> Any:
  query = urllib.parse.urlencode(params or {})
  url = f"{DEVELOCITY_HOST}{path}"
  if query:
    url = f"{url}?{query}"
  req = urllib.request.Request(
      url,
      headers={"Authorization": f"Bearer {access_key}", "Accept": "application/json"},
  )
  with urllib.request.urlopen(req, timeout=120) as resp:
    return json.loads(resp.read().decode("utf-8"))


def verify_api_permission(access_key: str) -> None:
  """Raise SystemExit if the key cannot access build data via the API."""
  url = (
      f"{DEVELOCITY_HOST}/api/auth/token"
      "?permissions=accessBuildDataViaApi&expiresInHours=1"
  )
  req = urllib.request.Request(url, method="POST", headers={
      "Authorization": f"Bearer {access_key}",
  })
  try:
    urllib.request.urlopen(req, timeout=30)
  except urllib.error.HTTPError as e:
    body = e.read().decode("utf-8", errors="replace")
    if e.code == 403 and "accessBuildDataViaApi" in body:
      raise SystemExit(
          "This access key can sign in to Develocity but lacks "
          "'Access build data via the API' (API Client / exportData). "
          "Create a new access key with that permission, or ask an admin to "
          "assign the API Client or Developer role, then update "
          "~/.asf-jira-pat [develocity] DEVELOCITY_ACCESS_KEY.") from e
    raise SystemExit(f"Develocity token probe HTTP {e.code}: {body[:500]}") from e


def fetch_build_page(
    access_key: str,
    from_instant: int,
    query: str,
    max_builds: int,
) -> List[Dict[str, Any]]:
  params = {
      "fromInstant": str(from_instant),
      "reverse": "true",
      "maxBuilds": str(max_builds),
      "maxWaitSecs": "30",
      "query": query,
  }
  data = api_get("/api/builds", access_key, params)
  if not isinstance(data, list):
    raise SystemExit(f"Unexpected /api/builds response: {type(data)}")
  return data


def summarize(builds: List[Dict[str, Any]]) -> None:
  total = len(builds)
  failed = sum(1 for b in builds if b.get("buildOutcome") == "failed")
  durations = [b.get("buildDuration") for b in builds if b.get("buildDuration") is not None]
  print(f"Build scans (sample): {total}")
  print(f"Failed: {failed} ({100.0 * failed / total:.1f}%)" if total else "Failed: 0")
  if durations:
    durations.sort()
    mid = durations[len(durations) // 2]
    print(f"Build duration median (ms): {mid}")
    print(f"Build duration max (ms): {max(durations)}")


def main() -> None:
  ap = argparse.ArgumentParser(description=__doc__)
  ap.add_argument(
      "--from-ms",
      type=int,
      default=1782889200000,
      help="Start instant (ms since epoch); default 2026-07-01 00:00 PDT",
  )
  ap.add_argument("--max-builds", type=int, default=200)
  ap.add_argument("--creds", default=DEFAULT_CREDS)
  ap.add_argument(
      "--query",
      default=f'maven.topLevelProjectName:"{ROOT_PROJECT}" tag:CI',
      help="Develocity advanced search query",
  )
  args = ap.parse_args()

  access_key = load_access_key(args.creds)
  verify_api_permission(access_key)
  try:
    builds = fetch_build_page(access_key, args.from_ms, args.query, args.max_builds)
  except urllib.error.HTTPError as e:
    body = e.read().decode("utf-8", errors="replace")[:500]
    if e.code in (403, 404) and "accessBuildDataViaApi" in body:
      raise SystemExit(
          "Develocity access key is missing API permission (exportData / "
          "Access build data via the API). Regenerate the key in My settings "
          "with that permission enabled, then update ~/.asf-jira-pat.") from e
    if e.code == 404:
      raise SystemExit(
          "Develocity /api/builds returned 404. If the key is new, ensure it "
          "has 'Access build data via the API' (exportData). Otherwise the "
          f"query may match no builds: {args.query}\n{body}") from e
    raise SystemExit(f"Develocity API HTTP {e.code}: {body}") from e

  summarize(builds)


if __name__ == "__main__":
  main()
