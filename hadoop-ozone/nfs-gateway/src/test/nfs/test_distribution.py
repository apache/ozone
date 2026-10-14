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

"""Smoke-test the NFS gateway entry point in a built Ozone distribution."""

import argparse
from pathlib import Path
import subprocess
import sys
import zipfile


GATEWAY_CLASS = "org/apache/hadoop/ozone/nfs/gateway/NfsGatewayDaemon.class"
CLASSPATH_FILE = "share/ozone/classpath/ozone-nfs-gateway.classpath"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("distribution", type=Path,
                        help="unpacked Ozone distribution directory")
    distribution = parser.parse_args().distribution.resolve()

    classpath = distribution / CLASSPATH_FILE
    if not classpath.is_file():
        raise AssertionError(f"NFS gateway classpath is missing: {classpath}")
    gateway_jars = list((distribution / "share/ozone/lib").glob("ozone-nfs-gateway-*.jar"))
    if len(gateway_jars) != 1:
        raise AssertionError(f"Expected one NFS gateway jar, found {len(gateway_jars)}")
    gateway_jar = gateway_jars[0]
    if gateway_jar.name not in classpath.read_text(encoding="utf-8"):
        raise AssertionError(f"NFS gateway jar is missing from {CLASSPATH_FILE}")

    with zipfile.ZipFile(gateway_jar) as archive:
        if GATEWAY_CLASS not in archive.namelist():
            raise AssertionError(f"NFS gateway main class is missing from {gateway_jar}")

    ozone = distribution / "bin/ozone"
    if not ozone.is_file():
        raise AssertionError(f"Ozone launcher is missing: {ozone}")
    result = subprocess.run([str(ozone), "nfs"], cwd=distribution,
                            text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            check=False)
    expected = "NFS gateway requires Ozone security to be enabled"
    if result.returncode == 0 or expected not in result.stdout:
        raise AssertionError(f"ozone nfs did not reach the expected secure-mode guard:\n{result.stdout}")
    print("NFS distribution smoke test passed")


if __name__ == "__main__":
    try:
        main()
    except (AssertionError, OSError, zipfile.BadZipFile) as error:
        print(error, file=sys.stderr)
        sys.exit(1)
