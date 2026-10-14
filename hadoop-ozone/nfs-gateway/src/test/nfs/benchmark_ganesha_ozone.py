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

"""Measure sequential transfer and small-file latency on a mounted Ozone export."""

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import re
import resource
import statistics
import sys
import threading
import time
import uuid

from test_ganesha_write import require_nfs41_krb5p_mount


def write_all(descriptor, data):
    offset = 0
    while offset < len(data):
        written = os.write(descriptor, data[offset:])
        if written <= 0:
            raise OSError("NFS write made no progress")
        offset += written


def digest_repeated(data, total_bytes):
    digest = hashlib.sha256()
    remaining = total_bytes
    while remaining:
        chunk = data[:min(remaining, len(data))]
        digest.update(chunk)
        remaining -= len(chunk)
    return digest.digest()


def percentiles(samples):
    ordered = sorted(samples)
    return {
        "p50_ms": round(statistics.median(ordered) * 1000, 3),
        "p95_ms": round(ordered[math.ceil(0.95 * len(ordered)) - 1] * 1000, 3),
    }


def mount_options(mount):
    with open("/proc/mounts", encoding="utf-8") as mounts:
        for line in mounts:
            fields = line.split()
            path = re.sub(r"\\([0-7]{3})", lambda match: chr(int(match.group(1), 8)), fields[1])
            if path == str(mount):
                return {"source": fields[0], "options": fields[3].split(",")}
    raise ValueError("NFS mount disappeared before the benchmark started")


def sequential_transfer(directory, total_bytes, chunk_bytes):
    path = directory / "sequential.bin"
    payload = b"Ozone-NFS-benchmark" * ((chunk_bytes + 18) // 19)
    payload = payload[:chunk_bytes]
    expected_digest = digest_repeated(payload, total_bytes)

    descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        remaining = total_bytes
        chunks = math.ceil(total_bytes / len(payload))
        progress_every = max(1, math.ceil(chunks / 4))
        write_started = time.perf_counter()
        written_chunks = 0
        while remaining:
            chunk = payload[:min(remaining, len(payload))]
            write_all(descriptor, chunk)
            remaining -= len(chunk)
            written_chunks += 1
            if written_chunks % progress_every == 0 or remaining == 0:
                print("sequential write: {}/{} chunks".format(written_chunks, chunks),
                      file=sys.stderr, flush=True)
        write_seconds = time.perf_counter() - write_started

        sync_started = time.perf_counter()
        os.fsync(descriptor)
        sync_seconds = time.perf_counter() - sync_started
    finally:
        os.close(descriptor)

    descriptor = os.open(path, os.O_RDONLY)
    try:
        read_started = time.perf_counter()
        digest = hashlib.sha256()
        bytes_read = 0
        while True:
            chunk = os.read(descriptor, chunk_bytes)
            if not chunk:
                break
            bytes_read += len(chunk)
            digest.update(chunk)
        read_seconds = time.perf_counter() - read_started
    finally:
        os.close(descriptor)

    if bytes_read != total_bytes or digest.digest() != expected_digest:
        raise IOError("sequential readback did not match the bytes written")
    path.unlink()
    return {
        "bytes": total_bytes,
        "chunk_bytes": chunk_bytes,
        "write_seconds": round(write_seconds, 3),
        "write_mib_per_second_including_fsync": round(total_bytes / (write_seconds + sync_seconds) / (1024 ** 2), 3),
        "fsync_seconds": round(sync_seconds, 3),
        "read_seconds": round(read_seconds, 3),
        "read_mib_per_second": round(total_bytes / read_seconds / (1024 ** 2), 3),
        "readback_sha256": digest.hexdigest(),
    }


def small_files(directory, count, size_bytes):
    payload = b"n" * size_bytes
    samples = []
    for index in range(count):
        path = directory / ("small-{:05d}".format(index))
        started = time.perf_counter()
        descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        try:
            write_all(descriptor, payload)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        if path.stat().st_size != size_bytes:
            raise IOError("small-file size mismatch for {}".format(path.name))
        path.unlink()
        samples.append(time.perf_counter() - started)
        if (index + 1) % max(1, count // 5) == 0 or index + 1 == count:
            print("small files: {}/{} complete".format(index + 1, count),
                  file=sys.stderr, flush=True)
    return {
        "files": count,
        "bytes_per_file": size_bytes,
        "create_write_fsync_delete_files_per_second": round(count / sum(samples), 3),
        "operation_latency": percentiles(samples),
    }


def peak_rss_mib():
    # Linux reports ru_maxrss in KiB.
    return round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 3)


def process_rss_mib(pid):
    with open("/proc/{}/status".format(pid), encoding="utf-8") as status:
        for line in status:
            if line.startswith("VmRSS:"):
                return round(int(line.split()[1]) / 1024, 3)
    raise ValueError("process {} has no VmRSS entry".format(pid))


class ProcessRssSampler:
    def __init__(self, processes, interval_seconds=0.25):
        self.processes = processes
        self.interval_seconds = interval_seconds
        self.samples = {name: [] for name, _ in processes}
        self.finished = threading.Event()
        self.thread = threading.Thread(target=self._sample, daemon=True)
        self.stopped = False

    def start(self):
        for name, pid in self.processes:
            self.samples[name].append(process_rss_mib(pid))
        self.thread.start()

    def _sample(self):
        while not self.finished.wait(self.interval_seconds):
            for name, pid in self.processes:
                try:
                    self.samples[name].append(process_rss_mib(pid))
                except FileNotFoundError:
                    pass

    def stop(self):
        if self.stopped:
            return
        self.stopped = True
        self.finished.set()
        if self.thread.is_alive():
            self.thread.join()
        if self.thread.ident is not None:
            for name, pid in self.processes:
                try:
                    self.samples[name].append(process_rss_mib(pid))
                except FileNotFoundError:
                    pass

    def results(self):
        return {
            name: {
                "pid": pid,
                "peak_rss_mib": max(self.samples[name]),
                "samples": len(self.samples[name]),
            }
            for name, pid in self.processes
        }


def run_benchmark(mount, total_bytes, chunk_bytes, small_file_count, small_file_bytes, processes):
    work_dir = mount / ("nfs-benchmark-" + uuid.uuid4().hex)
    work_dir.mkdir()
    sampler = ProcessRssSampler(processes)
    try:
        sampler.start()
        print("starting sequential transfer: {} bytes".format(total_bytes),
              file=sys.stderr, flush=True)
        sequential = sequential_transfer(work_dir, total_bytes, chunk_bytes)
        print("starting small-file workload: {} files".format(small_file_count),
              file=sys.stderr, flush=True)
        small = small_files(work_dir, small_file_count, small_file_bytes)
        sampler.stop()
        return {
            "platform": platform.platform(),
            "architecture": platform.machine(),
            "python": platform.python_version(),
            "mount": mount_options(mount),
            "sequential": sequential,
            "small_files": small,
            "client_process_peak_rss_mib": peak_rss_mib(),
            "server_processes": sampler.results(),
        }
    finally:
        sampler.stop()
        for path in work_dir.iterdir():
            path.unlink()
        work_dir.rmdir()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mount", type=Path, required=True)
    parser.add_argument("--sequential-bytes", type=int, default=32 * 1024 * 1024)
    parser.add_argument("--chunk-bytes", type=int, default=1024 * 1024)
    parser.add_argument("--small-files", type=int, default=50)
    parser.add_argument("--small-file-bytes", type=int, default=4096)
    parser.add_argument(
        "--process", action="append", default=[], metavar="NAME=PID",
        help="sample peak RSS for a local Linux process; repeat for Ganesha, gateway, and OM",
    )
    arguments = parser.parse_args()
    if min(arguments.sequential_bytes, arguments.chunk_bytes,
           arguments.small_files, arguments.small_file_bytes) <= 0:
        parser.error("benchmark sizes and counts must be positive")
    mounted = require_nfs41_krb5p_mount(arguments.mount)
    processes = []
    process_names = set()
    for item in arguments.process:
        try:
            name, pid_text = item.split("=", 1)
            pid = int(pid_text)
        except ValueError:
            parser.error("--process must be NAME=PID")
        if not name or name in process_names or pid <= 0:
            parser.error("--process names must be non-empty and unique, with positive PIDs")
        process_names.add(name)
        processes.append((name, pid))
    print(json.dumps(run_benchmark(
        mounted,
        arguments.sequential_bytes,
        arguments.chunk_bytes,
        arguments.small_files,
        arguments.small_file_bytes,
        processes,
    ), indent=2, sort_keys=True))
