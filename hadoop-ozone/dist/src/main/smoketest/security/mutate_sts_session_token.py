#!/usr/bin/env python3

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

"""
Mutate an STS session token to simulate tampering attacks.

The token is base64url-encoded binary data whose fields are length-prefixed
with Hadoop VInt (variable-length integer) encoding, in this order:
  1. identifier  (contains the session policy JSON)
  2. password    (the HMAC signature bytes)
  3. kind        (token type string, e.g. "STSToken")
  4. service     (the service type, e.g. "STS")

Supported MUTATION_TYPE values:
  service         - corrupt the service type so token lookup fails
  signature       - flip a bit in the password so signature verification fails
  session_policy  - alter the policy inside the identifier to test policy enforcement
"""

import base64
import os


# ---------------------------------------------------------------------------
# Hadoop VInt encoding
#
# Single-byte range: values -112 .. 127 are stored as one byte (as signed).
# Multi-byte:  a leading "length byte" encodes the sign and number of
#              additional bytes:
#   positive multi-byte:  first byte is -113..-120  → 2..9 extra bytes
#   negative multi-byte:  first byte is -121..-128  → 2..9 extra bytes
# ---------------------------------------------------------------------------

def _vint_byte(value: int) -> int:
    """Return value in the range 0-255 (treat Python int as signed byte)."""
    return value & 0xFF


def read_vint(buf: bytes, pos: int) -> tuple[int, int]:
    """Read a Hadoop VInt from buf at pos.  Returns (value, bytes_consumed).

    The first byte encodes the sign and the total byte count:
      -113 .. -120  →  positive, (total - 1) extra bytes  (-111 - first extra bytes)
      -121 .. -128  →  negative, (total - 1) extra bytes  (-119 - first extra bytes)
    """
    if pos >= len(buf):
        raise ValueError(f"VInt read out of bounds at position {pos}")

    first = buf[pos] if buf[pos] < 128 else buf[pos] - 256  # unsigned → signed

    # Single-byte: -112 to 127
    if first >= -112:
        return first, 1

    # Multi-byte: first byte encodes sign and number of extra bytes
    is_negative = first < -120
    # decode_vint_size returns *total* bytes including the first byte;
    # subtract 1 to get the number of payload bytes that follow.
    total_bytes = (-119 - first) if is_negative else (-111 - first)
    n_extra = total_bytes - 1

    end = pos + 1 + n_extra
    if end > len(buf):
        raise ValueError(f"Truncated VInt at position {pos}: need {total_bytes} bytes, have {len(buf) - pos}")

    magnitude = int.from_bytes(buf[pos + 1:end], byteorder="big")
    value = ~magnitude if is_negative else magnitude
    return value, total_bytes


def write_vint(value: int) -> bytes:
    """Encode an integer as a Hadoop VInt.

    For multi-byte values the first byte is:
      positive:  -113 - (n_extra - 1)  →  -113 down to -120  (1..8 extra bytes)
      negative:  -121 - (n_extra - 1)  →  -121 down to -128  (1..8 extra bytes)
    Followed by the magnitude bytes big-endian.
    """
    # Single-byte range: stored directly as signed byte
    if -112 <= value <= 127:
        return bytes([_vint_byte(value)])

    # For multi-byte we store the magnitude (complement for negatives)
    magnitude = (~value) if value < 0 else value

    # Count bytes needed for the magnitude (at least 1)
    tmp = magnitude
    n_extra = 0
    while tmp != 0:
        tmp >>= 8
        n_extra += 1
    if n_extra == 0:
        n_extra = 1

    # First byte encodes sign and extra count
    first = (-120 - n_extra) if value < 0 else (-112 - n_extra)

    return bytes([_vint_byte(first)]) + magnitude.to_bytes(n_extra, byteorder="big")


# ---------------------------------------------------------------------------
# Token parsing / reassembly
# ---------------------------------------------------------------------------

def read_field(buf: bytes, pos: int) -> tuple[bytearray, int]:
    """Read one length-prefixed field.  Returns (field_bytes, new_pos)."""
    length, n = read_vint(buf, pos)
    if length < 0:
        raise ValueError(f"Negative field length {length} at position {pos}")
    pos += n
    if pos + length > len(buf):
        raise ValueError(f"Field length {length} exceeds remaining bytes at position {pos}")
    return bytearray(buf[pos:pos + length]), pos + length


def write_field(data: bytes) -> bytes:
    """Encode one length-prefixed field."""
    return write_vint(len(data)) + bytes(data)


def decode_token(token: str) -> tuple[bytearray, bytearray, bytearray, bytearray]:
    """Base64url-decode and parse token into its four fields."""
    raw = base64.urlsafe_b64decode(token + "=" * ((4 - len(token) % 4) % 4))
    pos = 0
    identifier, pos = read_field(raw, pos)
    password,   pos = read_field(raw, pos)
    kind,       pos = read_field(raw, pos)
    service,    pos = read_field(raw, pos)
    return identifier, password, kind, service


def encode_token(identifier: bytes, password: bytes, kind: bytes, service: bytes) -> str:
    """Reassemble the four fields and base64url-encode the result."""
    raw = write_field(identifier) + write_field(password) + write_field(kind) + write_field(service)
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


# ---------------------------------------------------------------------------
# Mutations
# ---------------------------------------------------------------------------

def mutate_service(service: bytearray) -> bytearray:
    """Replace service bytes with garbage of the same length."""
    return bytearray(b"BAD" if len(service) == 3 else b"X" * len(service))


def mutate_signature(password: bytearray) -> bytearray:
    """Flip the first bit of the signature."""
    if not password:
        raise ValueError("Token password is empty")
    password[0] ^= 0x01
    return password


def mutate_session_policy(identifier: bytearray) -> bytearray:
    """Swap internal grant permission from read to write."""
    old, new = b'"permissions":["read"]', b'"permissions":["write"]'
    pos = identifier.find(old)
    if pos < 0:
        raise ValueError('Could not find \'"permissions":["read"]\' in identifier to mutate')
    identifier[pos:pos + len(old)] = new
    return identifier


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    token = os.environ["SESSION_TOKEN"]
    mutation = os.environ["MUTATION_TYPE"]

    identifier, password, kind, service = decode_token(token)

    if mutation == "service":
        service = mutate_service(service)
    elif mutation == "signature":
        password = mutate_signature(password)
    elif mutation == "session_policy":
        identifier = mutate_session_policy(identifier)
    else:
        raise ValueError(f"Unsupported mutation type: {mutation!r}")

    print(encode_token(identifier, password, kind, service))


if __name__ == "__main__":
    main()
