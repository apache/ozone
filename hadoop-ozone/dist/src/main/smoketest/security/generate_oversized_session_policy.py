#!/usr/bin/env python3

import json


def main() -> None:
    statement = {
        "Effect": "Allow",
        "Action": "s3:GetObject",
        "Resource": "arn:aws:s3:::bucket123/*",
    }
    policy = {"Version": "2012-10-17", "Statement": [statement]}
    base = json.dumps(policy, separators=(",", ":"))
    # Keep the payload comfortably above the STS policy size limit.
    policy["Pad"] = "X" * (35000 - len(base) + 64)
    print(json.dumps(policy, separators=(",", ":")))


if __name__ == "__main__":
    main()
