#!/usr/bin/env python3
"""Object-storage conformance probe for one profile of config/storage.yaml.

Runs the S3 operations an object-storage index backend relies on against the profile's bucket,
under a fresh prefix below the profile prefix, through the aws CLI, and writes a JSON report.
Credentials are read from the environment variables the profile names and never printed.
Usage: scripts/check_storage.py PROFILE REPORT [--storage config/storage.yaml]
"""
import argparse
import datetime
import json
import os
import subprocess
import sys
import tempfile

import yaml

# Required by rocksdb-cloud's single-writer use; conditional create is reported, not required.
REQUIRED = ["put", "head", "get", "range_get", "list", "multipart", "copy", "delete", "list_after_delete"]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("profile")
    parser.add_argument("report")
    parser.add_argument("--storage", default="config/storage.yaml")
    args = parser.parse_args()
    if os.path.exists(args.report):
        sys.exit(f"report already exists: {args.report}")
    profile = yaml.safe_load(open(args.storage))["profiles"][args.profile]
    if profile["kind"] != "s3":
        sys.exit("only S3-compatible profiles are supported")
    env = dict(os.environ)
    env["AWS_ACCESS_KEY_ID"] = os.environ[profile["access_key_env"]]
    env["AWS_SECRET_ACCESS_KEY"] = os.environ[profile["secret_key_env"]]
    token = os.environ.get(profile.get("session_token_env", ""), "")
    if token:
        env["AWS_SESSION_TOKEN"] = token
    else:
        env.pop("AWS_SESSION_TOKEN", None)
    env.pop("AWS_PROFILE", None)
    env["AWS_SHARED_CREDENTIALS_FILE"] = os.devnull
    # rocksdb-cloud's AWS SDK 1.8.8 sends no request checksums; COS rejects the CLI's default ones.
    env["AWS_REQUEST_CHECKSUM_CALCULATION"] = "when_required"
    env["AWS_RESPONSE_CHECKSUM_VALIDATION"] = "when_required"
    work = tempfile.mkdtemp(prefix="check-storage-")
    config = os.path.join(work, "aws.cfg")
    style = "virtual" if profile["addressing"] == "virtual_hosted" else "path"
    with open(config, "w") as out:
        out.write(f"[default]\nregion = {profile['region']}\ns3 =\n  addressing_style = {style}\n")
    env["AWS_CONFIG_FILE"] = config
    bucket = profile["bucket"]
    prefix = profile["prefix"] + "check-storage-" + datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ") + "/"

    def api(*command, output=None):
        result = subprocess.run(
            ["aws", "s3api", "--endpoint-url", profile["endpoint"], *command, "--output", "json"],
            env=env, capture_output=True, text=True)
        error = None
        if result.returncode != 0:
            lines = result.stderr.strip().splitlines()
            error = lines[-1] if lines else f"exit {result.returncode}"
        return result.returncode == 0, (json.loads(result.stdout) if result.returncode == 0 and result.stdout.strip() else None), error

    checks = {}

    def record(name, ok, detail=None):
        checks[name] = {"ok": ok, "detail": detail}

    small = os.path.join(work, "small.bin")
    with open(small, "wb") as out:
        out.write(b"hello-sibench-0123456789")
    ok, _, error = api("put-object", "--bucket", bucket, "--key", prefix + "small.bin", "--body", small)
    record("put", ok, error)
    ok, head, error = api("head-object", "--bucket", bucket, "--key", prefix + "small.bin")
    record("head", ok and head.get("ContentLength") == 24, error or (head or {}).get("ContentLength"))
    got = os.path.join(work, "got.bin")
    ok, _, error = api("get-object", "--bucket", bucket, "--key", prefix + "small.bin", got)
    record("get", ok and open(got, "rb").read() == b"hello-sibench-0123456789", error)
    ok, _, error = api("get-object", "--bucket", bucket, "--key", prefix + "small.bin", "--range", "bytes=6-12", got)
    record("range_get", ok and open(got, "rb").read() == b"sibench", error)
    ok, listing, error = api("list-objects-v2", "--bucket", bucket, "--prefix", prefix)
    keys = [item["Key"] for item in (listing or {}).get("Contents", [])]
    record("list", ok and keys == [prefix + "small.bin"], error or keys)
    ok, _, error = api("put-object", "--bucket", bucket, "--key", prefix + "small.bin", "--body", small, "--if-none-match", "*")
    record("conditional_create_refuses_existing", not ok, error or "existing object was overwritten")
    part1, part2 = os.urandom(5 * 1024 * 1024), os.urandom(1024)
    paths = []
    for index, data in enumerate((part1, part2), start=1):
        path = os.path.join(work, f"part{index}.bin")
        with open(path, "wb") as out:
            out.write(data)
        paths.append(path)
    ok, created, error = api("create-multipart-upload", "--bucket", bucket, "--key", prefix + "multipart.bin")
    multipart_ok, multipart_error = ok, error
    if ok:
        upload = created["UploadId"]
        parts = []
        for number, path in enumerate(paths, start=1):
            ok, part, error = api("upload-part", "--bucket", bucket, "--key", prefix + "multipart.bin",
                                  "--upload-id", upload, "--part-number", str(number), "--body", path)
            if not ok:
                multipart_ok, multipart_error = False, error
                break
            parts.append({"ETag": part["ETag"], "PartNumber": number})
        if multipart_ok:
            manifest = os.path.join(work, "parts.json")
            json.dump({"Parts": parts}, open(manifest, "w"))
            ok, _, error = api("complete-multipart-upload", "--bucket", bucket, "--key", prefix + "multipart.bin",
                               "--upload-id", upload, "--multipart-upload", "file://" + manifest)
            multipart_ok, multipart_error = ok, error
            if ok:
                ok, _, error = api("get-object", "--bucket", bucket, "--key", prefix + "multipart.bin", got)
                multipart_ok = ok and open(got, "rb").read() == part1 + part2
                multipart_error = error
        else:
            api("abort-multipart-upload", "--bucket", bucket, "--key", prefix + "multipart.bin", "--upload-id", upload)
    record("multipart", multipart_ok, multipart_error)
    ok, _, error = api("copy-object", "--bucket", bucket, "--key", prefix + "copy.bin",
                       "--copy-source", f"{bucket}/{prefix}small.bin")
    record("copy", ok, error)
    deleted = True
    for key in ("small.bin", "multipart.bin", "copy.bin"):
        ok, _, error = api("delete-object", "--bucket", bucket, "--key", prefix + key)
        deleted &= ok
    record("delete", deleted, None)
    ok, listing, error = api("list-objects-v2", "--bucket", bucket, "--prefix", prefix)
    remaining = [item["Key"] for item in (listing or {}).get("Contents", [])]
    record("list_after_delete", ok and not remaining, error or remaining)
    ok, versioning, error = api("get-bucket-versioning", "--bucket", bucket)
    record("versioning_disabled", ok and (versioning or {}).get("Status") in (None, "Suspended"), error or versioning)
    report = {
        "profile": args.profile,
        "endpoint": profile["endpoint"],
        "bucket": bucket,
        "prefix": prefix,
        "addressing": profile["addressing"],
        "checks": checks,
        "required": REQUIRED,
        "status": "passed" if all(checks[name]["ok"] for name in REQUIRED) else "failed",
    }
    with open(args.report, "x") as out:
        json.dump(report, out, indent=2)
    print(json.dumps({name: check["ok"] for name, check in checks.items()} | {"status": report["status"]}))
    sys.exit(0 if report["status"] == "passed" else 1)


if __name__ == "__main__":
    main()
