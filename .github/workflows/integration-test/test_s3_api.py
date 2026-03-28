#!/usr/bin/env python3
"""Integration tests for go-s3-server S3 API, auth, write-once, and sharding."""

import os
import pathlib
import unittest
import urllib.request

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

ENDPOINT_NORMAL = "http://127.0.0.1:9000"
ENDPOINT_WRITEONCE = "http://127.0.0.1:9001"
BUCKET = "test-cache"
REGION = "us-east-1"
ACCESS_KEY = "test-key-id"
SECRET_KEY = "test-secret-key"
DATA_DIR_NORMAL = "/tmp/s3-data"
DATA_DIR_WRITEONCE = "/tmp/s3-data-writeonce"


def make_client(endpoint, access_key=ACCESS_KEY, secret_key=SECRET_KEY):
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=REGION,
        config=Config(s3={"payload_signing_enabled": True}),
    )


class TestS3API(unittest.TestCase):
    """Tests against the normal (non-write-once) server on port 9000."""

    @classmethod
    def setUpClass(cls):
        cls.s3 = make_client(ENDPOINT_NORMAL)

    def test_put_and_get_object(self):
        key = "api/v1test000000000001"
        body = b"hello world cache data"
        self.s3.put_object(Bucket=BUCKET, Key=key, Body=body)

        resp = self.s3.get_object(Bucket=BUCKET, Key=key)
        got = resp["Body"].read()
        self.assertEqual(body, got)

    def test_get_nonexistent_returns_404(self):
        with self.assertRaises(ClientError) as ctx:
            self.s3.get_object(Bucket=BUCKET, Key="nonexistent/v1xxxx000000000000")
        self.assertEqual(
            ctx.exception.response["Error"]["Code"], "NoSuchKey"
        )

    def test_list_objects_v2(self):
        prefix = "listtest/"
        keys = [
            f"{prefix}v1aaaa{i:012d}" for i in range(3)
        ]
        for k in keys:
            self.s3.put_object(Bucket=BUCKET, Key=k, Body=b"data")

        resp = self.s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
        listed_keys = [obj["Key"] for obj in resp.get("Contents", [])]
        self.assertEqual(len(listed_keys), 3)
        self.assertEqual(listed_keys, sorted(listed_keys))

    def test_list_objects_pagination(self):
        prefix = "pagtest/"
        keys = [f"{prefix}v1aaaa{i:012d}" for i in range(5)]
        for k in keys:
            self.s3.put_object(Bucket=BUCKET, Key=k, Body=b"d")

        # Page 1
        resp = self.s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix, MaxKeys=2)
        self.assertEqual(len(resp["Contents"]), 2)
        self.assertTrue(resp["IsTruncated"])

        # Page 2
        resp = self.s3.list_objects_v2(
            Bucket=BUCKET,
            Prefix=prefix,
            MaxKeys=2,
            ContinuationToken=resp["NextContinuationToken"],
        )
        self.assertEqual(len(resp["Contents"]), 2)
        self.assertTrue(resp["IsTruncated"])

        # Page 3
        resp = self.s3.list_objects_v2(
            Bucket=BUCKET,
            Prefix=prefix,
            MaxKeys=2,
            ContinuationToken=resp["NextContinuationToken"],
        )
        self.assertEqual(len(resp["Contents"]), 1)
        self.assertFalse(resp["IsTruncated"])

    def test_put_overwrite_allowed(self):
        key = "overwrite/v1test000000000002"
        self.s3.put_object(Bucket=BUCKET, Key=key, Body=b"first")
        self.s3.put_object(Bucket=BUCKET, Key=key, Body=b"second")

        resp = self.s3.get_object(Bucket=BUCKET, Key=key)
        self.assertEqual(resp["Body"].read(), b"second")

    def test_sharded_storage(self):
        key = "shardtest/v1aabbccdd11223344"
        self.s3.put_object(Bucket=BUCKET, Key=key, Body=b"sharddata")

        # Expected sharded path: {data_dir}/shardtest/v1/aa/bbccdd11223344
        expected = pathlib.Path(DATA_DIR_NORMAL) / "shardtest" / "v1" / "aa" / "bbccdd11223344"
        self.assertTrue(expected.exists(), f"Sharded file not found at {expected}")
        self.assertEqual(expected.read_bytes(), b"sharddata")

    def test_metadata_roundtrip(self):
        key = "metatest/v1meta000000000001"
        meta = {"outputid": "abc123", "custom": "val2"}
        self.s3.put_object(Bucket=BUCKET, Key=key, Body=b"metabody", Metadata=meta)

        resp = self.s3.get_object(Bucket=BUCKET, Key=key)
        got_meta = resp.get("Metadata", {})
        self.assertEqual(got_meta.get("Outputid") or got_meta.get("outputid"), "abc123")
        self.assertEqual(got_meta.get("Custom") or got_meta.get("custom"), "val2")


class TestAuth(unittest.TestCase):
    """Auth tests against the normal server."""

    def test_invalid_secret_key(self):
        s3 = make_client(ENDPOINT_NORMAL, secret_key="wrongsecret")
        with self.assertRaises(ClientError) as ctx:
            s3.get_object(Bucket=BUCKET, Key="auth/v1test000000000001")
        self.assertEqual(
            ctx.exception.response["ResponseMetadata"]["HTTPStatusCode"], 403
        )

    def test_invalid_access_key(self):
        s3 = make_client(ENDPOINT_NORMAL, access_key="NONEXISTENT")
        with self.assertRaises(ClientError) as ctx:
            s3.get_object(Bucket=BUCKET, Key="auth/v1test000000000002")
        self.assertEqual(
            ctx.exception.response["ResponseMetadata"]["HTTPStatusCode"], 403
        )

    def test_no_auth_header(self):
        try:
            req = urllib.request.Request(
                f"{ENDPOINT_NORMAL}/{BUCKET}/auth/v1test000000000003"
            )
            urllib.request.urlopen(req)
            self.fail("Expected HTTP error for unsigned request")
        except urllib.error.HTTPError as e:
            self.assertEqual(e.code, 403)


class TestWriteOnce(unittest.TestCase):
    """Tests against the write-once server on port 9001."""

    @classmethod
    def setUpClass(cls):
        cls.s3 = make_client(ENDPOINT_WRITEONCE)

    def test_first_put_succeeds(self):
        key = "wo/v1first000000000001"
        self.s3.put_object(Bucket=BUCKET, Key=key, Body=b"first write")

        resp = self.s3.get_object(Bucket=BUCKET, Key=key)
        self.assertEqual(resp["Body"].read(), b"first write")

    def test_same_content_idempotent(self):
        key = "wo/v1idempotent0000001"
        body = b"idempotent content"
        self.s3.put_object(Bucket=BUCKET, Key=key, Body=body)
        # Second PUT with same content should succeed
        self.s3.put_object(Bucket=BUCKET, Key=key, Body=body)

        resp = self.s3.get_object(Bucket=BUCKET, Key=key)
        self.assertEqual(resp["Body"].read(), body)

    def test_different_content_returns_409(self):
        key = "wo/v1conflict000000001"
        self.s3.put_object(Bucket=BUCKET, Key=key, Body=b"original")

        with self.assertRaises(ClientError) as ctx:
            self.s3.put_object(Bucket=BUCKET, Key=key, Body=b"different")
        self.assertEqual(
            ctx.exception.response["ResponseMetadata"]["HTTPStatusCode"], 409
        )

    def test_original_content_preserved(self):
        key = "wo/v1preserve000000001"
        self.s3.put_object(Bucket=BUCKET, Key=key, Body=b"keep this")

        try:
            self.s3.put_object(Bucket=BUCKET, Key=key, Body=b"discard this")
        except ClientError:
            pass

        resp = self.s3.get_object(Bucket=BUCKET, Key=key)
        self.assertEqual(resp["Body"].read(), b"keep this")

    def test_sharded_storage_writeonce(self):
        key = "woshard/v1aabbccdd99887766"
        self.s3.put_object(Bucket=BUCKET, Key=key, Body=b"wosharddata")

        expected = pathlib.Path(DATA_DIR_WRITEONCE) / "woshard" / "v1" / "aa" / "bbccdd99887766"
        self.assertTrue(expected.exists(), f"Sharded file not found at {expected}")


if __name__ == "__main__":
    unittest.main(verbosity=2)
