import hashlib

from berg_pipeline.resources import R2Resource


def _r2():
    return R2Resource(
        account_id="account",
        bucket="bucket",
        access_key_id="key",
        secret_access_key="secret",
    )


def test_single_part_object_matches_content_not_only_size(tmp_path):
    path = tmp_path / "day.parquet"
    path.write_bytes(b"correct")
    r2 = _r2()

    good = {"size": 7, "etag": f'"{hashlib.md5(b"correct").hexdigest()}"'}  # noqa: S324
    same_size_wrong = {"size": 7, "etag": hashlib.md5(b"corrupt").hexdigest()}  # noqa: S324

    assert r2.object_matches(path, "legs/day.parquet", good)
    assert not r2.object_matches(path, "legs/day.parquet", same_size_wrong)


def test_multipart_object_matches_sha256_metadata(tmp_path):
    path = tmp_path / "routes.pmtiles"
    path.write_bytes(b"route bytes")

    class Client:
        def __init__(self, checksum):
            self.checksum = checksum

        def head_object(self, **kwargs):
            assert kwargs == {"Bucket": "bucket", "Key": "static/routes.pmtiles"}
            return {"Metadata": {"berg-sha256": self.checksum}}

    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    remote = {"size": path.stat().st_size, "etag": '"multipart-2"'}
    r2 = _r2()

    assert r2.object_matches(path, "static/routes.pmtiles", remote, client=Client(digest))
    assert not r2.object_matches(path, "static/routes.pmtiles", remote, client=Client("0" * 64))


def test_upload_records_sha256_metadata(tmp_path):
    path = tmp_path / "stations.json"
    path.write_bytes(b"{}")

    class Client:
        call = None

        def upload_file(self, *args, **kwargs):
            self.call = (args, kwargs)

    client = Client()
    _r2().upload(path, "static/stations.json", client=client)

    assert client.call == (
        (str(path), "bucket", "static/stations.json"),
        {"ExtraArgs": {"Metadata": {"berg-sha256": hashlib.sha256(b"{}").hexdigest()}}},
    )
