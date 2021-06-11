from dvc.fs.oss import OSSFileSystem

bucket_name = "bucket-name"
endpoint = "endpoint"
key_id = "Fq2UVErCz4I6tq"
key_secret = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsu"


def test_init(dvc):
    prefix = "some/prefix"
    url = f"oss://{bucket_name}/{prefix}"
    config = {
        "url": url,
        "oss_key_id": key_id,
        "oss_key_secret": key_secret,
        "oss_endpoint": endpoint,
    }
    fs = OSSFileSystem(**config)
    assert fs.endpoint == endpoint
    assert fs.key_id == key_id
    assert fs.key_secret == key_secret
