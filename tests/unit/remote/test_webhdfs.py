from dvc.fs.webhdfs import WebHDFSFileSystem

user = "test"
webhdfs_token = "token"
webhdfs_alias = "alias-name"
hdfscli_config = "path/to/cli/config"


def test_init(dvc):
    url = "webhdfs://test@127.0.0.1:50070"
    config = {
        "host": url,
        "webhdfs_token": webhdfs_token,
        "webhdfs_alias": webhdfs_alias,
        "hdfscli_config": hdfscli_config,
        "user": user,
    }

    fs = WebHDFSFileSystem(**config)
    assert fs.token == webhdfs_token
    assert fs.alias == webhdfs_alias
    assert fs.user == user
    assert fs.hdfscli_config == hdfscli_config
