import os

import pytest
from mock import call, patch
from osfclient.models import File, Folder, OSFCore, Project, Storage
from osfclient.tests import mocks
from osfclient.tests.fake_responses import files_node, project_node

from dvc.exceptions import DvcException
from dvc.path_info import URLInfo
from dvc.tree.osf import OSFTree

username = "example@mail.com"
data_dir = "data"
url = f"osf://osf.io/{data_dir}"
project = "abcd"
password = "12345"
config = {
    "url": url,
    "project": project,
    "user": username,
    "password": password,
}


@pytest.fixture
def passwd_env_var():
    os.environ["OSF_PASSWORD"] = password
    os.environ["OSF_USER"] = username
    os.environ["OSF_PROJECT0"] = project
    yield

    del os.environ["OSF_PASSWORD"]
    del os.environ["OSF_USER"]
    del os.environ["OSF_PROJECT0"]


def test_init(dvc):
    tree = OSFTree(dvc, config)

    assert tree.path_info == url
    assert tree.project_guid == project
    assert tree.password == password
    assert tree.user == username


def test_init_envvar(dvc, passwd_env_var):
    config_env = {"url": url, "project": project, "osf_username": username}
    tree = OSFTree(dvc, config_env)

    assert tree.password == password


@patch.object(
    OSFCore, "_get", return_value=mocks.FakeResponse(200, project_node)
)
def test_project(OSFCore_get, dvc):
    tree = OSFTree(dvc, config)
    proj = tree.project

    calls = [
        call("https://api.osf.io/v2//guids/abcd/"),
        call("https://api.osf.io/v2//nodes/abcd/"),
    ]
    OSFCore_get.assert_has_calls(calls)

    assert isinstance(proj, Project)


@patch.object(OSFCore, "_get")
def test_list_paths(OSFCore_get, dvc):
    _files_url = (
        f"https://api.osf.io/v2//nodes/{project}/files/osfstorage/foo123"
    )
    json = files_node(project, "osfstorage", ["foo/hello.txt", "foo/bye.txt"])
    response = mocks.FakeResponse(200, json)
    OSFCore_get.return_value = response

    store = Folder({})
    store._files_url = _files_url

    with patch.object(OSFTree, "storage", new=store):
        tree = OSFTree(dvc, config)
        files = list(tree._list_paths())
        assert len(files) == 2
        assert "/foo/hello.txt" in files
        assert "/foo/bye.txt" in files

    OSFCore_get.assert_called_once_with(_files_url)


def test_get_file_obj(dvc):
    store = Storage({})
    store._files_url = (
        f"https://api.osf.io/v2//nodes/{project}/files/osfstorage"
    )

    json1 = files_node(
        project,
        "osfstorage",
        file_names=["hello.txt", "bye.txt"],
        folder_names=["data"],
    )
    top_level_response = mocks.FakeResponse(200, json1)

    second_level_url = (
        "https://api.osf.io/v2/nodes/9zpcy/files/osfstorage/data123/"
    )
    json2 = files_node(
        project, "osfstorage", file_names=["hello2.txt", "bye2.txt"]
    )
    second_level_response = mocks.FakeResponse(200, json2)

    def simple_OSFCore_get(request_url):
        if request_url == store._files_url:
            return top_level_response
        elif request_url == second_level_url:
            return second_level_response

    with patch.object(
        OSFCore, "_get", side_effect=simple_OSFCore_get
    ), patch.object(OSFTree, "storage", new=store):
        path_info = URLInfo(url) / "hello2.txt"
        tree = OSFTree(dvc, config)
        file = tree._get_file_obj(path_info)
        assert isinstance(file, File)
        assert file.name == "hello2.txt"


def test_is_dir(dvc):
    path_info = URLInfo(url) / "dir/"
    f = File({})

    f.path = "/data/dir/"
    with patch.object(OSFTree, "_get_file_obj", return_value=f):
        tree = OSFTree(dvc, config)
        assert tree.isdir(path_info)

    f.path = "/data/file"
    with patch.object(OSFTree, "_get_file_obj", return_value=f):
        tree = OSFTree(dvc, config)
        assert not tree.isdir(path_info)


def test_walk_files(dvc):
    path_info = URLInfo(url)

    f1 = "/data/dir/"
    f2 = "/data/file1"
    f3 = "/data/file2"

    with patch.object(OSFTree, "_list_paths", return_value=[f1, f2, f3]):
        tree = OSFTree(dvc, config)
        files = [i.url for i in tree.walk_files(path_info)]
        assert "osf://osf.io/data/file1" in files
        assert "osf://osf.io/data/file2" in files


def test_get_md5(dvc):
    path_info = URLInfo(url) / "data/file"

    f = File({})
    f.path = "/data/file"
    f.hashes = {"md5": "md5_hash"}

    with patch.object(OSFTree, "_get_file_obj", return_value=f):
        tree = OSFTree(dvc, config)
        # print(tree._get_file_obj(path_info))
        assert tree.get_md5(path_info) == "md5_hash"


@patch.object(File, "remove")
def test_remove(File_remove, dvc):
    path_info = URLInfo(url) / "data/file"
    f = File({})
    f.path = "/data/file"

    with patch.object(OSFTree, "_get_file_obj", return_value=f):
        tree = OSFTree(dvc, config)
        tree.remove(path_info)

    File_remove.assert_called_once()


def test_download(dvc, tmp_dir):
    def write_mock(f):
        f.write("test".encode("utf-8"))

    path_info = URLInfo(url) / "data/file"
    f = File({})
    f.path = "/data/file"
    f.write_to = write_mock

    to_file = tmp_dir / "file"

    with patch.object(OSFTree, "_get_file_obj", return_value=f):
        tree = OSFTree(dvc, config)
        tree._download(path_info, to_file, no_progress_bar=True)

    with open((tmp_dir / "file"), "rb") as fp:
        assert fp.read().decode("utf-8") == "test"


@patch.object(Storage, "create_file")
def test_upload(Storage_create_file, dvc, tmp_dir):
    to_info = URLInfo(url) / "data"
    store = Storage({})
    from_file = tmp_dir / "file"
    with open(from_file, "w") as f:
        f.write("test")

    with patch.object(OSFTree, "storage", new=store):
        tree = OSFTree(dvc, config)
        tree._upload(from_file, to_info, no_progress_bar=True)

    Storage_create_file.assert_called_once()


def test_get_file_obj_exception(dvc):
    store = Storage({})
    store._files_url = (
        f"https://api.osf.io/v2//nodes/{project}/files/osfstorage"
    )

    json1 = files_node(
        project,
        "osfstorage",
        file_names=["hello.txt", "bye.txt"],
        folder_names=["data"],
    )
    response = mocks.FakeResponse(429, json1)

    with patch.object(OSFCore, "_get", return_value=response), patch.object(
        OSFTree, "storage", new=store
    ):
        path_info = URLInfo(url) / "hello.txt"
        tree = OSFTree(dvc, config)
        with pytest.raises(DvcException):
            tree._get_file_obj(path_info)


def test_upload_exception(dvc, tmp_dir):
    to_info = URLInfo(url) / "data"
    from_file = tmp_dir / "file"
    with open(from_file, "w") as f:
        f.write("test")

    def mock_create_file(*args, **kwargs):
        raise RuntimeError

    with patch.object(Storage, "create_file", new=mock_create_file):
        store = Storage({})

        with patch.object(OSFTree, "storage", new=store):
            tree = OSFTree(dvc, config)
            with pytest.raises(DvcException):
                tree._upload(from_file, to_info, no_progress_bar=True)


def test_download_exception(dvc, tmp_dir):
    def write_mock(*args, **kwargs):
        raise RuntimeError

    path_info = URLInfo(url) / "data/file"
    f = File({})
    f.path = "/data/file"
    f.write_to = write_mock

    to_file = tmp_dir / "file"

    with patch.object(OSFTree, "_get_file_obj", return_value=f):
        tree = OSFTree(dvc, config)
        with pytest.raises(DvcException):
            tree._download(path_info, to_file, no_progress_bar=True)
