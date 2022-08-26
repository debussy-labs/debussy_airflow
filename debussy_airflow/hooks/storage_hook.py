import io
import os
import shutil
from abc import ABC, abstractmethod
from typing import Optional, Sequence, Union
from uuid import uuid4

import paramiko
import pysftp
from airflow.hooks.base_hook import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.google.cloud.hooks.gcs import GCSHook as AirflowGCSHook
from airflow.providers.google.cloud.hooks.gcs import _parse_gcs_url


def parse_blob_list_to_ls_behavior(full_path_blobs, prefix):
    # relative to remove_uri
    relative_path_blobs = [
        full_path_blob.replace(prefix, "", 1) for full_path_blob in full_path_blobs
    ]
    # only the names of the structures immediately next to remote_uri. eg:
    # if folder contains '/folder/a' and '/folder/b'. if remote uri is '/' only returns 'folder'
    remote_uri_level_names = list(
        {relative_path_blob.split("/")[0] for relative_path_blob in relative_path_blobs}
    )
    return remote_uri_level_names


class StorageHookInterface(BaseHook, ABC):
    @abstractmethod
    def upload_file(self, remote_file_uri, local_file_uri, **kwargs):
        pass

    @abstractmethod
    def upload_string(self, remote_file_uri, data_string, **kwargs):
        pass

    @abstractmethod
    def download_file(self, remote_file_uri, local_file_uri, **kwargs):
        pass

    @abstractmethod
    def download_string(self, remote_file_uri, **kwargs):
        pass

    @abstractmethod
    def exists_file(self, file_path, **kwargs):
        pass

    @abstractmethod
    def list_dir(self, remote_uri):
        pass

    @staticmethod
    def basename(path_uri):
        return os.path.basename(path_uri)


class GCSHook(StorageHookInterface, AirflowGCSHook):
    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
    ):
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            delegate_to=delegate_to,
            impersonation_chain=impersonation_chain,
        )

    def upload_file(
        self, remote_file_uri, local_file_uri, mime_type="application/octet-stream"
    ):
        self.log.info(
            f"GCSHook - Uploading file from {local_file_uri} to {remote_file_uri}"
        )
        bucket, object_path = _parse_gcs_url(remote_file_uri)

        return super().upload(
            bucket_name=bucket,
            object_name=object_path,
            filename=local_file_uri,
            mime_type=mime_type,
        )

    def upload_string(self, remote_file_uri, data_string, mime_type="text/plain"):
        bucket, object_path = _parse_gcs_url(remote_file_uri)
        client = self.get_conn()
        bucket = client.bucket(bucket)
        blob = bucket.blob(blob_name=object_path)
        blob.upload_from_string(data=data_string, content_type=mime_type)

    def download_file(self, remote_file_uri, local_file_uri):
        bucket, object_path = _parse_gcs_url(remote_file_uri)
        super().download(bucket, object_path, local_file_uri)

    def download_string(self, remote_file_uri, encoding="utf-8"):
        bucket, object_path = _parse_gcs_url(remote_file_uri)
        return super().download(bucket, object_path).decode(encoding)

    def exists_file(self, file_path):
        bucket, object_path = _parse_gcs_url(file_path)
        exists_file = super().exists(bucket_name=bucket, object_name=object_path)
        return exists_file

    def list_dir(self, remote_uri):
        # this function should mimic the behavior of `ls` in a linux shell
        if not remote_uri.endswith("/"):
            remote_uri = remote_uri + "/"
        bucket, object_path = _parse_gcs_url(remote_uri)
        full_path_blobs = self.list(bucket_name=bucket, prefix=object_path)
        return parse_blob_list_to_ls_behavior(full_path_blobs, prefix=object_path)


class SFTPHook(StorageHookInterface):
    def __init__(self, sftp_conn_id: str, temp_folder: str = "/tmp"):
        self.sftp_conn_id = sftp_conn_id
        self.temp_folder = temp_folder

    def get_conn(self):
        if self.sftp_conn_id is None:
            raise ValueError("sftp_conn_id is None")
        conn = self.get_connection(self.sftp_conn_id)
        cnopts = pysftp.CnOpts()
        conn_params = {
            "host": conn.host,
            "port": conn.port,
            "username": conn.login,
            "cnopts": cnopts,
        }
        if (
            "ignore_hostkey_verification" in conn.extra_dejson
            and conn.extra_dejson["ignore_hostkey_verification"]
        ):
            cnopts.hostkeys = None
        if conn.password is not None:
            conn_params["password"] = conn.password
        if "private_key" in conn.extra_dejson:
            key_file_like = io.StringIO(conn.extra_dejson["private_key"])
            key_ready = paramiko.RSAKey.from_private_key(file_obj=key_file_like)
            conn_params["private_key"] = key_ready
        if "private_key_pass" in conn.extra_dejson:
            conn_params["private_key_pass"] = conn.extra_dejson["private_key_pass"]

        return pysftp.Connection(**conn_params)

    def upload_file(self, remote_file_uri, local_file_uri):
        self.log.info(
            f"SFTPHook - Uploading file from {local_file_uri} to {remote_file_uri}"
        )
        with self.get_conn() as sftp:
            # FIXME: remote_file_uri is fully qualified and has should have file name. need to deal with file path
            if remote_file_uri:
                with sftp.cd(remote_file_uri):
                    sftp.put(local_file_uri)
            else:
                sftp.put(local_file_uri)

    @staticmethod
    def basename(path_uri):
        return os.path.basename(path_uri)

    def upload_string(self, remote_file_uri, data_string):
        # FIXME: might use remote file uri file name instead of random name, can use a random named folder tho
        local_filename = os.path.join(self.temp_folder, f"{uuid4()}")
        with open(local_filename, "w") as f:
            f.write(data_string)
        self.upload_file(remote_file_uri=remote_file_uri, local_file_uri=local_filename)

    def list_dir(self, remote_uri):
        # code from pysftp get_d but yielding the remote and local paths instead of downloading
        from stat import S_ISREG

        # from pysftp.helpers import reparent
        files = []
        with self.get_conn() as sftp_conn:
            with sftp_conn.cd(remote_uri):
                for sattr in sftp_conn.listdir_attr("."):
                    if S_ISREG(sattr.st_mode):
                        rname = sattr.filename
                        files.append(rname)
        return files

    def download_file(self, remote_file_uri, local_file_uri):
        self.log.info(
            f"SFTPHook - Downloading file from {remote_file_uri} to {local_file_uri}"
        )
        with self.get_conn() as sftp_conn:
            sftp_conn.get(remote_file_uri, local_file_uri)

    def download_string(self, remote_file_uri):
        local_filename = os.path.join(self.temp_folder, f"{uuid4()}")
        self.download_file(
            bucket=None, remote_file_uri=remote_file_uri, local_file_uri=local_filename
        )
        with open(local_filename) as f:
            ret = f.read()
        return ret

    def exists_file(self, file_path):
        raise NotImplementedError()


class S3Hook(S3Hook, StorageHookInterface):
    # S3Hook adapater for StorageHookInterface
    def upload_file(self, remote_file_uri, local_file_uri, **kwargs):
        bucket_name, key = super().parse_s3_url(remote_file_uri)
        super().load_file(
            filename=local_file_uri, key=key, bucket_name=bucket_name, replace=True
        )

    def upload_string(self, remote_file_uri, data_string, replace=True, **kwargs):
        bucket_name, key = super().parse_s3_url(remote_file_uri)
        super().load_string(
            string_data=data_string, key=key, bucket_name=bucket_name, replace=replace
        )

    def download_file(self, remote_file_uri, local_file_uri):
        bucket_name, key = super().parse_s3_url(remote_file_uri)
        src = super().download_file(key=key, bucket_name=bucket_name)
        shutil.copyfile(src, local_file_uri)

    def download_string(self, remote_file_uri):
        bucket_name, key = super().parse_s3_url(remote_file_uri)
        file = super().download_file(key=key, bucket_name=bucket_name)
        with open(file) as f:
            data = f.read()
        return data

    def exists_file(self, file_path, **kwargs):
        bucket_name, key = super().parse_s3_url(file_path)
        return super().check_for_key(key=key, bucket_name=bucket_name)

    def list_dir(self, remote_uri):
        # this function should mimic the behavior of `ls` in a linux shell
        if not remote_uri.endswith("/"):
            remote_uri = remote_uri + "/"
        bucket_name, key = super().parse_s3_url(remote_uri)
        full_path_blobs = super().list_keys(bucket_name=bucket_name, prefix=key)
        return parse_blob_list_to_ls_behavior(full_path_blobs, prefix=key)
