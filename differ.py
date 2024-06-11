import gzip
import os
import struct
import sys
from io import BytesIO
from pathlib import Path
import time
from urllib.parse import urlparse

import boto3
from mypy_boto3_s3 import S3Client

S3_CLIENT: S3Client = boto3.client("s3")
MULTIPART_TRIGGER_SIZE = 10 << 19
S3_TAG_UNCOMPRESSED_SIZE = "total_uncompressed_size"


class S3Object:
    """Represents an S3 Object and handles operations related to the object.
    """
    def __init__(self, uri: str) -> None:
        url = urlparse(uri)
        self._bucket = url.netloc
        self._key = url.path.lstrip("/")
        self._size: int | None = None
        pass

    def exists(self) -> bool:
        try:
            resp = S3_CLIENT.head_object(Bucket=self._bucket, Key=self._key)
            self._size = resp.get("ContentLength")
            return True
        except S3_CLIENT.exceptions.NoSuchKey:
            return False
        except S3_CLIENT.exceptions.ClientError as err:
            # NOTE: This case is required because of https://github.com/boto/boto3/issues/2442
            if err.response["Error"]["Code"] == "404":
                return False
            else:
                raise err

    def size(self, refresh=False) -> int:
        if self._size is None or refresh:
            self.exists()

        return self._size  # type: ignore

    def _get_bytes_from_gz_footer(self) -> int:
        last_four_bytes = S3_CLIENT.get_object(
            Bucket=self._bucket,
            Key=self._key,
            Range=f"bytes={self._size-4}-{self._size}",  # type: ignore
        )["Body"]
        return struct.unpack("I", last_four_bytes.read(4))[0]

    def _get_bytes_from_tag(self) -> int:
        tags = S3_CLIENT.get_object_tagging(Bucket=self._bucket, Key=self._key)
        tag_value = [
            tag["Value"]
            for tag in tags["TagSet"]
            if tag["Key"] == S3_TAG_UNCOMPRESSED_SIZE
        ][0]
        return int(tag_value)

    def get_uncompressed_size(self) -> int:
        if self._key.endswith(".gz"):
            return self._get_bytes_from_tag()
        else:
            return self._size  # type: ignore

    def set_uncompressed_size(self, size: int):
        S3_CLIENT.put_object_tagging(
            Bucket=self._bucket,
            Key=self._key,
            Tagging={
                "TagSet": [{"Key": S3_TAG_UNCOMPRESSED_SIZE, "Value": f"{size}"}]
            },
        )

    def start_multipart_upload(self) -> str:
        mpu = S3_CLIENT.create_multipart_upload(Bucket=self._bucket, Key=self._key)
        upload_id = mpu["UploadId"]
        return upload_id

    def complete_multipart_upload(self, upload_id: str, parts: list):
        S3_CLIENT.complete_multipart_upload(
            Bucket=self._bucket,
            Key=self._key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )

    def copy_existing_object(self, upload_id: str) -> dict:
        part = S3_CLIENT.upload_part_copy(
            Bucket=self._bucket,
            Key=self._key,
            CopySource={"Bucket": self._bucket, "Key": self._key},
            PartNumber=1,
            UploadId=upload_id,
        )
        return {"PartNumber": 1, "ETag": part["CopyPartResult"]["ETag"]}  # type: ignore

    def upload_part(self, upload_id: str, part_id: int, part_bytes: BytesIO) -> dict:
        part_bytes.seek(0)
        # mpu.upload_part_from_file(part_bytes, partCount[0])
        part = S3_CLIENT.upload_part(
            Bucket=self._bucket,
            Key=self._key,
            PartNumber=part_id,
            UploadId=upload_id,
            Body=part_bytes,
        )
        part_bytes.seek(0)
        part_bytes.truncate()
        return {"PartNumber": part_id, "ETag": part["ETag"]}  # type: ignore

    def __str__(self):
        return f"s3://{self._bucket}/{self._key}"


class S3DiffUploader:
    """
    S3DiffUploader makes use of multi-part uploads to only upload the changed parts
    of a file to Amazon S3. It can compress files on the fly to GZIP, making use of
    the fact that you can simply concatenate GZIP files.

    The original, uncompressed size of the file is required to know where to begin
    uploading subsequent parts. In order to make this utility stateless, that data
    is stored in a tag on the S3 Object itself.
    """
    def __init__(self, src: Path, dest: str, compress: bool = True):
        self._src = src
        self._s3_target = S3Object(dest)
        self._compress = compress
        pass

    def upload(self):
        # Check if the file already exists
        # If the target is < 5MB, we re-upload the whole thing.
        # If the target is already > 5MB, we upload our diff.
        # Initialize our multipart upload, a variable to keep track of parts, and where we're starting from
        start_time = time.time()
        starting_byte = 0
        parts = []
        mpu_id = self._s3_target.start_multipart_upload()
        stream = BytesIO()
        compressor = gzip.GzipFile(fileobj=stream, mode="w")
        print(f"Uploading file to {self._s3_target}")

        # If the target is >5mb, we copy the existing target and start from where we left off
        if self._s3_target.exists() and self._s3_target.size() > MULTIPART_TRIGGER_SIZE:
            starting_byte = self._s3_target.get_uncompressed_size()
            parts.append(self._s3_target.copy_existing_object(mpu_id))
            print(f"File exists and is larger than 5mb, starting from {starting_byte}")

        # Now open the source file and begin uploading parts of it
        uncompressed_file_size = os.stat(self._src).st_size
        with open(self._src, "rb") as inputFile:
            inputFile.seek(starting_byte)
            while True:  # until EOF
                chunk = inputFile.read(8192)
                if not chunk:  # EOF?
                    # if starting_byte > 0:
                    #     print(f"Forcing size to {compressor.size+starting_byte}")
                    #     compressor.size += starting_byte
                    compressor.close()
                    part = self._s3_target.upload_part(mpu_id, len(parts) + 1, stream)
                    parts.append(part)
                    self._s3_target.complete_multipart_upload(mpu_id, parts)
                    self._s3_target.set_uncompressed_size(uncompressed_file_size)
                    break
                compressor.write(chunk)
                if (
                    stream.tell() > MULTIPART_TRIGGER_SIZE
                ):  # min size for multipart upload is 5242880
                    part = self._s3_target.upload_part(mpu_id, len(parts) + 1, stream)
                    parts.append(part)
        
        end_time = time.time()
        print("Completed in: %0.3f seconds" % (end_time - start_time))
        print(f"Uploaded {uncompressed_file_size-starting_byte} of {uncompressed_file_size} bytes, size on S3 is {self._s3_target.size(True)} bytes")


if __name__ == "__main__":
    path = Path(sys.argv[1])
    s3_path = sys.argv[2]
    differ = S3DiffUploader(path, s3_path).upload()
