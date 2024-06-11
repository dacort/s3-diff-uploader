# S3 Diff Uploader

A simple Python tool to demonstrate differential uploading of large and/or compressed files to S3.

## Usage

1. Clone and change into the repo directory

```bash
git clone https://github.com/dacort/s3-diff-uploader
cd s3-diff-uploader
```

2. (Optionally) create a venv and install the requirements

```bash
python3 -m venv .venv && source .venv/bin/activate
python3 -m pip install -r requirements.txt
```

3. Run against a file

```bash
python3 differ.py <local-file.csv> <s3://bucket/name-of-target-file.csv.gz>
```

> [!NOTE]
> Files are currently auto-compressed, but you still have to provide the `.gz` extension.

4. Add data to the file, and run the same command.

Once the compressed file gets above 5mb, you'll start to see only differential uploads.

## How it works

There are two main components:
- A combination of [multi-part uploads](https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html) and  [`UploadPartCopy`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPartCopy.html) can be used to seed subsequent uploads with existing data on S3.
- For the compression piece, GZIP files can simply be concatenated [per the RFC](https://datatracker.ietf.org/doc/html/rfc1952#page-5).

In order to do utilize compression, you need to copy the existing object and then begin uploading the remaining sections from where you left off. In order to do this in a stateless fashion, this code utilizes a tag on the S3 object itself to keep track of the total uncompressed size.

## TODO

- Add `--watch` flag to detect and upload file changes