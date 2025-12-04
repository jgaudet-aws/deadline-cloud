# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Module for uploading input files to S3 Content-Addressable Storage (CAS) with chunking support.
Implements the v2 manifest format's chunking feature for large files.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional
import xxhash
import boto3
from botocore.exceptions import ClientError

from .asset_manifests import HashAlgorithm


# ============== Data Structures ==============

@dataclass
class InputFile:
    """Represents an input file to be uploaded."""
    
    local_path: Path
    size: int
    hash: str  # xxh128 of whole file
    mtime: int  # unix timestamp
    runnable: bool = False  # POSIX execute bit


@dataclass
class S3Options:
    """S3 bucket and prefix configuration for uploads."""
    
    bucket: str
    cas_prefix: str  # e.g., "Data"
    manifest_prefix: str  # e.g., "Manifests/farm-xxx/queue-yyy"


@dataclass
class ChunkInfo:
    """Information about a single chunk of a file."""
    
    s3_key: str  # "{cas_prefix}/{chunk_hash}.xxh128"
    size: int
    hash: str  # xxh128 of this chunk
    offset: int  # 0 for small files (single chunk)


@dataclass
class UploadResult:
    """Result of uploading a file, including chunk information."""
    
    input_file: InputFile
    chunks: list[ChunkInfo] = field(default_factory=list)
    uploaded: bool = False  # Any chunks actually uploaded?


# ============== Core Functions ==============

DEFAULT_CHUNK_SIZE = 256 * 1024 * 1024  # 256MB


def should_upload(s3_client: Any, bucket: str, s3_key: str, expected_size: int) -> bool:
    """
    Check if we need to upload - either doesn't exist or size mismatch.
    
    Size mismatch indicates corruption or incomplete upload - re-upload in this case.
    """
    try:
        response = s3_client.head_object(Bucket=bucket, Key=s3_key)
        # Size mismatch = re-upload (handles interrupted uploads)
        return response['ContentLength'] != expected_size
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return True  # Doesn't exist = upload
        raise


def read_chunk(file_path: Path, offset: int, chunk_size: int) -> bytes:
    """Read a chunk of data from file at given offset."""
    with open(file_path, 'rb') as f:
        f.seek(offset)
        return f.read(chunk_size)


def hash_chunk(data: bytes) -> str:
    """Compute xxh128 hash of data."""
    return xxhash.xxh128(data).hexdigest()


def upload_bytes(s3_client: Any, bucket: str, s3_key: str, data: bytes) -> None:
    """Upload bytes to S3."""
    s3_client.put_object(Bucket=bucket, Key=s3_key, Body=data)


def process_file(
    s3_client: Any,
    file: InputFile,
    s3_opts: S3Options,
    chunk_size: int = DEFAULT_CHUNK_SIZE
) -> UploadResult:
    """
    Process a single file - upload chunks as needed.
    
    All files treated uniformly - small files are single-chunk files with offset: 0.
    """
    chunks: list[ChunkInfo] = []
    any_uploaded = False
    
    offset = 0
    while offset < file.size:
        # Read chunk
        chunk_data = read_chunk(file.local_path, offset, chunk_size)
        chunk_hash = hash_chunk(chunk_data)
        chunk_size_actual = len(chunk_data)
        s3_key = f"{s3_opts.cas_prefix}/{chunk_hash}.xxh128"
        
        # Upload if needed
        if should_upload(s3_client, s3_opts.bucket, s3_key, chunk_size_actual):
            upload_bytes(s3_client, s3_opts.bucket, s3_key, chunk_data)
            any_uploaded = True
        
        chunks.append(ChunkInfo(
            s3_key=s3_key,
            size=chunk_size_actual,
            hash=chunk_hash,
            offset=offset
        ))
        
        offset += chunk_size
    
    return UploadResult(input_file=file, chunks=chunks, uploaded=any_uploaded)


def upload(
    files: list[InputFile],
    s3_opts: S3Options,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    session: Optional[boto3.Session] = None
) -> list[UploadResult]:
    """
    Upload input files to S3 CAS with chunking support.
    
    Args:
        files: List of input files to upload
        s3_opts: S3 bucket and prefix configuration
        chunk_size: Chunk size in bytes (default 256MB)
        session: Optional boto3 session
    
    Returns:
        List of UploadResult with chunk mappings
    """
    session = session or boto3.Session()
    s3_client = session.client('s3')
    
    results: list[UploadResult] = []
    for file in files:
        result = process_file(s3_client, file, s3_opts, chunk_size)
        results.append(result)
    
    return results


# ============== Helper: Generate Manifest Entry ==============

def to_manifest_entry(result: UploadResult, dir_index: Optional[int] = None) -> dict[str, Any]:
    """
    Convert UploadResult to v2 manifest file entry.
    
    Small files (1 chunk) use 'hash' field.
    Large files (>1 chunk) use 'chunkhashes' array.
    """
    file = result.input_file
    filename = file.local_path.name
    name = f"${dir_index}/{filename}" if dir_index is not None else filename
    
    entry = {
        "name": name,
        "size": file.size,
        "mtime": file.mtime
    }
    
    if len(result.chunks) == 1:
        # Small file - single hash
        entry["hash"] = result.chunks[0].hash
    else:
        # Large file - chunk hashes
        entry["chunkhashes"] = [chunk.hash for chunk in result.chunks]
    
    # Only include runnable if true (per v2 spec - canonical format)
    if file.runnable:
        entry["runnable"] = True
    
    return entry
