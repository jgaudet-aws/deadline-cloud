# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for chunked upload functionality (v2 manifest format).
"""

import os
from pathlib import Path
from unittest.mock import MagicMock, patch, call
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

from deadline.job_attachments.upload_chunked import (
    ChunkInfo,
    InputFile,
    S3Options,
    UploadResult,
    DEFAULT_CHUNK_SIZE,
    should_upload,
    read_chunk,
    hash_chunk,
    upload_bytes,
    process_file,
    upload,
    to_manifest_entry,
)


class TestChunkedUpload:
    """Tests for chunked upload functionality."""

    @pytest.fixture
    def s3_options(self) -> S3Options:
        """Fixture providing S3 options."""
        return S3Options(
            bucket="test-bucket",
            cas_prefix="Data",
            manifest_prefix="Manifests/farm-123/queue-456",
        )

    @pytest.fixture
    def small_file(self, tmp_path: Path) -> InputFile:
        """Fixture creating a small test file (< 256MB)."""
        file_path = tmp_path / "small_file.txt"
        content = b"Hello, World!" * 1000  # ~13KB
        file_path.write_bytes(content)
        
        return InputFile(
            local_path=file_path,
            size=len(content),
            hash="abc123def456",
            mtime=1712972834,
            runnable=False,
        )

    @pytest.fixture
    def large_file(self, tmp_path: Path) -> InputFile:
        """Fixture creating a large test file (> 256MB, simulated)."""
        file_path = tmp_path / "large_file.bin"
        # Create a file with 300MB of data (simulated with smaller chunks for testing)
        chunk_size = 1024 * 1024  # 1MB chunks for testing
        num_chunks = 300
        
        with open(file_path, "wb") as f:
            for i in range(num_chunks):
                f.write(bytes([i % 256]) * chunk_size)
        
        file_size = file_path.stat().st_size
        
        return InputFile(
            local_path=file_path,
            size=file_size,
            hash="xyz789abc012",
            mtime=1712972900,
            runnable=False,
        )

    def test_should_upload_file_not_exists(self):
        """Test should_upload when file doesn't exist in S3."""
        mock_s3 = MagicMock()
        mock_s3.head_object.side_effect = ClientError(
            {"Error": {"Code": "404"}}, "head_object"
        )
        
        result = should_upload(mock_s3, "bucket", "key", 1024)
        
        assert result is True
        mock_s3.head_object.assert_called_once_with(Bucket="bucket", Key="key")

    def test_should_upload_size_mismatch(self):
        """Test should_upload when file exists but size doesn't match."""
        mock_s3 = MagicMock()
        mock_s3.head_object.return_value = {"ContentLength": 1024}
        
        result = should_upload(mock_s3, "bucket", "key", 2048)
        
        assert result is True

    def test_should_upload_size_matches(self):
        """Test should_upload when file exists and size matches."""
        mock_s3 = MagicMock()
        mock_s3.head_object.return_value = {"ContentLength": 1024}
        
        result = should_upload(mock_s3, "bucket", "key", 1024)
        
        assert result is False

    def test_read_chunk(self, tmp_path: Path):
        """Test reading a chunk from a file."""
        file_path = tmp_path / "test_file.txt"
        content = b"0123456789" * 100  # 1000 bytes
        file_path.write_bytes(content)
        
        # Read first 100 bytes
        chunk = read_chunk(file_path, 0, 100)
        assert len(chunk) == 100
        assert chunk == content[:100]
        
        # Read middle 100 bytes
        chunk = read_chunk(file_path, 500, 100)
        assert len(chunk) == 100
        assert chunk == content[500:600]
        
        # Read last chunk (may be smaller)
        chunk = read_chunk(file_path, 900, 200)
        assert len(chunk) == 100
        assert chunk == content[900:]

    def test_hash_chunk(self):
        """Test hashing a chunk of data."""
        data = b"Hello, World!"
        hash_result = hash_chunk(data)
        
        # Verify it's a valid hex string
        assert isinstance(hash_result, str)
        assert len(hash_result) == 32  # xxh128 produces 32 hex characters
        assert all(c in "0123456789abcdef" for c in hash_result)
        
        # Verify consistency
        assert hash_chunk(data) == hash_result

    def test_upload_bytes(self):
        """Test uploading bytes to S3."""
        mock_s3 = MagicMock()
        data = b"test data"
        
        upload_bytes(mock_s3, "bucket", "key", data)
        
        mock_s3.put_object.assert_called_once_with(
            Bucket="bucket", Key="key", Body=data
        )

    def test_process_file_small_single_chunk(self, small_file: InputFile, s3_options: S3Options):
        """Test processing a small file that fits in a single chunk."""
        mock_s3 = MagicMock()
        mock_s3.head_object.side_effect = ClientError(
            {"Error": {"Code": "404"}}, "head_object"
        )
        
        result = process_file(mock_s3, small_file, s3_options)
        
        # Should have exactly 1 chunk
        assert len(result.chunks) == 1
        assert result.chunks[0].offset == 0
        assert result.chunks[0].size == small_file.size
        assert result.uploaded is True
        assert result.input_file == small_file
        
        # Verify S3 key format
        assert result.chunks[0].s3_key.startswith("Data/")
        assert result.chunks[0].s3_key.endswith(".xxh128")

    def test_process_file_already_uploaded(self, small_file: InputFile, s3_options: S3Options):
        """Test processing a file that's already uploaded."""
        mock_s3 = MagicMock()
        # File exists and size matches
        mock_s3.head_object.return_value = {"ContentLength": small_file.size}
        
        result = process_file(mock_s3, small_file, s3_options)
        
        assert len(result.chunks) == 1
        assert result.uploaded is False  # Not uploaded because it already exists
        mock_s3.put_object.assert_not_called()

    def test_process_file_large_multiple_chunks(self, tmp_path: Path, s3_options: S3Options):
        """Test processing a large file that requires multiple chunks."""
        # Create a file that will be split into 3 chunks
        file_path = tmp_path / "large_file.bin"
        chunk_size = 100  # Small chunk size for testing
        content = b"X" * 250  # Will create 3 chunks: 100, 100, 50
        file_path.write_bytes(content)
        
        large_file = InputFile(
            local_path=file_path,
            size=len(content),
            hash="test_hash",
            mtime=1234567890,
        )
        
        mock_s3 = MagicMock()
        mock_s3.head_object.side_effect = ClientError(
            {"Error": {"Code": "404"}}, "head_object"
        )
        
        result = process_file(mock_s3, large_file, s3_options, chunk_size=chunk_size)
        
        # Should have 3 chunks
        assert len(result.chunks) == 3
        assert result.chunks[0].offset == 0
        assert result.chunks[0].size == 100
        assert result.chunks[1].offset == 100
        assert result.chunks[1].size == 100
        assert result.chunks[2].offset == 200
        assert result.chunks[2].size == 50
        assert result.uploaded is True
        
        # Verify all chunks were uploaded
        assert mock_s3.put_object.call_count == 3

    @mock_aws
    def test_upload_single_file(self, small_file: InputFile, s3_options: S3Options, s3, create_s3_bucket):
        """Test uploading a single file."""
        create_s3_bucket(s3_options.bucket)
        
        results = upload([small_file], s3_options)
        
        assert len(results) == 1
        assert results[0].input_file == small_file
        assert len(results[0].chunks) == 1
        assert results[0].uploaded is True

    @mock_aws
    def test_upload_multiple_files(self, tmp_path: Path, s3_options: S3Options, s3, create_s3_bucket):
        """Test uploading multiple files."""
        create_s3_bucket(s3_options.bucket)
        
        # Create multiple test files
        files = []
        for i in range(3):
            file_path = tmp_path / f"file_{i}.txt"
            content = f"Content {i}".encode() * 100
            file_path.write_bytes(content)
            
            files.append(
                InputFile(
                    local_path=file_path,
                    size=len(content),
                    hash=f"hash_{i}",
                    mtime=1234567890 + i,
                )
            )
        
        results = upload(files, s3_options)
        
        assert len(results) == 3
        for i, result in enumerate(results):
            assert result.input_file == files[i]
            assert len(result.chunks) >= 1

    def test_upload_with_custom_chunk_size(self, tmp_path: Path, s3_options: S3Options):
        """Test uploading with a custom chunk size."""
        file_path = tmp_path / "custom_chunk.bin"
        content = b"Y" * 500
        file_path.write_bytes(content)
        
        file = InputFile(
            local_path=file_path,
            size=len(content),
            hash="custom_hash",
            mtime=1234567890,
        )
        
        mock_s3 = MagicMock()
        mock_s3.head_object.side_effect = ClientError(
            {"Error": {"Code": "404"}}, "head_object"
        )
        
        with patch("deadline.job_attachments.upload_chunked.boto3.Session") as mock_session:
            mock_session.return_value.client.return_value = mock_s3
            
            results = upload([file], s3_options, chunk_size=200)
        
        # Should create 3 chunks: 200, 200, 100
        assert len(results[0].chunks) == 3

    def test_to_manifest_entry_small_file(self, small_file: InputFile):
        """Test converting a small file (1 chunk) to manifest entry."""
        chunk = ChunkInfo(
            s3_key="Data/abc123.xxh128",
            size=small_file.size,
            hash="abc123",
            offset=0,
        )
        result = UploadResult(
            input_file=small_file,
            chunks=[chunk],
            uploaded=True,
        )
        
        entry = to_manifest_entry(result)
        
        assert entry["name"] == small_file.local_path.name
        assert entry["size"] == small_file.size
        assert entry["mtime"] == small_file.mtime
        assert entry["hash"] == "abc123"
        assert "chunkhashes" not in entry
        assert "runnable" not in entry  # False by default, not included

    def test_to_manifest_entry_large_file(self, tmp_path: Path):
        """Test converting a large file (multiple chunks) to manifest entry."""
        file_path = tmp_path / "large.bin"
        file_path.write_bytes(b"X" * 1000)
        
        file = InputFile(
            local_path=file_path,
            size=1000,
            hash="file_hash",
            mtime=1234567890,
        )
        
        chunks = [
            ChunkInfo(s3_key="Data/chunk1.xxh128", size=400, hash="chunk1", offset=0),
            ChunkInfo(s3_key="Data/chunk2.xxh128", size=400, hash="chunk2", offset=400),
            ChunkInfo(s3_key="Data/chunk3.xxh128", size=200, hash="chunk3", offset=800),
        ]
        
        result = UploadResult(input_file=file, chunks=chunks, uploaded=True)
        
        entry = to_manifest_entry(result)
        
        assert entry["name"] == file.local_path.name
        assert entry["size"] == 1000
        assert entry["mtime"] == 1234567890
        assert "hash" not in entry
        assert entry["chunkhashes"] == ["chunk1", "chunk2", "chunk3"]

    def test_to_manifest_entry_with_dir_index(self, small_file: InputFile):
        """Test manifest entry with directory index."""
        chunk = ChunkInfo(
            s3_key="Data/abc123.xxh128",
            size=small_file.size,
            hash="abc123",
            offset=0,
        )
        result = UploadResult(input_file=small_file, chunks=[chunk], uploaded=True)
        
        entry = to_manifest_entry(result, dir_index=1)
        
        assert entry["name"] == f"$1/{small_file.local_path.name}"

    def test_to_manifest_entry_runnable_file(self, tmp_path: Path):
        """Test manifest entry for a runnable file."""
        file_path = tmp_path / "script.sh"
        file_path.write_bytes(b"#!/bin/bash\necho hello")
        
        file = InputFile(
            local_path=file_path,
            size=file_path.stat().st_size,
            hash="script_hash",
            mtime=1234567890,
            runnable=True,
        )
        
        chunk = ChunkInfo(
            s3_key="Data/script.xxh128",
            size=file.size,
            hash="script_hash",
            offset=0,
        )
        result = UploadResult(input_file=file, chunks=[chunk], uploaded=True)
        
        entry = to_manifest_entry(result)
        
        assert entry["runnable"] is True

    def test_default_chunk_size(self):
        """Test that default chunk size is 256MB."""
        assert DEFAULT_CHUNK_SIZE == 256 * 1024 * 1024

    @mock_aws
    def test_deduplication_same_content(self, tmp_path: Path, s3_options: S3Options, s3, create_s3_bucket):
        """Test that files with same content are deduplicated."""
        create_s3_bucket(s3_options.bucket)
        
        # Create two files with identical content
        content = b"Identical content" * 1000
        
        file1_path = tmp_path / "file1.txt"
        file1_path.write_bytes(content)
        file1 = InputFile(
            local_path=file1_path,
            size=len(content),
            hash="hash1",
            mtime=1234567890,
        )
        
        file2_path = tmp_path / "file2.txt"
        file2_path.write_bytes(content)
        file2 = InputFile(
            local_path=file2_path,
            size=len(content),
            hash="hash2",
            mtime=1234567891,
        )
        
        # Upload first file
        results1 = upload([file1], s3_options)
        assert results1[0].uploaded is True
        
        # Upload second file - should be deduplicated
        results2 = upload([file2], s3_options)
        # Both files have same content, so same chunk hash
        # Second upload should skip because chunk already exists
        assert results2[0].uploaded is False

    def test_process_file_size_mismatch_reuploads(self, small_file: InputFile, s3_options: S3Options):
        """Test that files with size mismatch are re-uploaded."""
        mock_s3 = MagicMock()
        # File exists but with wrong size
        mock_s3.head_object.return_value = {"ContentLength": small_file.size + 100}
        
        result = process_file(mock_s3, small_file, s3_options)
        
        # Should re-upload due to size mismatch
        assert result.uploaded is True
        mock_s3.put_object.assert_called_once()
