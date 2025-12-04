# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Integration tests for V2 Manifest Chunked Upload functionality."""

import os
from pathlib import Path
from typing import Any

import boto3
import pytest
from deadline_test_fixtures.job_attachment_manager import JobAttachmentManager
from pytest import TempPathFactory

from deadline.job_attachments.upload_chunked import (
    InputFile,
    S3Options,
    upload,
    to_manifest_entry,
    DEFAULT_CHUNK_SIZE,
)
from deadline.job_attachments.asset_manifests import HashAlgorithm, hash_file


def create_test_file(file_path: Path, size_bytes: int, pattern: bytes = b"X") -> Path:
    """
    Create a test file with specified size.
    
    Args:
        file_path: Path where file should be created
        size_bytes: Size of file in bytes
        pattern: Byte pattern to fill file with (default: b"X")
    
    Returns:
        Path to created file
    """
    file_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Write in chunks to handle large files efficiently
    chunk_size = 1024 * 1024  # 1MB chunks
    with open(file_path, "wb") as f:
        remaining = size_bytes
        while remaining > 0:
            write_size = min(chunk_size, remaining)
            f.write(pattern * write_size)
            remaining -= write_size
    
    return file_path


def read_s3_object(bucket_name: str, key: str, session: boto3.Session) -> bytes:
    """Read an S3 object and return its content."""
    s3_client = session.client("s3")
    response = s3_client.get_object(Bucket=bucket_name, Key=key)
    return response["Body"].read()


def verify_s3_object_exists(bucket_name: str, key: str, session: boto3.Session) -> dict[str, Any]:
    """Verify S3 object exists and return its metadata."""
    s3_client = session.client("s3")
    response = s3_client.head_object(Bucket=bucket_name, Key=key)
    return response


class TestChunkedUploadIntegration:
    """Integration tests for chunked upload with real S3."""

    @pytest.mark.integ
    def test_upload_large_file_multiple_chunks(
        self,
        deploy_job_attachment_resources: JobAttachmentManager,
        tmp_path_factory: TempPathFactory,
    ) -> None:
        """
        Test 1.2: Upload Large File (Multiple Chunks)
        
        Objective: Verify large files are split into multiple chunks
        
        Steps:
        1. Create a 600MB test file (will create 3 chunks: 256MB, 256MB, 88MB)
        2. Upload using upload_chunked.upload()
        3. Verify 3 separate S3 objects exist in CAS
        4. Verify each chunk has correct size
        5. Verify manifest entry uses chunkhashes array with 3 hashes
        6. Reconstruct file from chunks and verify integrity
        
        Expected Results:
        - 3 S3 objects created with correct sizes
        - Manifest has chunkhashes array with 3 entries
        - Chunks can be reassembled to original file
        """
        # SETUP
        if deploy_job_attachment_resources.farm_id is None:
            raise TypeError("Farm ID was not properly retrieved when initializing resources.")
        if deploy_job_attachment_resources.queue is None:
            raise TypeError("Queue was not properly created when initializing resources.")

        bucket_name = deploy_job_attachment_resources.bucket_name
        bucket_root_prefix = deploy_job_attachment_resources.bucket_root_prefix
        session = boto3.Session()
        s3_client = session.client("s3")

        # Create test directory
        test_dir = tmp_path_factory.mktemp("chunked_upload_test")
        
        # Step 1: Create a 600MB test file
        # This will create 3 chunks: 256MB, 256MB, 88MB
        file_size = 600 * 1024 * 1024  # 600MB
        test_file_path = test_dir / "large_test_file.bin"
        
        print(f"Creating {file_size / (1024*1024):.0f}MB test file at {test_file_path}")
        create_test_file(test_file_path, file_size, pattern=b"A")
        
        # Compute hash of the entire file for verification
        file_hash = hash_file(str(test_file_path), HashAlgorithm.XXH128)
        file_mtime = int(test_file_path.stat().st_mtime)
        
        input_file = InputFile(
            local_path=test_file_path,
            size=file_size,
            hash=file_hash,
            mtime=file_mtime,
            runnable=False,
        )
        
        s3_options = S3Options(
            bucket=bucket_name,
            cas_prefix=f"{bucket_root_prefix}/Data",
            manifest_prefix=f"{bucket_root_prefix}/Manifests/test",
        )
        
        # Step 2: Upload using chunked upload
        print(f"Uploading file with chunked upload (chunk size: {DEFAULT_CHUNK_SIZE / (1024*1024):.0f}MB)")
        results = upload([input_file], s3_options, session=session)
        
        assert len(results) == 1, "Should have one upload result"
        result = results[0]
        
        # Step 3: Verify 3 separate S3 objects exist in CAS
        expected_num_chunks = 3
        assert len(result.chunks) == expected_num_chunks, (
            f"Expected {expected_num_chunks} chunks, got {len(result.chunks)}"
        )
        
        print(f"Verified {len(result.chunks)} chunks created")
        
        # Step 4: Verify each chunk has correct size
        expected_chunk_sizes = [
            256 * 1024 * 1024,  # First chunk: 256MB
            256 * 1024 * 1024,  # Second chunk: 256MB
            88 * 1024 * 1024,   # Third chunk: 88MB (600 - 512)
        ]
        
        for i, (chunk, expected_size) in enumerate(zip(result.chunks, expected_chunk_sizes)):
            print(f"Verifying chunk {i+1}: {chunk.s3_key}")
            
            # Verify chunk exists in S3
            metadata = verify_s3_object_exists(bucket_name, chunk.s3_key, session)
            actual_size = metadata["ContentLength"]
            
            assert chunk.size == expected_size, (
                f"Chunk {i+1} size mismatch: expected {expected_size}, got {chunk.size}"
            )
            assert actual_size == expected_size, (
                f"Chunk {i+1} S3 object size mismatch: expected {expected_size}, got {actual_size}"
            )
            
            # Verify S3 key format
            assert chunk.s3_key.startswith(s3_options.cas_prefix), (
                f"Chunk S3 key should start with CAS prefix: {chunk.s3_key}"
            )
            assert chunk.s3_key.endswith(".xxh128"), (
                f"Chunk S3 key should end with .xxh128: {chunk.s3_key}"
            )
            
            # Verify offset
            expected_offset = i * DEFAULT_CHUNK_SIZE
            assert chunk.offset == expected_offset, (
                f"Chunk {i+1} offset mismatch: expected {expected_offset}, got {chunk.offset}"
            )
            
            print(f"  ✓ Chunk {i+1}: size={chunk.size}, offset={chunk.offset}, hash={chunk.hash[:16]}...")
        
        # Step 5: Verify manifest entry uses chunkhashes array with 3 hashes
        manifest_entry = to_manifest_entry(result)
        
        assert "chunkhashes" in manifest_entry, (
            "Manifest entry should have 'chunkhashes' field for multi-chunk file"
        )
        assert "hash" not in manifest_entry, (
            "Manifest entry should NOT have 'hash' field for multi-chunk file"
        )
        assert len(manifest_entry["chunkhashes"]) == expected_num_chunks, (
            f"Manifest should have {expected_num_chunks} chunk hashes"
        )
        
        # Verify manifest structure
        assert manifest_entry["name"] == test_file_path.name
        assert manifest_entry["size"] == file_size
        assert manifest_entry["mtime"] == file_mtime
        assert "runnable" not in manifest_entry  # Should not be present when False
        
        print(f"Verified manifest entry:")
        print(f"  name: {manifest_entry['name']}")
        print(f"  size: {manifest_entry['size']}")
        print(f"  chunkhashes: {len(manifest_entry['chunkhashes'])} hashes")
        
        # Step 6: Reconstruct file from chunks and verify integrity
        print("Reconstructing file from chunks to verify integrity...")
        reconstructed_file_path = test_dir / "reconstructed_file.bin"
        
        with open(reconstructed_file_path, "wb") as reconstructed_file:
            for i, chunk in enumerate(result.chunks):
                print(f"  Downloading chunk {i+1} from S3: {chunk.s3_key}")
                chunk_data = read_s3_object(bucket_name, chunk.s3_key, session)
                
                # Verify chunk size matches
                assert len(chunk_data) == chunk.size, (
                    f"Downloaded chunk {i+1} size mismatch: expected {chunk.size}, got {len(chunk_data)}"
                )
                
                # Verify chunk hash matches
                from deadline.job_attachments.asset_manifests import hash_data
                chunk_hash = hash_data(chunk_data, HashAlgorithm.XXH128)
                assert chunk_hash == chunk.hash, (
                    f"Chunk {i+1} hash mismatch: expected {chunk.hash}, got {chunk_hash}"
                )
                
                reconstructed_file.write(chunk_data)
        
        # Verify reconstructed file matches original
        reconstructed_size = reconstructed_file_path.stat().st_size
        assert reconstructed_size == file_size, (
            f"Reconstructed file size mismatch: expected {file_size}, got {reconstructed_size}"
        )
        
        reconstructed_hash = hash_file(str(reconstructed_file_path), HashAlgorithm.XXH128)
        assert reconstructed_hash == file_hash, (
            f"Reconstructed file hash mismatch: expected {file_hash}, got {reconstructed_hash}"
        )
        
        print("✓ File successfully reconstructed from chunks with matching hash")
        
        # Verify byte-by-byte comparison
        with open(test_file_path, "rb") as original, open(reconstructed_file_path, "rb") as reconstructed:
            chunk_size = 1024 * 1024  # Compare in 1MB chunks
            while True:
                original_chunk = original.read(chunk_size)
                reconstructed_chunk = reconstructed.read(chunk_size)
                
                if not original_chunk and not reconstructed_chunk:
                    break
                
                assert original_chunk == reconstructed_chunk, (
                    "Reconstructed file content does not match original"
                )
        
        print("✓ Byte-by-byte verification passed")
        
        # CLEANUP
        print("Cleaning up test files...")
        test_file_path.unlink()
        reconstructed_file_path.unlink()
        
        print("\n" + "="*80)
        print("TEST PASSED: Large file chunked upload integration test")
        print("="*80)
        print(f"File size: {file_size / (1024*1024):.0f}MB")
        print(f"Chunks created: {len(result.chunks)}")
        print(f"Chunks uploaded: {result.uploaded}")
        print(f"All chunks verified in S3: ✓")
        print(f"Manifest format correct: ✓")
        print(f"File reconstruction successful: ✓")
        print("="*80)
