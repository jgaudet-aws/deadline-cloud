# Chunked Upload Integration Tests

## Overview

Integration tests for the V2 manifest chunked upload functionality. These tests validate end-to-end upload of large files split into 256MB chunks to S3 Content-Addressable Storage (CAS).

## Test File

`test_chunked_upload_integration.py`

## Prerequisites

### 1. AWS Resources
You need access to AWS resources for integration testing:
- AWS account with S3 access
- Deadline farm and queue configured
- Job attachment settings enabled on the queue

### 2. AWS Credentials
Set up your AWS credentials using one of these methods:

```bash
# Option 1: AWS Profile
export AWS_PROFILE=deadline-test

# Option 2: Environment variables
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_REGION=us-west-2
```

### 3. Test Fixtures
The tests use `deadline_test_fixtures` which automatically provisions:
- S3 bucket for job attachments
- Deadline farm
- Deadline queue with job attachment settings

## Running the Tests

### Activate Virtual Environment
```bash
source "/Users/gaudetj/Library/Application Support/hatch/env/virtual/deadline/jozXYpWm/deadline/bin/activate"
```

### Run All Chunked Upload Integration Tests
```bash
cd context/deadline-cloud
pytest test/integ/deadline_job_attachments/test_chunked_upload_integration.py -v
```

### Run Specific Test
```bash
pytest test/integ/deadline_job_attachments/test_chunked_upload_integration.py::TestChunkedUploadIntegration::test_upload_large_file_multiple_chunks -v
```

### Run with Detailed Output
```bash
pytest test/integ/deadline_job_attachments/test_chunked_upload_integration.py -v -s
```

### Run with Coverage
```bash
pytest test/integ/deadline_job_attachments/test_chunked_upload_integration.py \
  --cov=src/deadline/job_attachments/upload_chunked \
  --cov-report=html \
  -v
```

## Test 1.2: Upload Large File (Multiple Chunks)

### What It Tests
- Creates a 600MB test file
- Uploads using chunked upload (creates 3 chunks: 256MB, 256MB, 88MB)
- Verifies all chunks exist in S3 with correct sizes
- Validates manifest uses `chunkhashes` array
- Reconstructs file from chunks and verifies integrity

### Expected Duration
- File creation: ~5-10 seconds
- Upload: ~30-60 seconds (depends on network speed)
- Verification: ~30-60 seconds (downloading chunks)
- Total: ~1-2 minutes

### Expected Output
```
Creating 600MB test file at /tmp/pytest-xxx/chunked_upload_test/large_test_file.bin
Uploading file with chunked upload (chunk size: 256MB)
Verified 3 chunks created
Verifying chunk 1: bucket-name/Data/abc123...xxh128
  ✓ Chunk 1: size=268435456, offset=0, hash=abc123...
Verifying chunk 2: bucket-name/Data/def456...xxh128
  ✓ Chunk 2: size=268435456, offset=268435456, hash=def456...
Verifying chunk 3: bucket-name/Data/ghi789...xxh128
  ✓ Chunk 3: size=92274688, offset=536870912, hash=ghi789...
Verified manifest entry:
  name: large_test_file.bin
  size: 629145600
  chunkhashes: 3 hashes
Reconstructing file from chunks to verify integrity...
  Downloading chunk 1 from S3: bucket-name/Data/abc123...xxh128
  Downloading chunk 2 from S3: bucket-name/Data/def456...xxh128
  Downloading chunk 3 from S3: bucket-name/Data/ghi789...xxh128
✓ File successfully reconstructed from chunks with matching hash
✓ Byte-by-byte verification passed
Cleaning up test files...

================================================================================
TEST PASSED: Large file chunked upload integration test
================================================================================
File size: 600MB
Chunks created: 3
Chunks uploaded: True
All chunks verified in S3: ✓
Manifest format correct: ✓
File reconstruction successful: ✓
================================================================================
```

## Test Validations

The test performs the following validations:

### 1. Chunk Creation
- ✅ Correct number of chunks (3 for 600MB file)
- ✅ Correct chunk sizes (256MB, 256MB, 88MB)
- ✅ Correct chunk offsets (0, 256MB, 512MB)

### 2. S3 Upload
- ✅ All chunks uploaded to S3
- ✅ S3 objects have correct sizes
- ✅ S3 keys follow CAS format: `{cas_prefix}/{hash}.xxh128`

### 3. Manifest Format
- ✅ Uses `chunkhashes` array (not `hash` field)
- ✅ Contains 3 chunk hashes
- ✅ Includes correct file metadata (name, size, mtime)
- ✅ Excludes `runnable` field when false

### 4. Data Integrity
- ✅ Each chunk hash matches downloaded content
- ✅ Reconstructed file size matches original
- ✅ Reconstructed file hash matches original
- ✅ Byte-by-byte comparison passes

## Troubleshooting

### Test Fails with "Farm ID not retrieved"
**Cause**: Test fixtures failed to provision AWS resources

**Solution**: 
- Check AWS credentials are valid
- Verify IAM permissions for Deadline and S3
- Check AWS region is supported

### Test Fails with "Access Denied"
**Cause**: Insufficient S3 permissions

**Solution**: Ensure IAM role/user has:
- `s3:PutObject`
- `s3:GetObject`
- `s3:HeadObject`
- `s3:ListBucket`

### Test Times Out
**Cause**: Slow network or large file size

**Solution**:
- Increase pytest timeout: `pytest --timeout=300`
- Check network connectivity to S3
- Consider reducing test file size for local testing

### Chunks Not Found in S3
**Cause**: Upload failed silently or wrong bucket/prefix

**Solution**:
- Check test output for upload errors
- Verify bucket name and prefix in test fixtures
- Check S3 bucket exists and is accessible

## Cleanup

The test automatically cleans up:
- ✅ Local test files (original and reconstructed)
- ⚠️ S3 objects remain for debugging (manual cleanup needed)

To clean up S3 objects after testing:
```bash
aws s3 rm s3://your-test-bucket/Data/ --recursive
```

## Adding More Tests

To add additional integration tests, follow this pattern:

```python
@pytest.mark.integ
def test_your_new_test(
    self,
    deploy_job_attachment_resources: JobAttachmentManager,
    tmp_path_factory: TempPathFactory,
) -> None:
    """
    Test X.Y: Your Test Name
    
    Objective: What you're testing
    
    Steps:
    1. Setup
    2. Execute
    3. Verify
    
    Expected Results:
    - What should happen
    """
    # Your test implementation
    pass
```

## Related Documentation

- **Design Doc**: `context/JA/upload_design.md`
- **Integration Test Plan**: `INTEGRATION_TEST_PLAN.md`
- **Unit Tests**: `test/unit/deadline_job_attachments/test_upload_chunked.py`
- **Implementation**: `src/deadline/job_attachments/upload_chunked.py`

## Contact

For questions or issues with these tests, refer to the integration test plan or contact the Job Attachments team.
