---
name: Bug Report
about: Report a bug or unexpected behavior
title: '[BUG] '
labels: ['bug', 'needs-triage']
assignees: []
---

## Bug Description
<!-- A clear and concise description of what the bug is -->

## Expected Behavior
<!-- What you expected to happen -->

## Actual Behavior
<!-- What actually happened -->

## Reproduction Steps
<!-- Steps to reproduce the behavior -->
1. 
2. 
3. 
4. 

## Minimal Code Example
<!-- Provide a minimal code example that reproduces the issue -->
```java
// Your code here
```

## Environment Details

### System Information
- **OS**: [e.g., Ubuntu 22.04, Windows 11, macOS 14.1]
- **Java Version**: [e.g., OpenJDK 17.0.2, Oracle JDK 21.0.1]
- **Library Version**: [e.g., 1.0.0, main branch commit abc123]

### Module Information
<!-- Which modules are you using? -->
- [ ] tileverse-storage-core
- [ ] tileverse-storage-s3
- [ ] tileverse-storage-azure
- [ ] tileverse-storage-gcs
- [ ] tileverse-storage-all

### Configuration
<!-- Relevant configuration details -->
- **Cache Configuration**: 
- **Block Size Settings**: 
- **Authentication Method**: 
- **Other Settings**: 

## Error Details

### Stack Trace
<!-- Full stack trace if available -->
```
Paste stack trace here
```

### Log Output
<!-- Relevant log output with DEBUG level if possible -->
```
Paste logs here
```

### Error Message
<!-- The specific error message you received -->

## Data Source Information
<!-- If the issue is related to a specific data source -->
- **Source Type**: [File, HTTP, S3, Azure Blob, GCS]
- **Source Location**: [e.g., s3://bucket/key, https://example.com/data.bin]
- **File Size**: [e.g., 100MB, 5GB]
- **Access Pattern**: [e.g., sequential reads, random access, batch operations]

## Network Environment
<!-- If relevant to cloud storage or HTTP issues -->
- **Network Type**: [Corporate, Home, Cloud, Mobile]
- **Proxy Configuration**: [Yes/No, details if applicable]
- **Geographic Location**: [Region if relevant for cloud access]
- **Bandwidth**: [If known and relevant]

## Workaround
<!-- If you found a temporary workaround, describe it here -->

## Impact Assessment
- [ ] Blocks development/production
- [ ] Causes data corruption
- [ ] Performance degradation
- [ ] Intermittent failures
- [ ] Minor inconvenience

## Frequency
- [ ] Always reproducible
- [ ] Intermittent (occurs sometimes)
- [ ] Rare (occurred once or twice)
- [ ] Unknown

## Additional Context
<!-- Any other context about the problem -->
<!-- Screenshots, profiling data, related issues, etc. -->

## Checklist
<!-- Please check all that apply -->
- [ ] I have searched existing issues to ensure this is not a duplicate
- [ ] I have provided a minimal code example that reproduces the issue
- [ ] I have included all relevant environment details
- [ ] I have tried the latest version of the library
- [ ] I have checked the documentation for this functionality