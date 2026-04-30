workspace "Tileverse Storage - Dynamic Views" "Runtime scenarios for the Tileverse Storage library" {

    model {
        # People and external systems
        developer = person "Developer" "A developer using the Tileverse Storage library"
        application = person "Application" "An application that uses the library to read data"

        # External systems
        localFileSystem = softwareSystem "Local File System" "Local files on disk"
        httpServer = softwareSystem "HTTP Server" "Remote HTTP/HTTPS servers"
        awsS3 = softwareSystem "Amazon S3" "AWS S3 and S3-compatible storage"

        # Main system
        storageLibrary = softwareSystem "Tileverse Storage" "Java I/O abstraction over object storage (local files, HTTP, S3, Azure Blob, GCS)" {
            
            # Core module
            coreModule = container "Core Module" "Core interfaces and base implementations" "Java 17, Maven" {
                # Core components
                rangeReaderInterface = component "RangeReader Interface" "Main interface for reading byte ranges" "Java Interface"
                abstractRangeReader = component "AbstractRangeReader" "Base implementation with common functionality" "Java Abstract Class"
                fileRangeReader = component "FileRangeReader" "Reads ranges from local files" "Java Class"
                httpRangeReader = component "HttpRangeReader" "Reads ranges from HTTP servers" "Java Class"
                
                # Decorators
                cachingRangeReader = component "CachingRangeReader" "In-memory caching decorator" "Java Class"
                diskCachingRangeReader = component "DiskCachingRangeReader" "Disk-based caching decorator" "Java Class"
                
                # Authentication
                authenticationSystem = component "Authentication System" "HTTP authentication implementations" "Java Package"
            }
            
            # S3 module
            s3Module = container "S3 Module" "Amazon S3 and S3-compatible storage support" "Java 17, AWS SDK v2" {
                s3RangeReader = component "S3RangeReader" "Reads ranges from S3 storage" "Java Class"
            }
        }

        # Relationships for dynamic views
        application -> storageLibrary "Uses library to read data"
        application -> coreModule "Uses core functionality"
        application -> s3Module "Uses S3 functionality"
        
        # Direct relationships needed for dynamic views
        application -> fileRangeReader "Uses for file reading"
        application -> httpRangeReader "Uses for HTTP reading"
        application -> cachingRangeReader "Uses for cached reading"
        application -> diskCachingRangeReader "Uses for disk cached reading"
        application -> s3RangeReader "Uses for S3 reading"
        
        # Core component relationships
        fileRangeReader -> rangeReaderInterface "Implements"
        httpRangeReader -> rangeReaderInterface "Implements"
        s3RangeReader -> rangeReaderInterface "Implements"
        fileRangeReader -> abstractRangeReader "Extends"
        httpRangeReader -> abstractRangeReader "Extends"
        s3RangeReader -> abstractRangeReader "Extends"
        
        # Decorator relationships
        cachingRangeReader -> rangeReaderInterface "Implements"
        diskCachingRangeReader -> rangeReaderInterface "Implements"
        
        # Decorator chaining relationships
        cachingRangeReader -> diskCachingRangeReader "Delegates to"
        diskCachingRangeReader -> fileRangeReader "Delegates to"
        diskCachingRangeReader -> httpRangeReader "Delegates to"
        
        # External system relationships
        fileRangeReader -> localFileSystem "Reads from"
        httpRangeReader -> httpServer "Makes range requests to"
        s3RangeReader -> awsS3 "Makes range requests to"
        httpRangeReader -> authenticationSystem "Uses for authentication"
        diskCachingRangeReader -> localFileSystem "Reads/writes cache files"
        
        # Internal relationships
        abstractRangeReader -> application "Returns data to"
        fileRangeReader -> abstractRangeReader "Uses base functionality"
        httpRangeReader -> abstractRangeReader "Uses base functionality"
    }

    views {
        dynamic coreModule "BasicFileRead" {
            title "Basic File Range Reading"
            description "Shows the flow for reading a range from a local file"
            
            application -> fileRangeReader "1. readRange(offset, length)"
            fileRangeReader -> abstractRangeReader "2. readRange() validation"
            abstractRangeReader -> fileRangeReader "3. readRangeNoFlip()"
            fileRangeReader -> localFileSystem "4. FileChannel.read()"
            localFileSystem -> fileRangeReader "5. returns data"
            fileRangeReader -> abstractRangeReader "6. advances buffer position"
            abstractRangeReader -> application "7. returns bytes read"
            
            autoLayout
        }

        dynamic coreModule "HttpRangeRead" {
            title "HTTP Range Reading with Authentication"
            description "Shows the flow for reading a range from an HTTP server with authentication"
            
            application -> httpRangeReader "1. readRange(offset, length)"
            httpRangeReader -> abstractRangeReader "2. validation & preparation"
            abstractRangeReader -> httpRangeReader "3. readRangeNoFlip()"
            httpRangeReader -> authenticationSystem "4. prepare auth headers"
            authenticationSystem -> httpRangeReader "5. returns headers"
            httpRangeReader -> httpServer "6. HTTP Range request with auth"
            httpServer -> httpRangeReader "7. returns range data"
            httpRangeReader -> application "8. returns prepared data"
            
            autoLayout
        }

        dynamic coreModule "MultiLevelCaching" {
            title "Multi-Level Caching Scenario"
            description "Shows the decorator pattern in action with memory and disk caching"
            
            application -> cachingRangeReader "1. readRange(offset, length)"
            cachingRangeReader -> diskCachingRangeReader "2. cache miss - delegate"
            diskCachingRangeReader -> fileRangeReader "3. delegate to base reader"
            fileRangeReader -> localFileSystem "4. file range request"
            localFileSystem -> fileRangeReader "5. returns data"
            fileRangeReader -> diskCachingRangeReader "6. returns data"
            diskCachingRangeReader -> cachingRangeReader "7. returns data"
            cachingRangeReader -> application "8. returns cached data"
            
            autoLayout
        }

        dynamic coreModule "CacheHitScenario" {
            title "Cache Hit Scenario"
            description "Shows efficient cache hit path"
            
            application -> cachingRangeReader "1. readRange(offset, length)"
            cachingRangeReader -> application "2. returns cached data immediately"
            
            autoLayout
        }

        dynamic coreModule "DiskCacheRecovery" {
            title "Disk Cache Recovery After External Deletion"
            description "Shows resilience when cache files are deleted externally"
            
            application -> diskCachingRangeReader "1. readRange(offset, length)"
            diskCachingRangeReader -> localFileSystem "2. attempt to read cache file"
            localFileSystem -> diskCachingRangeReader "3. NoSuchFileException"
            diskCachingRangeReader -> httpRangeReader "4. re-load from delegate"
            httpRangeReader -> httpServer "5. HTTP range request"
            httpServer -> httpRangeReader "6. returns fresh data"
            httpRangeReader -> diskCachingRangeReader "7. returns data"
            diskCachingRangeReader -> localFileSystem "8. writes new cache file"
            diskCachingRangeReader -> application "9. returns data"
            
            autoLayout
        }

        dynamic s3Module "S3Authentication" {
            title "S3 Authentication Flow"
            description "Shows AWS credential resolution and S3 API authentication"
            
            application -> s3RangeReader "1. readRange(offset, length)"
            s3RangeReader -> awsS3 "2. authenticate and get range"
            awsS3 -> s3RangeReader "3. returns authenticated response"
            s3RangeReader -> application "4. returns data"
            
            autoLayout
        }

        styles {
            element "Software System" {
                background #1168BD
                color #ffffff
            }
            element "Container" {
                background #438DD5
                color #ffffff
            }
            element "Component" {
                background #85BBF0
                color #000000
            }
        }
    }
}

