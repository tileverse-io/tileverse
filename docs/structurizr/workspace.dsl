workspace "Tileverse Storage" "Architecture documentation for the Tileverse Storage library (I/O abstraction over object storage)" {

    model {
        properties {
            "structurizr.groupSeparator" "/"
        }

        # People and external systems
        developer = person "Developer" "A developer using the Tileverse Storage library" {
            tags "Person"
        }
        
        application = person "Application" "An application that uses the library to read data" {
            tags "Application"
        }

        # External systems
        localFileSystem = softwareSystem "Local File System" "Local files on disk" {
            tags "External"
        }
        
        httpServer = softwareSystem "HTTP Server" "Remote HTTP/HTTPS servers" {
            tags "External"
        }
        
        awsS3 = softwareSystem "Amazon S3" "AWS S3 and S3-compatible storage" {
            tags "External,Cloud"
        }
        
        azureBlob = softwareSystem "Azure Blob Storage" "Microsoft Azure Blob Storage service" {
            tags "External,Cloud"
        }
        
        googleCloud = softwareSystem "Google Cloud Storage" "Google Cloud Storage service" {
            tags "External,Cloud"
        }

        # Main system
        storageLibrary = softwareSystem "Tileverse Storage" "Java I/O abstraction over object storage (local files, HTTP, S3, Azure Blob, GCS). Exposes a container Storage API (list, range/streaming reads, atomic writes, deletes, server-side copy, presigned URLs) and a byte-range RangeReader API used by single-file consumers like PMTiles, COG, and single-file Parquet." {
            tags "TileverseSystem"

            # Core module
            coreModule = container "Core Module" "Storage API, RangeReader API, SPI (StorageProvider/StorageConfig), decorators, File and HTTP backends" "Java 17, Maven" {
                tags "Module,Core"

                # Top-level Storage API
                storageInterface = component "Storage Interface" "Container API: stat, list, openRangeReader, read, put, delete, copy/move, presign" "Java Interface" {
                    tags "Interface"
                }

                rangeReaderInterface = component "RangeReader Interface" "Byte-range read API for a single known blob" "Java Interface" {
                    tags "Interface"
                }

                abstractRangeReader = component "AbstractRangeReader" "Base implementation with common functionality" "Java Abstract Class" {
                    tags "Abstract"
                }

                fileStorage = component "FileStorage / FileRangeReader" "Local filesystem backend (io.tileverse.storage.file)" "Java Class" {
                    tags "Implementation"
                }

                httpStorage = component "HttpStorage / HttpRangeReader" "Read-only HTTP/HTTPS backend (io.tileverse.storage.http)" "Java Class" {
                    tags "Implementation"
                }

                # Decorators
                cachingRangeReader = component "CachingRangeReader" "In-memory caching decorator" "Java Class" {
                    tags "Decorator"
                }

                diskCachingRangeReader = component "DiskCachingRangeReader" "Disk-based caching decorator" "Java Class" {
                    tags "Decorator"
                }

                # SPI
                storageProvider = component "StorageProvider SPI" "ServiceLoader-based provider SPI (StorageProvider, StorageConfig, StorageParameter)" "Java Interface" {
                    tags "Interface"
                }

                # Top-level entry point
                storageFactory = component "StorageFactory" "Resolves a StorageProvider for a URI/Properties and opens a Storage; per-key range reads via Storage.openRangeReader(String)" "Java Class" {
                    tags "Factory"
                }

                # Authentication
                authenticationSystem = component "Authentication System" "HTTP authentication implementations" "Java Package" {
                    tags "Authentication"
                }
            }

            # Cloud provider modules
            s3Module = container "S3 Module" "Amazon S3 and S3-compatible storage support (io.tileverse.storage.s3)" "Java 17, AWS SDK v2" {
                tags "Module,Cloud"

                s3Storage = component "S3Storage / S3RangeReader" "S3 backend (general-purpose buckets and S3 Express One Zone)" "Java Class" {
                    tags "Implementation"
                }
            }

            azureModule = container "Azure Module" "Azure Blob Storage and Data Lake Gen2 support (io.tileverse.storage.azure)" "Java 17, Azure SDK" {
                tags "Module,Cloud"

                azureBlobStorage = component "AzureBlobStorage / AzureBlobRangeReader" "Azure Blob backend (flat keyspace + virtual directories)" "Java Class" {
                    tags "Implementation"
                }

                azureDataLakeStorage = component "AzureDataLakeStorage" "Azure Data Lake Gen2 backend (HNS, real directories, atomic rename)" "Java Class" {
                    tags "Implementation"
                }
            }

            gcsModule = container "GCS Module" "Google Cloud Storage support (io.tileverse.storage.gcs)" "Java 17, Google Cloud SDK" {
                tags "Module,Cloud"

                gcsStorage = component "GoogleCloudStorage / GoogleCloudStorageRangeReader" "GCS backend (flat or Hierarchical Namespace; HNS auto-detected)" "Java Class" {
                    tags "Implementation"
                }
            }

            # Aggregation module
            allModule = container "All Module" "Aggregates all providers into a single dependency" "Java 17, Maven" {
                tags "Module,Aggregation"
            }
        }

        # Relationships - External to system
        developer -> storageLibrary "Uses library to build applications"
        application -> storageLibrary "Uses to read data ranges"
        
        # Relationships - System to external
        storageLibrary -> localFileSystem "Reads from local files"
        storageLibrary -> httpServer "Makes HTTP range requests"
        storageLibrary -> awsS3 "Makes S3 range requests"
        storageLibrary -> azureBlob "Makes Azure Blob range requests"
        storageLibrary -> googleCloud "Makes GCS range requests"
        
        # Container relationships
        coreModule -> localFileSystem "Reads / writes via FileStorage"
        coreModule -> httpServer "Reads via HttpStorage"
        s3Module -> awsS3 "Reads / writes via S3Storage"
        azureModule -> azureBlob "Reads / writes via AzureBlobStorage / AzureDataLakeStorage"
        gcsModule -> googleCloud "Reads / writes via GoogleCloudStorage"

        allModule -> coreModule "Depends on"
        allModule -> s3Module "Depends on"
        allModule -> azureModule "Depends on"
        allModule -> gcsModule "Depends on"

        # Component relationships - Core
        abstractRangeReader -> rangeReaderInterface "Implements"
        fileStorage -> storageInterface "Implements"
        httpStorage -> storageInterface "Implements"
        fileStorage -> abstractRangeReader "Provides RangeReader via"
        httpStorage -> abstractRangeReader "Provides RangeReader via"

        cachingRangeReader -> rangeReaderInterface "Implements (decorator)"
        diskCachingRangeReader -> rangeReaderInterface "Implements (decorator)"

        httpStorage -> authenticationSystem "Uses for authentication"

        # Component relationships - Cloud modules
        s3Storage -> storageInterface "Implements"
        azureBlobStorage -> storageInterface "Implements"
        azureDataLakeStorage -> storageInterface "Implements"
        gcsStorage -> storageInterface "Implements"
        s3Storage -> abstractRangeReader "Provides RangeReader via"
        azureBlobStorage -> abstractRangeReader "Provides RangeReader via"
        azureDataLakeStorage -> abstractRangeReader "Provides RangeReader via"
        gcsStorage -> abstractRangeReader "Provides RangeReader via"

        # Component relationships - StorageFactory
        storageFactory -> storageProvider "Discovers providers via ServiceLoader"
        storageFactory -> storageInterface "Returns instances of"
        storageFactory -> s3Module "Opens Storage backed by"
        storageFactory -> azureModule "Opens Storage backed by"
        storageFactory -> gcsModule "Opens Storage backed by"
    }

    views {
        systemContext storageLibrary "SystemContext" {
            include *
            autoLayout
            title "System Context - Tileverse Storage"
            description "Shows how the Tileverse Storage library fits into the overall ecosystem, connecting applications to various data sources."
        }

        container storageLibrary "Containers" {
            include *
            autoLayout
            title "Container View - Tileverse Storage Modules"
            description "Shows the modular structure of the library with core functionality and cloud provider extensions."
        }

        component coreModule "CoreComponents" {
            include *
            autoLayout
            title "Component View - Core Module"
            description "Shows the internal structure of the core module including the StorageProvider SPI and the decorator pattern implementation."
        }

        component allModule "AllModuleComponents" {
            include *
            autoLayout
            title "Component View - All Module"
            description "Shows the All module aggregating the per-backend providers; consumers reach the providers through StorageFactory in core."
        }

        styles {
            element "Person" {
                shape Person
                background #08427B
                color #ffffff
            }
            
            element "Application" {
                shape Robot
                background #1168BD
                color #ffffff
            }
            
            element "TileverseSystem" {
                background #2E7D32
                color #ffffff
            }
            
            element "External" {
                background #999999
                color #ffffff
            }
            
            element "External,Cloud" {
                background #FF5722
                color #ffffff
            }
            
            element "Module" {
                shape Component
            }
            
            element "Module,Core" {
                background #1976D2
                color #ffffff
            }
            
            element "Module,Cloud" {
                background #FF5722
                color #ffffff
            }
            
            element "Module,Aggregation" {
                background #9C27B0
                color #ffffff
            }

            element "Interface" {
                shape Component
                background #4CAF50
                color #ffffff
            }
            
            element "Abstract" {
                shape Component
                background #8BC34A
                color #ffffff
            }
            
            element "Implementation" {
                shape Component
                background #2196F3
                color #ffffff
            }
            
            element "Decorator" {
                shape Component
                background #FF9800
                color #ffffff
            }
            
            element "Authentication" {
                shape Component
                background #E91E63
                color #ffffff
            }

            element "Factory" {
                shape Component
                background #673AB7
                color #ffffff
            }
        }
        
        theme default
    }
}