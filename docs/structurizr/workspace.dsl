workspace "Tileverse" "Architecture documentation for the Tileverse Java libraries (cloud-native geospatial I/O, PMTiles, Vector Tiles, Tile Matrix Set, Tile Stores)" {

    model {
        properties {
            "structurizr.groupSeparator" "/"
        }

        # People and external systems
        developer = person "Developer" "A developer building applications with the Tileverse libraries" {
            tags "Person"
        }

        application = person "Application" "An application that consumes Tileverse to access tiled or cloud-stored geospatial data" {
            tags "Application"
        }

        # External systems
        localFileSystem = softwareSystem "Local File System" "Local files on disk" {
            tags "External"
        }

        httpServer = softwareSystem "HTTP Server" "Remote HTTP/HTTPS servers" {
            tags "External"
        }

        awsS3 = softwareSystem "Amazon S3" "AWS S3 and S3-compatible storage (MinIO, Ceph RGW, R2, B2, Wasabi, etc.)" {
            tags "External,Cloud"
        }

        azureBlob = softwareSystem "Azure Blob Storage" "Microsoft Azure Blob Storage and Data Lake Gen2" {
            tags "External,Cloud"
        }

        googleCloud = softwareSystem "Google Cloud Storage" "Google Cloud Storage service (flat and Hierarchical Namespace buckets)" {
            tags "External,Cloud"
        }

        # The whole Tileverse software system
        tileverse = softwareSystem "Tileverse" "Java toolkit for cloud-native geospatial data access: object-storage I/O abstraction, PMTiles archive reader, Mapbox Vector Tiles codec, OGC Tile Matrix Set model, and format-agnostic tile stores." {
            tags "TileverseSystem"

            # === Storage modules ===
            coreModule = container "tileverse-storage-core" "Storage API, RangeReader API, SPI (StorageProvider/StorageConfig), decorators, File and HTTP backends" "Java 17, Maven" {
                tags "Module,Core,Storage"

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

                cachingRangeReader = component "CachingRangeReader" "In-memory caching decorator (Caffeine-backed)" "Java Class" {
                    tags "Decorator"
                }

                diskCachingRangeReader = component "DiskCachingRangeReader" "Disk-based caching decorator" "Java Class" {
                    tags "Decorator"
                }

                blockAlignedRangeReader = component "BlockAlignedRangeReader" "Reads coalesced into fixed-size aligned blocks" "Java Class" {
                    tags "Decorator"
                }

                storageProvider = component "StorageProvider SPI" "ServiceLoader-based provider SPI (StorageProvider, StorageConfig, StorageParameter)" "Java Interface" {
                    tags "Interface"
                }

                storageFactory = component "StorageFactory" "Resolves a StorageProvider for a URI/Properties and opens a Storage; per-key range reads via Storage.openRangeReader(String)" "Java Class" {
                    tags "Factory"
                }

                authenticationSystem = component "Authentication System" "HTTP authentication implementations (Basic, Bearer, API Key, Digest, custom headers)" "Java Package" {
                    tags "Authentication"
                }
            }

            s3Module = container "tileverse-storage-s3" "Amazon S3 and S3-compatible storage support (io.tileverse.storage.s3)" "Java 17, AWS SDK v2" {
                tags "Module,Cloud,Storage"

                s3Storage = component "S3Storage / S3RangeReader" "S3 backend (general-purpose buckets and S3 Express One Zone)" "Java Class" {
                    tags "Implementation"
                }
            }

            azureModule = container "tileverse-storage-azure" "Azure Blob Storage and Data Lake Gen2 support (io.tileverse.storage.azure)" "Java 17, Azure SDK" {
                tags "Module,Cloud,Storage"

                azureBlobStorage = component "AzureBlobStorage / AzureBlobRangeReader" "Azure Blob backend (flat keyspace + virtual directories)" "Java Class" {
                    tags "Implementation"
                }

                azureDataLakeStorage = component "AzureDataLakeStorage" "Azure Data Lake Gen2 backend (HNS, real directories, atomic rename)" "Java Class" {
                    tags "Implementation"
                }
            }

            gcsModule = container "tileverse-storage-gcs" "Google Cloud Storage support (io.tileverse.storage.gcs)" "Java 17, Google Cloud SDK" {
                tags "Module,Cloud,Storage"

                gcsStorage = component "GoogleCloudStorage / GoogleCloudStorageRangeReader" "GCS backend (flat or Hierarchical Namespace; HNS auto-detected)" "Java Class" {
                    tags "Implementation"
                }
            }

            allModule = container "tileverse-storage-all" "Aggregates all storage providers into a single dependency" "Java 17, Maven" {
                tags "Module,Aggregation,Storage"
            }

            # === Other Tileverse modules ===
            vectortilesModule = container "tileverse-vectortiles" "Mapbox Vector Tiles (MVT) codec: encode/decode between MVT bytes and a JTS-backed model" "Java 17, Protocol Buffers, JTS" {
                tags "Module,Format"

                vectorTileCodec = component "VectorTileCodec" "Encode VectorTile to byte[]/OutputStream/ByteBuffer; decode from byte[]/ByteBuffer/InputStream" "Java Class" {
                    tags "Codec"
                }

                vectorTileBuilder = component "VectorTileBuilder" "Fluent builder for VectorTile/LayerBuilder/FeatureBuilder" "Java Class" {
                    tags "Builder"
                }

                vectorTileModel = component "VectorTile Model" "VectorTile, Layer, Feature interfaces (JTS Geometry payload)" "Java Interfaces" {
                    tags "Interface"
                }
            }

            tilematrixsetModule = container "tileverse-tilematrixset" "OGC Two Dimensional Tile Matrix Set (17-083r2) data model and JSON/XML encodings" "Java 17, JTS, Jackson" {
                tags "Module,Math"

                tileMatrixSet = component "TileMatrixSet" "Top-level OGC TMS: identifier, CRS, extent, list of TileMatrix levels" "Java Interface" {
                    tags "Interface"
                }

                tilePyramid = component "TilePyramid" "Grid arithmetic only (no CRS): TileRange levels with shared CornerOfOrigin" "Java Class" {
                    tags "Implementation"
                }

                defaultSets = component "DefaultTileMatrixSets" "WebMercatorQuad, WorldCRS84Quad, and legacy variants" "Java Class" {
                    tags "Implementation"
                }

                matrixSetIO = component "TileMatrixSetIO" "JSON (Annex C) and XML (Annex D) round-trip" "Java Class" {
                    tags "Codec"
                }
            }

            tilestoreModule = container "tileverse-tilestore" "Format-agnostic TileStore<T> abstraction over a TileMatrixSet, plus VectorTileStore, RasterTileStore, and TileJSON v3 model" "Java 17" {
                tags "Module,Abstraction"

                tileStoreInterface = component "TileStore<T>" "Generic tile store: load by Tile, findTiles by extent and zoom or resolution" "Java Interface" {
                    tags "Interface"
                }

                abstractTileStore = component "AbstractTileStore<T>" "Base class holding the TileMatrixSet" "Java Abstract Class" {
                    tags "Abstract"
                }

                vectorTileStore = component "VectorTileStore" "Abstract tile store whose payload is a decoded VectorTile" "Java Abstract Class" {
                    tags "Abstract"
                }

                rasterTileStore = component "RasterTileStore" "Abstract tile store whose payload is a decoded RenderedImage; advertises a MIME type" "Java Abstract Class" {
                    tags "Abstract"
                }

                tileJson = component "TileJSON v3 Model" "Jackson record model of the TileJSON 3.0.0 spec (io.tileverse.jackson.databind.tilejson.v3)" "Java Records" {
                    tags "Model"
                }
            }

            pmtilesModule = container "tileverse-pmtiles" "PMTiles v3 archive reader: low-level PMTilesReader plus PMTilesVectorTileStore and PMTilesRasterTileStore wrappers" "Java 17" {
                tags "Module,Format"

                pmtilesReader = component "PMTilesReader" "Low-level reader: header, directory, raw tile bytes; streaming IOFunction overload" "Java Class" {
                    tags "Reader"
                }

                pmtilesHeader = component "PMTilesHeader" "Archive metadata: tileType, MIME type, zoom range, bounds, compression" "Java Record" {
                    tags "Model"
                }

                pmtilesVectorStore = component "PMTilesVectorTileStore" "VectorTileStore over a PMTiles archive (validates tileType == MVT)" "Java Class" {
                    tags "Implementation"
                }

                pmtilesRasterStore = component "PMTilesRasterTileStore" "RasterTileStore over a PMTiles archive (PNG/JPEG/WebP/AVIF); decodes via ImageIO from the streaming InputStream" "Java Class" {
                    tags "Implementation"
                }

                hilbertCurve = component "HilbertCurve" "Hilbert curve indexing used by the directory" "Java Class" {
                    tags "Internal"
                }
            }
        }

        # === External relationships ===
        developer -> tileverse "Builds applications with"
        application -> tileverse "Reads geospatial data through"

        tileverse -> localFileSystem "Reads / writes local files"
        tileverse -> httpServer "Issues range / streaming HTTP requests"
        tileverse -> awsS3 "Issues range / streaming S3 requests"
        tileverse -> azureBlob "Issues range / streaming Azure requests"
        tileverse -> googleCloud "Issues range / streaming GCS requests"

        # === Container relationships ===
        coreModule -> localFileSystem "Reads / writes via FileStorage"
        coreModule -> httpServer "Reads via HttpStorage"
        s3Module -> awsS3 "Reads / writes via S3Storage"
        azureModule -> azureBlob "Reads / writes via AzureBlobStorage / AzureDataLakeStorage"
        gcsModule -> googleCloud "Reads / writes via GoogleCloudStorage"

        allModule -> coreModule "Aggregates"
        allModule -> s3Module "Aggregates"
        allModule -> azureModule "Aggregates"
        allModule -> gcsModule "Aggregates"

        pmtilesModule -> coreModule "Reads tile bytes through RangeReader"
        pmtilesModule -> tilematrixsetModule "Uses for WebMercatorQuad math"
        pmtilesModule -> vectortilesModule "Decodes MVT tiles with"
        pmtilesModule -> tilestoreModule "Implements TileStore<T> via VectorTileStore / RasterTileStore"

        tilestoreModule -> tilematrixsetModule "Builds on TileMatrixSet"
        tilestoreModule -> vectortilesModule "Returns VectorTile from VectorTileStore"

        # === Component relationships - Storage core ===
        abstractRangeReader -> rangeReaderInterface "Implements"
        fileStorage -> storageInterface "Implements"
        httpStorage -> storageInterface "Implements"
        fileStorage -> abstractRangeReader "Provides RangeReader via"
        httpStorage -> abstractRangeReader "Provides RangeReader via"

        cachingRangeReader -> rangeReaderInterface "Implements (decorator)"
        diskCachingRangeReader -> rangeReaderInterface "Implements (decorator)"
        blockAlignedRangeReader -> rangeReaderInterface "Implements (decorator)"

        httpStorage -> authenticationSystem "Uses for authentication"

        storageFactory -> storageProvider "Discovers providers via ServiceLoader"
        storageFactory -> storageInterface "Returns instances of"
        storageFactory -> s3Module "Opens Storage backed by"
        storageFactory -> azureModule "Opens Storage backed by"
        storageFactory -> gcsModule "Opens Storage backed by"

        # === Component relationships - Cloud modules ===
        s3Storage -> storageInterface "Implements"
        azureBlobStorage -> storageInterface "Implements"
        azureDataLakeStorage -> storageInterface "Implements"
        gcsStorage -> storageInterface "Implements"
        s3Storage -> abstractRangeReader "Provides RangeReader via"
        azureBlobStorage -> abstractRangeReader "Provides RangeReader via"
        azureDataLakeStorage -> abstractRangeReader "Provides RangeReader via"
        gcsStorage -> abstractRangeReader "Provides RangeReader via"

        # === Component relationships - Vector Tiles ===
        vectorTileBuilder -> vectorTileModel "Builds"
        vectorTileCodec -> vectorTileModel "Encodes / decodes"

        # === Component relationships - Tile Stores ===
        abstractTileStore -> tileStoreInterface "Implements"
        vectorTileStore -> abstractTileStore "Extends with VectorTile payload"
        rasterTileStore -> abstractTileStore "Extends with RenderedImage payload"
        vectorTileStore -> tileJson "Exposes per-layer metadata as"

        # === Component relationships - PMTiles ===
        pmtilesReader -> pmtilesHeader "Returns"
        pmtilesReader -> hilbertCurve "Indexes tiles with"
        pmtilesReader -> rangeReaderInterface "Reads bytes through"
        pmtilesVectorStore -> pmtilesReader "Streams tile bytes from"
        pmtilesRasterStore -> pmtilesReader "Streams tile bytes from"
        pmtilesVectorStore -> vectorTileCodec "Decodes tiles with"
        pmtilesVectorStore -> vectorTileStore "Extends"
        pmtilesRasterStore -> rasterTileStore "Extends"
    }

    views {
        systemContext tileverse "SystemContext" {
            include *
            autoLayout
            title "System Context - Tileverse"
            description "How the Tileverse libraries fit between consumer applications and external data sources."
        }

        container tileverse "Containers" {
            include *
            autoLayout
            title "Container View - Tileverse Modules"
            description "All six Tileverse modules and their dependencies on each other and on external storage systems."
        }

        component coreModule "CoreComponents" {
            include *
            autoLayout
            title "Component View - tileverse-storage-core"
            description "Internal structure of the storage core: Storage/RangeReader interfaces, decorators, SPI, and built-in File/HTTP backends."
        }

        component allModule "AllModuleComponents" {
            include *
            autoLayout
            title "Component View - tileverse-storage-all"
            description "The all-aggregator module pulling in every storage backend; consumers reach the providers through StorageFactory in core."
        }

        component pmtilesModule "PMTilesComponents" {
            include *
            autoLayout
            title "Component View - tileverse-pmtiles"
            description "PMTiles internals: PMTilesReader, header, Hilbert indexing, and the vector/raster tile-store wrappers."
        }

        component vectortilesModule "VectorTilesComponents" {
            include *
            autoLayout
            title "Component View - tileverse-vectortiles"
            description "VectorTileCodec, VectorTileBuilder, and the JTS-backed VectorTile model."
        }

        component tilematrixsetModule "TileMatrixSetComponents" {
            include *
            autoLayout
            title "Component View - tileverse-tilematrixset"
            description "OGC TMS data model: TileMatrixSet, TilePyramid, bundled DefaultTileMatrixSets, and the JSON/XML codecs."
        }

        component tilestoreModule "TileStoreComponents" {
            include *
            autoLayout
            title "Component View - tileverse-tilestore"
            description "Format-agnostic TileStore<T> abstraction with VectorTileStore, RasterTileStore, and the TileJSON v3 model."
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

            element "Module,Core,Storage" {
                background #1976D2
                color #ffffff
            }

            element "Module,Cloud,Storage" {
                background #FF5722
                color #ffffff
            }

            element "Module,Aggregation,Storage" {
                background #9C27B0
                color #ffffff
            }

            element "Module,Format" {
                background #00897B
                color #ffffff
            }

            element "Module,Math" {
                background #5E35B1
                color #ffffff
            }

            element "Module,Abstraction" {
                background #455A64
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

            element "Codec" {
                shape Component
                background #00BCD4
                color #ffffff
            }

            element "Builder" {
                shape Component
                background #03A9F4
                color #ffffff
            }

            element "Reader" {
                shape Component
                background #3F51B5
                color #ffffff
            }

            element "Model" {
                shape Component
                background #607D8B
                color #ffffff
            }

            element "Internal" {
                shape Component
                background #BDBDBD
                color #000000
            }
        }

        theme default
    }
}
