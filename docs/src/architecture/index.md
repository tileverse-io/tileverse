# Tileverse Architecture

Understand how the Tileverse modules fit together.

## Module Overview

Tileverse is composed of four main modules:

1. **Range Reader**: Foundation for byte-range data access
2. **Tile Matrix Set**: Coordinate systems and tiling schemes
3. **Vector Tiles**: MVT encoding/decoding
4. **PMTiles**: Tile archive format

## Architecture Diagram

```
┌─────────────────────────────────────────────────────┐
│                   Applications                      │
│  (GeoServer, Custom Tile Servers, ETL Pipelines)   │
└─────────────────┬───────────────────────────────────┘
                  │
    ┌─────────────┼─────────────┬─────────────┐
    │             │             │             │
    ▼             ▼             ▼             ▼
┌─────────┐  ┌──────────┐  ┌────────┐  ┌──────────┐
│ PMTiles │  │  Vector  │  │  Tile  │  │  Other   │
│         │  │  Tiles   │  │Matrix  │  │ Formats  │
└────┬────┘  └────┬─────┘  └────┬───┘  └────┬─────┘
     │            │             │            │
     └────────────┴─────────────┴────────────┘
                       │
                       ▼
              ┌────────────────┐
              │ Range Reader   │
              │  (Core Layer)  │
              └────────┬───────┘
                       │
         ┌─────────────┼─────────────┬────────────┐
         │             │             │            │
         ▼             ▼             ▼            ▼
    ┌────────┐    ┌───────┐    ┌────────┐   ┌─────┐
    │  File  │    │  HTTP │    │   S3   │   │ ... │
    └────────┘    └───────┘    └────────┘   └─────┘
```

## Design Principles

1. **Modularity**: Each library can be used independently
2. **Composability**: Libraries work together seamlessly
3. **Cloud-native**: Designed for cloud storage from the ground up
4. **Performance**: Optimized for high-throughput server applications
5. **Standards-based**: Implements established specifications

## Module Dependencies

- **PMTiles** depends on Range Reader, Tile Matrix Set, and Vector Tiles
- **Vector Tiles** is independent
- **Tile Matrix Set** is independent
- **Range Reader** is the foundation

## Next Steps

- [System Design](system-design.md)
- [Module Dependencies](dependencies.md)
