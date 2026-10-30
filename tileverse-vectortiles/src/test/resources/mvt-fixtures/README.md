# mvt-fixtures

The synthetic fixtures of [mapbox/mvt-fixtures](https://github.com/mapbox/mvt-fixtures), version 4.0.0, commit
54e216ce6bb52c8ceee97b2b9f65438d8eb67b34. Each directory under `fixtures/` holds a vector tile (`tile.mvt`), its
content as JSON (`tile.json`), and its description with its validity against versions 1 and 2 of the Mapbox Vector
Tile specification (`info.json`).

The source repository distributes its code under CC0 1.0 and its fixtures under the Open Data Commons Open Database
License (ODbL) 1.0, and asks that both licenses be included: see `LICENSE-CC0-1.0.md` and `LICENSE-ODbL-1.0.md`. The
real-world tiles of the source repository are not included.

`VectorTileCodecTest` decodes each fixture and checks the outcome.
