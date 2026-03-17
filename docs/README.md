# Tileverse Documentation

This directory contains the complete documentation for the Tileverse Range Reader library, built with MkDocs Material theme and featuring C4 model architectural diagrams.

## Quick Start

### Build Documentation

```bash
# One-time setup and build
./build.sh
```

### Development Server

```bash
# Start development server with auto-reload
./serve.sh
```

### Clean Build

```bash
# Remove all generated files and rebuild
./clean.sh && ./build.sh
```

## Documentation Structure

```
docs/
├── build.sh              # Main build script (sets up venv, generates diagrams, builds docs)
├── serve.sh              # Development server script
├── clean.sh              # Clean build artifacts script
├── update-supporters.sh  # Regenerates the root SUPPORTERS.md file
├── requirements.txt      # Python dependencies for MkDocs
├── mkdocs.yml            # MkDocs configuration for the entire monorepo
├── resources/            # Supporters maintenance inputs
│   ├── additional-individual-supporters.txt
│   ├── contributor-aliases.txt
│   └── ignored-contributors.txt
│
├── src/                  # Documentation source files (Markdown)
│   ├── index.md          # Monorepo homepage
│   ├── getting-started.md# Guide for new users to get started with the project
│   ├── developer-guide/  # General developer documentation
│   │   ├── index.md      # Overview of development
│   │   ├── building.md   # Build instructions for the monorepo
│   │   ├── testing.md    # General testing strategy
│   │   ├── contributing.md # Guidelines for contributing
│   │   └── architecture/ # Core architecture documents and diagrams
│   ├── rangereader/      # Range Reader module documentation
│   │   └── developer-guide/ # Range Reader specific architecture/performance
│   ├── pmtiles/          # PMTiles module documentation
│   ├── vectortiles/      # Vector Tiles module documentation
│   ├── tilematrixset/    # Tile Matrix Set module documentation
│   └── assets/           # Static assets (images, CSS, JS)
│       └── images/
│           ├── supporters/   # Supporters logos used by the repository README
│           └── rangereader/  # Generated SVG diagrams for Range Reader
│               └── .gitkeep  # Preserves directory in git
│
├── structurizr/          # C4 model architectural diagrams definitions (source)
│   ├── workspace.dsl     # Main C4 model definition
│   ├── dynamic-views.dsl # Dynamic view definitions
│   ├── exports/          # Generated PlantUML files (.puml)
│   │   └── .gitkeep      # Preserves directory in git
│   ├── structurizr-generate-diagrams.sh    # Generates PlantUML from DSL
│   └── plantuml-generate-svg.sh            # Converts PlantUML to SVG
│
└── site/                 # Generated static documentation (after build)
```

## Features

### C4 Model Integration (Range Reader)

The documentation for the Range Reader module includes automatically generated C4 architectural diagrams:

- **System Context**: Shows how the Range Reader fits in the broader ecosystem
- **Container View**: Displays the modular architecture of Range Reader
- **Component Views**: Details internal structure of Range Reader's core and all modules
- **Runtime Views**: Dynamic scenarios (work in progress)

## Build Process

The `build.sh` script handles the complete build process:

1. **Environment Setup**: Creates Python virtual environment and installs dependencies
2. **Diagram Generation**: 
   - Runs Structurizr CLI to generate PlantUML from DSL files
   - Converts PlantUML to SVG using Docker
   - Copies SVGs to `src/assets/images/rangereader/` directory
3. **Documentation Build**: Uses MkDocs to build the complete site
4. **Validation**: Ensures all links and references are valid

## Requirements

- **Python 3.8+**: For MkDocs and dependencies
- **Docker**: For C4 diagram generation (Structurizr CLI and PlantUML)
- **Internet connection**: For pulling Docker images during diagram generation

## Contributing

When contributing to documentation:

1. Follow the existing structure and style
2. Update both content and navigation in `mkdocs.yml`
3. Test with `./serve.sh` before submitting
4. Ensure all links work with `./build.sh`
5. Include relevant architectural updates in C4 diagrams if needed

## Supporters Maintenance

The supporters list published in the repository root at `SUPPORTERS.md` is maintained from the documentation tree.

- The generator script lives at `docs/update-supporters.sh`.
- Additional individual supporters are maintained in `docs/resources/additional-individual-supporters.txt`.
- Contributor aliases can be normalized in `docs/resources/contributor-aliases.txt`.
- Entries that should not appear as individual supporters can be excluded in `docs/resources/ignored-contributors.txt`.
- The sponsors list published in the repository root at `SPONSORS.md` is maintained manually.

Run:

```bash
bash docs/update-supporters.sh
```
