#!/bin/bash
set -e

# Documentation build script for Tileverse
# This script sets up a virtual environment and builds the complete documentation
# for all Tileverse modules: Range Reader, PMTiles, Vector Tiles, and Tile Matrix Set

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="${SCRIPT_DIR}/.venv"

echo "🏗️  Building Tileverse Documentation"
echo "====================================="

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Check required dependencies
echo "🔍 Checking dependencies..."

if ! command_exists python3; then
    echo "❌ Python 3 is required but not installed"
    exit 1
fi

if ! command_exists docker; then
    echo "❌ Docker is required for diagram generation but not installed"
    exit 1
fi

echo "✅ All required dependencies found"

# Create and activate virtual environment
echo ""
echo "🐍 Setting up Python virtual environment..."

if [ ! -d "$VENV_DIR" ]; then
    echo "Creating new virtual environment..."
    python3 -m venv "$VENV_DIR"
else
    echo "Using existing virtual environment..."
fi

# Activate virtual environment
echo "Activating virtual environment: $VENV_DIR"
source "$VENV_DIR/bin/activate"

# Verify we're in the virtual environment
if [[ "$VIRTUAL_ENV" != "$VENV_DIR" ]]; then
    echo "❌ Failed to activate virtual environment"
    exit 1
fi

echo "✅ Virtual environment activated: $VIRTUAL_ENV"

# Upgrade pip and install dependencies
echo "📦 Installing Python dependencies..."
python -m pip install --upgrade pip
pip install -r requirements.txt

echo "✅ Python environment ready"

# Generate C4 model diagrams
echo ""
echo "📊 Generating C4 model diagrams..."

cd structurizr

# Make scripts executable
chmod +x *.sh

# Generate diagrams
echo "🔄 Running Structurizr diagram generation..."
./structurizr-generate-diagrams.sh

echo "✅ Diagram generation completed"

# Return to docs directory
cd "$SCRIPT_DIR"

# Validate MkDocs configuration
echo ""
echo "🔧 Validating MkDocs configuration..."
mkdocs --version

# Build documentation
echo ""
echo "📚 Building documentation..."
mkdocs build --verbose --strict

echo ""
echo "✅ Documentation build completed successfully!"
echo ""
echo "📁 Built site location: ${SCRIPT_DIR}/site/"
echo "🌐 To serve locally: mkdocs serve"
echo "🔄 To clean build: rm -rf site/ && ./build.sh"
echo ""
echo "💡 Remember to activate the virtual environment for future runs:"
echo "   source .venv/bin/activate"
