#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "Setting up Azure Resource Graph query tool..."

# Create virtual environment
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
else
    echo "Virtual environment already exists."
fi

# Activate virtual environment
source venv/bin/activate

# Upgrade pip
echo "Upgrading pip..."
pip install --upgrade pip

# Install dependencies
echo "Installing dependencies..."
pip install -r requirements.txt

# Create .env from example if it doesn't exist
if [ ! -f ".env" ]; then
    if [ -f "env.example" ]; then
        cp env.example .env
        echo "Created .env file from env.example - please edit it with your credentials."
    fi
fi

echo ""
echo "✓ Installation complete!"
echo ""
echo "To use the tool:"
echo "  1. Edit .env file with your Azure credentials"
echo "  2. Activate the environment:  source venv/bin/activate"
echo "  3. Run a query:"
echo "     python standalone_resource_graph.py -q \"Resources | project id, name, type\""
