#!/bin/bash
#
# Install git hooks for Icebox project
#

set -e

echo "🔧 Installing git hooks for Icebox project..."

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Check if we're in a git repository
if [ ! -d "$PROJECT_ROOT/.git" ]; then
    echo "❌ Error: Not in a git repository. Please run this script from the project root."
    exit 1
fi

# Create hooks directory if it doesn't exist
HOOKS_DIR="$PROJECT_ROOT/.git/hooks"
mkdir -p "$HOOKS_DIR"

# Copy pre-commit hook
echo "📝 Installing pre-commit hook..."
cp "$SCRIPT_DIR/../.git/hooks/pre-commit" "$HOOKS_DIR/pre-commit"
chmod +x "$HOOKS_DIR/pre-commit"

# Check if golangci-lint is installed
if ! command -v golangci-lint &> /dev/null; then
    echo "⚠️  golangci-lint not found. Installing..."
    go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
fi

echo "✅ Git hooks installed successfully!"
echo ""
echo "The pre-commit hook will now run automatically before each commit."
echo "It will check:"
echo "  - Code formatting (go fmt)"
echo "  - Code quality (go vet)"
echo "  - Linting (golangci-lint)"
echo "  - Module dependencies (go.mod/go.sum)"
echo ""
echo "Note: The hook focuses on code quality and formatting."
echo "Full testing is handled by the CI pipeline to keep commits fast."
echo ""
echo "To skip the hook for a specific commit, use:"
echo "  git commit --no-verify -m 'your message'"
echo ""
echo "To manually run the checks:"
echo "  make pre-commit"
