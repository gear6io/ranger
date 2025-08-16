# Contributing to Icebox

First off, thank you for considering contributing to Icebox! It's people like you that make Icebox such a great tool.

Icebox is an open-source project and we welcome any contributions, whether they are bug reports, feature requests, documentation improvements, or code contributions.

This document provides guidelines for contributing to Icebox.

## Table of Contents

- [Contributing to Icebox](#contributing-to-icebox)
  - [Table of Contents](#table-of-contents)
  - [Code of Conduct](#code-of-conduct)
  - [How Can I Contribute?](#how-can-i-contribute)
    - [Reporting Bugs](#reporting-bugs)
    - [Suggesting Enhancements](#suggesting-enhancements)
    - [Your First Code Contribution](#your-first-code-contribution)
    - [Pull Requests](#pull-requests)
  - [Development Setup](#development-setup)
  - [Coding Standards](#coding-standards)
    - [Go](#go)
    - [Git Commit Messages](#git-commit-messages)
  - [Pull Request Etiquette](#pull-request-etiquette)
  - [Community](#community)

## Code of Conduct

This project and everyone participating in it is governed by the [Icebox Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code. Please report unacceptable behavior to TFMV.

## How Can I Contribute?

### Reporting Bugs

If you find a bug, please ensure the bug was not already reported by searching on GitHub under [Issues](https://github.com/TFMV/icebox/issues).

If you're unable to find an open issue addressing the problem, [open a new one](https://github.com/TFMV/icebox/issues/new). Be sure to include a **title and clear description**, as much relevant information as possible, and a **code sample or an executable test case** demonstrating the expected behavior that is not occurring.

### Suggesting Enhancements

If you have an idea for a new feature or an improvement to an existing one, please open an issue on GitHub. Clearly describe the proposed enhancement, including:

- A clear and descriptive title.
- A detailed explanation of the enhancement.
- The motivation or use case for the enhancement.
- Any potential drawbacks or considerations.

This allows for discussion and refinement of the idea before any code is written.

### Your First Code Contribution

Unsure where to begin contributing to Icebox? You can start by looking through `good first issue` and `help wanted` issues:

- [Good first issues](https://github.com/TFMV/icebox/labels/good%20first%20issue) - issues which should only require a few lines of code, and a test or two.
- [Help wanted issues](https://github.com/TFMV/icebox/labels/help%20wanted) - issues which should be a bit more involved than `good first issue` issues.

### Pull Requests

We welcome pull requests! Please follow these steps:

1. Fork the repository and create your branch from `main`.
2. If you've added code that should be tested, add tests.
3. If you've changed APIs, update the documentation.
4. Ensure the test suite passes (`go test ./...`).
5. Make sure your code lints (`golangci-lint run`).
6. Issue that pull request!

## Development Setup

Please refer to the [Development section in the README.md](README.md#development) for instructions on how to set up your development environment.

Key prerequisites include:

- Go 1.21+
- DuckDB (for local CLI testing, optional, see README for details)

General build and test commands:

```bash
git clone https://github.com/TFMV/icebox.git
cd icebox
go mod tidy
go build -o icebox cmd/icebox/main.go
go test ./...
```

### Pre-commit Hook

To ensure code quality, we provide a pre-commit hook that runs automatically before each commit. The hook performs the following checks:

- **Code formatting**: Ensures Go code is properly formatted
- **Code quality**: Runs `go vet` to catch common mistakes
- **Linting**: Runs `golangci-lint` if available
- **Dependencies**: Verifies `go.mod` and `go.sum` are clean

**Note**: The hook focuses on code quality and formatting. Full testing is handled by the CI pipeline to keep commits fast and focused.

#### Installation

Install the pre-commit hook using the Makefile:
```bash
make install-hooks
```

#### Usage

Once installed, the hook runs automatically on every commit. If any checks fail, the commit will be blocked until the issues are resolved.

To manually run the same checks:
```bash
make pre-commit
```

## Coding Standards

### Go

- Follow standard Go formatting (use `gofmt` or `goimports`).
- Write clear, concise, and well-documented code.
- Aim for simplicity and readability.
- Handle errors explicitly; avoid panics in library code.
- Write unit tests for new functionality and bug fixes.
- Consider using `golangci-lint` for linting your code. A configuration file (`.golangci.yml`) may be added to the project in the future.

### Git Commit Messages

- Use the present tense ("Add feature" not "Added feature").
- Use the imperative mood ("Move cursor to..." not "Moves cursor to...").
- Limit the first line to 72 characters or less.
- Reference issues and pull requests liberally after the first line.
- Consider using [Conventional Commits](https://www.conventionalcommits.org/) for more structured commit messages, though not strictly enforced yet.

Example:

```
feat: Add support for Avro file imports

This commit introduces the capability to import data from Avro files
into Iceberg tables. It includes schema inference for Avro and updates
the import CLI command.

Fixes #123
Related to #456
```

## Pull Request Etiquette

- **Keep PRs focused**: Each PR should address a single concern (bug fix, feature, refactor).
- **Provide a clear description**: Explain the "what" and "why" of your changes. Link to relevant issues.
- **Request reviews**: Request reviews from maintainers or other contributors.
- **Be responsive to feedback**: Address comments and questions promptly.
- **Ensure CI checks pass**: All automated checks (tests, linting) should pass before a PR is merged.
- **Rebase your branch**: Before submitting a PR, and before merging, rebase your branch on top of the latest `main` to ensure a clean commit history. Avoid merge commits in your PR branch.

## Community

If you have questions, ideas, or just want to chat about Icebox, you can reach out via [GitHub Issues](https://github.com/TFMV/icebox/issues) or other channels that may be set up in the future (e.g., Discord, Slack).

We look forward to your contributions!
