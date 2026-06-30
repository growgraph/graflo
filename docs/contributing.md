# Contributing to GraFlo

We welcome contributions to GraFlo! This document provides guidelines and instructions for contributing to the project.

## Getting Started

1. Fork the repository on GitHub
2. Clone your fork locally
3. From the repository root (where `pyproject.toml` lives), install development dependencies:
   ```bash
   uv sync --extra dev
   ```
   Add `--extra docs` if you will build the documentation site locally.
4. Install pre-commit hooks:
   ```bash
   uv run pre-commit install
   ```

## Development Workflow

1. Create a new branch for your feature or bugfix:
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. Make your changes and ensure tests pass:
   ```bash
   uv run pytest test
   ```

3. Commit your changes with a descriptive message:
   ```bash
   git commit -m "Add feature: your feature description"
   ```

4. Push your branch to your fork:
   ```bash
   git push origin feature/your-feature-name
   ```

5. Create a Pull Request on GitHub

## Code Style

- Follow [PEP 8](https://www.python.org/dev/peps/pep-0008/) style guidelines
- Use type hints for all function parameters and return values
- Write docstrings following the Google style
- Keep functions focused and small
- Add tests for new features

## Documentation

- Update relevant documentation when adding new features
- Add docstrings to all new functions and classes
- Include examples in docstrings where appropriate
- Update the changelog for significant changes

To build and preview the docs site locally:

```bash
uv sync --extra docs
uv run mkdocs serve
```

If you edit the GraFlo meta-ontology (`graflo/rdf/ontology/graflo.ttl`), regenerate the interactive visualization and commit the updated assets:

```bash
uv run python docs/_build/scripts/build_ontology_viz.py
```

Visual tweaks and the graph viewer live in repo-owned files under `docs/_build/scripts/ontology_viz/` and are copied into `docs/assets/graflo-ontology-viz/` at build time. **Do not edit packages inside `.venv`.**

CI runs the same script and fails if `docs/assets/graflo-ontology-viz/` is out of date with the committed ontology.

## Testing

- Write tests for all new features
- Ensure all tests pass before submitting a PR
- Add tests for bug fixes
- Maintain or improve test coverage

## Pull Request Process

1. Ensure your PR description clearly describes the problem and solution
2. Include relevant tests
3. Update documentation as needed
4. Ensure all CI checks pass
5. Request review from maintainers

## Reporting Issues

When reporting issues, please include:

- Python version
- GraFlo version
- Steps to reproduce
- Expected behavior
- Actual behavior
- Any relevant error messages
