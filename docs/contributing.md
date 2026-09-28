# Contributing to GraFlo

We welcome contributions to GraFlo! This document provides guidelines and instructions for contributing to the project.

## Licensing your contribution

GraFlo is released under the Apache License 2.0. Before we can merge your first pull request, you
need to accept the [Growgraph Contributor License Agreement](https://github.com/growgraph/graflo/blob/main/CLA.md)
(CLA). You do it once, and it covers all your future contributions to Growgraph projects. You keep
the copyright in your work, and everything you contribute stays available under an open source
license.

When you open a pull request, a bot checks whether you have accepted the CLA. If you have not, it
comments with a link. Read the agreement and reply on the pull request with:

```
I have read the Growgraph CLA and I accept it.
```

The `cla` check then turns green. You can also sign the agreement on paper: see its section 11.

**Contributing as part of your job?** Contributions written at work are welcome. Many employers own
what their employees write, though, so before you contribute, either get your employer's permission
or ask them to sign our Corporate CLA (write to team@growgraph.dev). If you are not sure whether
your employer has a claim, ask them first.

Do not submit code you did not write without telling us its source and license.

Every commit must be authored with an e-mail address linked to your GitHub account; otherwise the
check cannot match the commit to your acceptance.

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

### Test databases

Most of the suite needs live database containers. From a clone, start them with the scripts under `docker/`:

```bash
cd docker
./start-all.sh    # Start all services
./stop-all.sh     # Stop all services
./cleanup-all.sh  # Remove containers and volumes
```

Per-engine compose files, ports, and env notes are in [`docker/README.md`](https://github.com/growgraph/graflo/blob/main/docker/README.md). Then run:

```bash
uv run pytest test
```

NebulaGraph tests are gated behind `pytest --run-nebula`. CI intentionally skips the database suite.

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
