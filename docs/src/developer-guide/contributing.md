# Contributing

Thank you for your interest in contributing to Tileverse! We welcome contributions from the community to help build the best Java ecosystem for cloud-native geospatial data.

## Code Contribution Process

1.  **Fork & Clone**: Fork the repo and clone it locally.
2.  **Branch**: Create a feature branch (`feat/my-feature` or `fix/my-bug`).
3.  **Code**: Implement your changes.
4.  **Test**: Add unit tests for new logic and ensure existing tests pass.
5.  **Format**: Run `make format` to ensure code style compliance.
6.  **PR**: Submit a Pull Request to the `main` branch.

## Coding Standards

*   **Java Version**: Target Java 17 compatibility, but build with Java 21+.
*   **Style**: We use the [Palantir Java Style](https://github.com/palantir/palantir-java-format).
*   **Javadoc**: Public APIs must be documented.
*   **Null Safety**: Use `Optional<T>` for return types that might be missing. Avoid returning `null`.

## Testing Requirements

*   **Unit Tests**: Required for all new logic.
*   **Integration Tests**: Required for cloud provider interactions (S3, Azure, GCS).
*   **Performance Tests**: Required if modifying critical I/O paths.

## Commit Messages

We follow the [Conventional Commits](https://www.conventionalcommits.org/) specification:

*   `feat: add GCS support`
*   `fix: handle EOF correctly in range reader`
*   `docs: update usage examples`
*   `chore: bump dependencies`

## Release Process

Releases are automated via GitHub Actions.

*   **Snapshots**: Published on every merge to `main`.
*   **Releases**: Published when a new tag (e.g., `v1.0.0`) is pushed.
