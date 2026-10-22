# Contribute

Contributions are welcome. If you use AI tools, read the [AI policy](../../ai.md) first.

## Process

1. Fork the repository and clone it.
2. Create a branch, for example `feat/my-feature` or `fix/my-bug`.
3. Make the change, with unit tests for new logic. Changes to the cloud backends also need integration tests.
4. Build with `./mvnw verify`. The build formats the code.
5. Open a pull request against `main`.

The [pull request template](https://github.com/tileverse-io/tileverse/blob/main/.github/PULL_REQUEST_TEMPLATE.md) lists what a pull request needs before the merge:

- New unit tests cover the change.
- The documentation is updated, if users can see the change.
- A GitHub issue describes the change, unless users see no difference.
- A bug fix or a small feature is a single commit, and each commit has a single goal.

The pull request is merged when the build checks are green, a maintainer has reviewed it, and the checklist is complete.

## Coding standards

- Target Java 17. The build runs on Java 17 and later.
- Follow the [Palantir Java Format](https://github.com/palantir/palantir-java-format). The build applies it.
- Document the public API with javadoc.
- Return `Optional<T>` for a value that may be missing. Avoid returning `null`.

## Commit messages

Commit titles follow [Conventional Commits](https://www.conventionalcommits.org/):

- `feat: add GCS support`
- `fix: handle EOF correctly in range reader`
- `docs: update usage examples`
- `chore: bump dependencies`

## Releases

GitHub Actions publishes the artifacts:

- Snapshots, on each push to `main` or to a release branch (`*.x`), except for changes to the documentation only.
- Releases, when a version tag such as `v2.1.0` is pushed.
