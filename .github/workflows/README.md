# GitHub Workflows

| Workflow | Runs on | Does |
|---|---|---|
| `pr-validation.yml` | PRs and pushes to `main`, `develop`, `*.x`; skipped for docs and Markdown-only changes | `make lint`, then build and unit tests on Java 17 (Linux, Windows, macOS) and Java 21 and 25 (Linux), then `./mvnw verify -Dcoverage` on Java 17, 21 and 25 |
| `integration.yml` | PRs to `main`; manual, with optional tileverse version, GeoTools ref and GeoServer ref | Installs this branch, then verifies GeoTools `gt-pmtiles` and GeoServer `gs-pmtiles-store` against it |
| `publish-snapshot.yml` | Pushes to `main` and `*.x` unless the commit message says `[skip-publish]`; manual | `./mvnw clean verify`, then a signed snapshot deploy to Maven Central |
| `publish-release.yml` | `v*` tags; manual, with the version | Verifies and deploys the signed release built with `-Drevision`, then creates the GitHub release |
| `docs.yml` | Pushes to `main` touching `docs/**`; manual | Builds the MkDocs site and publishes it to GitHub Pages |
| `backport.yaml` | A merged PR with a `backport ...` label | Opens the backport PR on the target branch |

Secrets: `GPG_PRIVATE_KEY` and `GPG_PASSPHRASE` sign the artifacts, `CENTRAL_USERNAME` and `CENTRAL_TOKEN` authenticate to Maven Central, `GH_TOKEN_BOT` lets the backport bot open PRs. The docs deploy uses the default `GITHUB_TOKEN`.
