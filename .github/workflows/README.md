# GitHub Workflows

This directory contains GitHub Actions workflows for the Tileverse Range Reader project.

## Documentation Workflows

### 📚 `docs.yml` - Main Documentation Workflow

**Triggers**: Push to `main`/`develop`, PRs affecting docs

**What it does**:
- Builds MkDocs documentation on every push and PR
- Validates MkDocs configuration and builds
- Deploys to GitHub Pages (main branch only)
- Provides build artifacts for PRs
- Validates navigation structure and links

**Environments**:
- **Build**: Validates and builds documentation
- **Deploy**: Deploys to GitHub Pages (main branch only)
- **Validate**: Runs additional checks on PRs

### 🔧 `docs-maintenance.yml` - Documentation Health Checks

**Triggers**: Weekly schedule (Mondays), manual dispatch

**What it does**:
- Comprehensive link checking (internal and external)
- Documentation completeness analysis
- Dependency security scanning
- Generates documentation metrics and health reports
- Creates issues for broken links (scheduled runs only)

**Features**:
- External link validation (configurable)
- Markdown syntax validation
- Documentation coverage metrics
- Automated issue creation for maintenance

### 🔍 `docs-preview.yml` - PR Preview and Comparison

**Triggers**: PRs affecting docs, manual dispatch

**What it does**:
- Builds documentation previews for PRs
- Compares changes between base and PR branches
- Provides downloadable preview artifacts
- Posts detailed PR comments with preview links and change analysis
- Configurable for integration with preview deployment services

**Features**:
- Side-by-side change analysis
- Preview artifact generation
- Automated PR comments with preview information
- Support for external preview deployment (Netlify, Surge.sh, etc.)

## Workflow Integration

### GitHub Pages Setup

To enable GitHub Pages deployment:

1. Go to repository **Settings** → **Pages**
2. Set **Source** to "GitHub Actions"
3. The main workflow will automatically deploy to GitHub Pages on `main` branch pushes

### Preview Deployments

The preview workflow is configured to work with external preview services:

- **Netlify**: Add deploy keys and configure branch previews
- **Surge.sh**: Add `SURGE_TOKEN` secret for automated deployments
- **Custom server**: Modify the deployment step to push to your server

### Required Secrets

For full functionality, configure these repository secrets:

- `SURGE_TOKEN`: For Surge.sh preview deployments (optional)
- Additional secrets as needed for your preview deployment service

## Manual Workflow Triggers

### Documentation Maintenance

```bash
# Trigger health check with external link validation
gh workflow run docs-maintenance.yml -f check_external_links=true
```

### Branch Previews

```bash
# Generate preview for specific branch
gh workflow run docs-preview.yml -f branch_name=feature/new-docs
```

## Customization

### Link Checking

Modify the `linkchecker` parameters in `docs-maintenance.yml`:

```yaml
# Check only internal links (faster)
linkchecker --no-warnings --check-extern=0 http://localhost:8000

# Check external links with custom timeout
linkchecker --no-warnings --timeout=30 --threads=5 http://localhost:8000
```

### Documentation Metrics

Customize the metrics calculation in `docs-maintenance.yml` by modifying:

- File size thresholds for coverage calculation
- Sections to include in metrics
- Badge generation logic

### Preview Configuration

Customize preview builds by modifying:

- Preview URL patterns
- Preview banner messages
- Deployment targets
- Artifact retention periods

## Monitoring

### Workflow Status

Monitor workflow health through:

- GitHub Actions tab for run status
- Issues automatically created for health check failures
- Email notifications (configure in repository settings)

### Documentation Health

The maintenance workflow provides:

- Weekly health check reports
- Link validation results
- Documentation coverage metrics
- Dependency security scans

## Troubleshooting

### Common Issues

**Build Failures**:
- Check MkDocs configuration syntax
- Verify all navigation items reference existing files
- Ensure Python dependencies are up to date

**Link Check Failures**:
- Review external link accessibility
- Check for typos in internal links
- Verify relative path correctness

**Preview Issues**:
- Ensure PR branch is up to date
- Check artifact upload/download permissions
- Verify preview service configuration

### Debug Mode

Enable debug output by adding to workflow files:

```yaml
- name: Debug step
  run: |
    set -x  # Enable debug output
    # Your commands here
```

## Contributing

When modifying workflows:

1. Test changes on a fork first
2. Use minimal permissions principle
3. Add appropriate error handling
4. Document any new secrets or configuration required
5. Update this README with changes

## Security

### Permissions

Workflows use minimal required permissions:

- `contents: read` - Read repository contents
- `pages: write` - Deploy to GitHub Pages
- `id-token: write` - OIDC token for Pages deployment

### Secrets

Never hardcode sensitive information in workflows. Use repository secrets for:

- Deployment tokens
- API keys
- Service credentials

### External Dependencies

Workflows pin action versions for security:

```yaml
- uses: actions/checkout@v7  # Pinned to major version
- uses: actions/setup-python@v7  # Pinned to major version
```

Review and update dependencies regularly.