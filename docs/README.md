# metacrafter documentation

This directory contains the Docusaurus documentation site for Metacrafter.

## Development

### Prerequisites

- Node.js 18+ and npm

### Installation

```bash
cd docs
npm install
```

### Local development

Start the development server:

```bash
npm start
```

This starts a local development server and opens a browser window. Most changes
are reflected live without restarting the server.

### Build

Build the site for production:

```bash
npm run build
```

This generates static content into the `build` directory.

### Serve

Serve the built site locally:

```bash
npm run serve
```

## Project structure

```
docs/
├── docusaurus.config.js    # Docusaurus configuration
├── sidebars.js             # Sidebar navigation
├── package.json            # Node.js dependencies
├── babel.config.js         # Babel configuration
├── src/
│   ├── css/custom.css      # Custom styles
│   ├── pages/index.js      # Homepage (documentation contents)
│   └── components/         # React components
├── static/img/             # Logo and favicon
└── docs/                   # Documentation content
    ├── getting-started/    # Installation, quick start, cookbook
    ├── use-cases/          # End-to-end examples
    ├── commands/           # CLI reference
    ├── formats/            # Format support
    ├── integrations/       # SDK, API, registry, LLM, catalogs
    ├── development/        # Contributing and internals
    ├── comparison.md
    └── license.md
```

## Deployment

The documentation is prepared for GitHub Pages at
[apicrafter.github.io/metacrafter](https://apicrafter.github.io/metacrafter/)
when changes are pushed to `main` or `master`. The workflow lives in
`.github/workflows/deploy-docs.yml`.

### GitHub Pages setup

1. Open the repository settings on GitHub.
2. Navigate to **Pages**.
3. Under **Source**, select **GitHub Actions**.

See `GITHUB_PAGES_SETUP.md` for details.

## Documentation structure

- **Getting Started**: Installation, quick start, positioning, cookbook
- **Use Cases**: PII detection, files, databases, semantic labeling, catalog export
- **CLI Reference**: Command-by-command documentation
- **Data File Formats**: Formats and compression codecs via iterabledata
- **Integrations**: SDK, API server, registry, LLM, rules, DataHub, OpenMetadata, Atlas
- **Development**: Contributing, architecture, community

## Contributing

When adding or updating documentation:

1. Edit the markdown files in `docs/docs/`.
2. Follow the existing frontmatter (`title`, `description`).
3. Test locally with `npm start`.
4. Confirm `npm run build` succeeds (broken links fail the build).
