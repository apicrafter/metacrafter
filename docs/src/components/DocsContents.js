import React from 'react';
import Link from '@docusaurus/Link';
import styles from './DocsContents.module.css';

const sections = [
  {
    title: 'Getting Started',
    to: '/getting-started/installation',
    description: 'Install Metacrafter and complete a first file, database, or PII scan.',
    links: [
      {label: 'Installation', to: '/getting-started/installation'},
      {label: 'Quick start', to: '/getting-started/quick-start'},
      {label: 'When to use', to: '/getting-started/when-to-use'},
      {label: 'Cookbook', to: '/getting-started/cookbook'},
      {label: 'Basic usage', to: '/getting-started/basic-usage'},
      {label: 'Troubleshooting', to: '/getting-started/troubleshooting'},
      {label: 'Best practices', to: '/getting-started/best-practices'},
    ],
  },
  {
    title: 'Use Cases',
    to: '/use-cases/pii-detection',
    description: 'End-to-end examples for PII, files, databases, semantic types, and catalogs.',
    links: [
      {label: 'PII detection', to: '/use-cases/pii-detection'},
      {label: 'File scanning', to: '/use-cases/file-scanning'},
      {label: 'Database scanning', to: '/use-cases/database-scanning'},
      {label: 'Semantic labeling', to: '/use-cases/semantic-labeling'},
      {label: 'Catalog export', to: '/use-cases/catalog-export'},
    ],
  },
  {
    title: 'CLI Reference',
    to: '/commands/',
    description: 'Command-by-command reference for scan, rules, server, and export.',
    links: [
      {label: 'All commands', to: '/commands/'},
      {label: 'Shared options', to: '/commands/shared-options'},
      {label: 'scan file', to: '/commands/scan-file'},
      {label: 'scan sql', to: '/commands/scan-sql'},
      {label: 'rules list', to: '/commands/rules'},
      {label: 'server run', to: '/commands/server'},
      {label: 'export datahub', to: '/commands/export-datahub'},
    ],
  },
  {
    title: 'Data File Formats',
    to: '/formats/',
    description: 'File formats and compression codecs supported via iterabledata.',
    links: [
      {label: 'Format support', to: '/formats/'},
      {label: 'scan file', to: '/commands/scan-file'},
    ],
  },
  {
    title: 'Integrations',
    to: '/integrations/sdk',
    description: 'Python API, REST server, registry, LLM/RAG, rules packs, and catalogs.',
    links: [
      {label: 'Python SDK', to: '/integrations/sdk'},
      {label: 'API server', to: '/integrations/api'},
      {label: 'Registry', to: '/integrations/registry'},
      {label: 'LLM classification', to: '/integrations/llm'},
      {label: 'Custom rules', to: '/integrations/rules'},
      {label: 'DataHub', to: '/integrations/datahub'},
    ],
  },
  {
    title: 'Development',
    to: '/development/contributing',
    description: 'Contributing, architecture, comparison with similar tools, and license.',
    links: [
      {label: 'Contributing', to: '/development/contributing'},
      {label: 'Architecture', to: '/development/architecture'},
      {label: 'Community', to: '/development/community'},
      {label: 'Comparison', to: '/comparison'},
      {label: 'License', to: '/license'},
    ],
  },
];

function Section({title, to, description, links}) {
  return (
    <article className={styles.card}>
      <h3 className={styles.cardTitle}>
        <Link to={to}>{title}</Link>
      </h3>
      <p className={styles.cardDescription}>{description}</p>
      <ul className={styles.linkList}>
        {links.map((item) => (
          <li key={item.label}>
            {item.href ? (
              <a href={item.href}>{item.label}</a>
            ) : (
              <Link to={item.to}>{item.label}</Link>
            )}
          </li>
        ))}
      </ul>
    </article>
  );
}

export default function DocsContents() {
  return (
    <section className={styles.contents}>
      <div className="container">
        <h2 className={styles.heading}>Documentation contents</h2>
        <p className={styles.intro}>
          Start with a section below, or use the sidebar from any page. The CLI
          entry point is <code>metacrafter</code>.
        </p>
        <div className={styles.grid}>
          {sections.map((section) => (
            <Section key={section.title} {...section} />
          ))}
        </div>
      </div>
    </section>
  );
}
