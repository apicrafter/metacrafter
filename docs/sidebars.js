/**
 * Creating a sidebar enables you to:
 - create an ordered group of docs
 - render a sidebar for each doc of that group
 - provide next/previous navigation
 */

// @ts-check

/** @type {import('@docusaurus/plugin-content-docs').SidebarsConfig} */
const sidebars = {
  docs: [
    {
      type: 'link',
      label: 'Contents',
      href: '/',
    },
    {
      type: 'category',
      label: 'Getting Started',
      items: [
        'getting-started/installation',
        'getting-started/quick-start',
        'getting-started/when-to-use',
        'getting-started/cookbook',
        'getting-started/basic-usage',
        'getting-started/troubleshooting',
        'getting-started/best-practices',
      ],
    },
    {
      type: 'category',
      label: 'Use Cases',
      items: [
        'use-cases/pii-detection',
        'use-cases/file-scanning',
        'use-cases/database-scanning',
        'use-cases/semantic-labeling',
        'use-cases/catalog-export',
      ],
    },
    {
      type: 'category',
      label: 'CLI Reference',
      items: [
        'commands/index',
        'commands/shared-options',
        {
          type: 'category',
          label: 'Scan',
          items: [
            'commands/scan-file',
            'commands/scan-sql',
            'commands/scan-mongodb',
            'commands/scan-bulk',
          ],
        },
        {
          type: 'category',
          label: 'Rules',
          items: ['commands/rules'],
        },
        {
          type: 'category',
          label: 'Server',
          items: ['commands/server'],
        },
        {
          type: 'category',
          label: 'Export',
          items: [
            'commands/export-datahub',
            'commands/export-openmetadata',
            'commands/export-atlas',
          ],
        },
      ],
    },
    {
      type: 'category',
      label: 'Data File Formats',
      items: ['formats/index'],
    },
    {
      type: 'category',
      label: 'Integrations',
      items: [
        'integrations/sdk',
        'integrations/api',
        'integrations/registry',
        'integrations/llm',
        'integrations/rules',
        'integrations/datahub',
        'integrations/openmetadata',
        'integrations/atlas',
      ],
    },
    {
      type: 'category',
      label: 'Development',
      items: [
        'development/contributing',
        'development/architecture',
        'development/community',
      ],
    },
    'comparison',
    'license',
  ],
};

module.exports = sidebars;
