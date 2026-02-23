import { defineConfig, defineDocs } from 'fumadocs-mdx/config';
import { remarkMdxMermaid } from 'fumadocs-core/mdx-plugins';
import type { Root } from 'mdast';
import type { Transformer } from 'unified';

function remarkMdxMermaidForMdx(): Transformer<Root, Root> {
  const transform = remarkMdxMermaid();

  return (tree, file) => {
    const path = file.path?.toLowerCase();
    if (!path?.endsWith('.mdx')) return;

    return transform(tree, file, () => {});
  };
}

export const docs = defineDocs({
  dir: 'content/docs',
});

export default defineConfig({
  mdxOptions: {
    remarkPlugins: [remarkMdxMermaidForMdx],
  },
});
