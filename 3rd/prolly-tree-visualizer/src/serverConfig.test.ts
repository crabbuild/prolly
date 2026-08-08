import { describe, expect, it } from 'vitest';
import type { UserConfig } from 'vite';
import viteConfig from '../vite.config';

describe('Vite development server', () => {
  it('serves the linked prolly WASM package from the repository workspace', () => {
    const repositoryRoot = decodeURIComponent(new URL('../../..', import.meta.url).pathname).replace(/\/$/, '');
    const config = viteConfig as UserConfig;

    expect(config.server?.fs?.allow).toContain(repositoryRoot);
  });
});
