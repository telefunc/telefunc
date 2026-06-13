import { prepare } from './prepare'
import { expect, describe, it } from 'vitest'

describe('prepare()', () => {
  it('fixture', async () => {
    const jobs = await prepare()
    expect(jobs).toMatchInlineSnapshot(`
      [
        {
          "jobCmd": "pnpm run test:units",
          "jobName": "Vitest",
          "jobSetups": [
            {
              "node_version": "23",
              "os": "ubuntu-latest",
            },
          ],
          "jobTests": null,
        },
        {
          "jobCmd": "pnpm run test:types",
          "jobName": "TypeScript",
          "jobSetups": [
            {
              "node_version": "23",
              "os": "ubuntu-latest",
            },
          ],
          "jobTests": null,
        },
        {
          "jobCmd": "pnpm exec test-e2e",
          "jobName": "Vite",
          "jobSetups": [
            {
              "node_version": "23",
              "os": "ubuntu-latest",
            },
            {
              "node_version": "23",
              "os": "windows-latest",
            },
          ],
          "jobTests": [
            {
              "localConfig": {
                "ci": {
                  "job": "Vite",
                },
              },
              "testFilePath": "examples/authentication/.dev.test.ts",
            },
            {
              "localConfig": {
                "ci": {
                  "job": "Vite",
                },
              },
              "testFilePath": "examples/authentication/.prod.test.ts",
            },
            {
              "localConfig": {
                "ci": {
                  "job": "Vite",
                },
              },
              "testFilePath": "examples/react-streaming/.test-dev.test.ts",
            },
            {
              "localConfig": {
                "ci": {
                  "job": "Vite",
                },
              },
              "testFilePath": "examples/react-streaming/.test-prod.test.ts",
            },
            {
              "localConfig": {
                "ci": {
                  "job": "Vite",
                },
              },
              "testFilePath": "examples/vike/.test-dev.test.ts",
            },
            {
              "localConfig": {
                "ci": {
                  "job": "Vite",
                },
              },
              "testFilePath": "examples/vike/.test-preview.test.ts",
            },
            {
              "localConfig": {
                "ci": {
                  "job": "Vite",
                },
              },
              "testFilePath": "test/playground-streaming/.test-dev.test.ts",
            },
            {
              "localConfig": {
                "ci": {
                  "job": "Vite",
                },
              },
              "testFilePath": "test/playground-streaming/.test-preview.test.ts",
            },
          ],
        },
        {
          "jobCmd": "pnpm exec test-e2e",
          "jobName": "React Native",
          "jobSetups": [
            {
              "node_version": "23",
              "os": "ubuntu-latest",
            },
            {
              "node_version": "23",
              "os": "windows-latest",
            },
          ],
          "jobTests": [
            {
              "localConfig": {
                "ci": {
                  "job": "React Native",
                },
              },
              "testFilePath": "examples/babel/.test.ts",
            },
          ],
        },
        {
          "jobCmd": "pnpm exec test-e2e",
          "jobName": "Cloudflare Workers",
          "jobSetups": [
            {
              "node_version": "23",
              "os": "ubuntu-latest",
            },
          ],
          "jobTests": [
            {
              "localConfig": {
                "ci": {
                  "job": "Cloudflare Workers",
                },
              },
              "testFilePath": "examples/cloudflare-workers/.dev.test.ts",
            },
            {
              "localConfig": {
                "ci": {
                  "job": "Cloudflare Workers",
                },
              },
              "testFilePath": "examples/cloudflare-workers/.test-miniflare.test.ts",
            },
            {
              "localConfig": {
                "ci": {
                  "job": "Cloudflare Workers",
                },
              },
              "testFilePath": "examples/cloudflare-workers/.test-wrangler.test.ts",
            },
          ],
        },
        {
          "jobCmd": "pnpm exec test-e2e",
          "jobName": "Next.js",
          "jobSetups": [
            {
              "node_version": "23",
              "os": "ubuntu-latest",
            },
            {
              "node_version": "23",
              "os": "windows-latest",
            },
          ],
          "jobTests": [
            {
              "localConfig": {
                "ci": {
                  "job": "Next.js",
                },
              },
              "testFilePath": "examples/next/.dev.test.ts",
            },
            {
              "localConfig": {
                "ci": {
                  "job": "Next.js",
                },
              },
              "testFilePath": "examples/next/.prod.test.ts",
            },
          ],
        },
        {
          "jobCmd": "pnpm exec test-e2e",
          "jobName": "SvelteKit",
          "jobSetups": [
            {
              "node_version": "23",
              "os": "ubuntu-latest",
            },
            {
              "node_version": "23",
              "os": "windows-latest",
            },
          ],
          "jobTests": [
            {
              "localConfig": {
                "ci": {
                  "job": "SvelteKit",
                },
              },
              "testFilePath": "examples/svelte-kit/.test-dev.test.ts",
            },
            {
              "localConfig": {
                "ci": {
                  "job": "SvelteKit",
                },
              },
              "testFilePath": "examples/svelte-kit/.test-preview.test.ts",
            },
          ],
        },
        {
          "jobCmd": "pnpm exec test-e2e",
          "jobName": "https://telefunc.com",
          "jobSetups": [
            {
              "node_version": "23",
              "os": "ubuntu-latest",
            },
          ],
          "jobTests": [
            {
              "localConfig": {
                "ci": {
                  "job": "https://telefunc.com",
                },
              },
              "testFilePath": "docs/.test-dev.test.ts",
            },
            {
              "localConfig": {
                "ci": {
                  "job": "https://telefunc.com",
                },
              },
              "testFilePath": "docs/.test-preview.test.ts",
            },
          ],
        },
      ]
    `)
  })
})
