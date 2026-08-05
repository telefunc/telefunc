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
          "splitFiles": false,
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
          "splitFiles": false,
        },
        {
          "jobCmd": "pnpm run test:released-api",
          "jobName": "Released API",
          "jobSetups": [
            {
              "node_version": "23",
              "os": "ubuntu-latest",
            },
          ],
          "jobTests": null,
          "splitFiles": false,
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
              "testFilePath": "test/playground/.test-dev.test.ts",
            },
            {
              "localConfig": {
                "ci": {
                  "job": "Vite",
                },
              },
              "testFilePath": "test/playground/.test-preview.test.ts",
            },
          ],
          "splitFiles": false,
        },
        {
          "jobCmd": "pnpm exec test-e2e",
          "jobName": "Playground Stream",
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
                  "job": "Playground Stream",
                },
              },
              "testFilePath": "test/playground-stream/.test-dev.binary-inline.sse.test.ts",
            },
            {
              "localConfig": {
                "ci": {
                  "job": "Playground Stream",
                },
              },
              "testFilePath": "test/playground-stream/.test-dev.binary-inline.ws.test.ts",
            },
            {
              "localConfig": {
                "ci": {
                  "job": "Playground Stream",
                },
              },
              "testFilePath": "test/playground-stream/.test-dev.channel.sse.test.ts",
            },
            {
              "localConfig": {
                "ci": {
                  "job": "Playground Stream",
                },
              },
              "testFilePath": "test/playground-stream/.test-dev.channel.ws.test.ts",
            },
            {
              "localConfig": {
                "ci": {
                  "job": "Playground Stream",
                },
              },
              "testFilePath": "test/playground-stream/.test-dev.sse-inline.sse.test.ts",
            },
            {
              "localConfig": {
                "ci": {
                  "job": "Playground Stream",
                },
              },
              "testFilePath": "test/playground-stream/.test-dev.sse-inline.ws.test.ts",
            },
            {
              "localConfig": {
                "ci": {
                  "job": "Playground Stream",
                },
              },
              "testFilePath": "test/playground-stream/.test-docker.binary-inline.sse.test.ts",
            },
            {
              "localConfig": {
                "ci": {
                  "job": "Playground Stream",
                },
              },
              "testFilePath": "test/playground-stream/.test-docker.redis-cluster.test.ts",
            },
            {
              "localConfig": {
                "ci": {
                  "job": "Playground Stream",
                },
              },
              "testFilePath": "test/playground-stream/.test-docker.redis-room.test.ts",
            },
            {
              "localConfig": {
                "ci": {
                  "job": "Playground Stream",
                },
              },
              "testFilePath": "test/playground-stream/.test-preview.binary-inline.sse.test.ts",
            },
            {
              "localConfig": {
                "ci": {
                  "job": "Playground Stream",
                },
              },
              "testFilePath": "test/playground-stream/.test-preview.binary-inline.ws.test.ts",
            },
            {
              "localConfig": {
                "ci": {
                  "job": "Playground Stream",
                },
              },
              "testFilePath": "test/playground-stream/.test-preview.channel.sse.test.ts",
            },
            {
              "localConfig": {
                "ci": {
                  "job": "Playground Stream",
                },
              },
              "testFilePath": "test/playground-stream/.test-preview.channel.ws.test.ts",
            },
            {
              "localConfig": {
                "ci": {
                  "job": "Playground Stream",
                },
              },
              "testFilePath": "test/playground-stream/.test-preview.sse-inline.sse.test.ts",
            },
            {
              "localConfig": {
                "ci": {
                  "job": "Playground Stream",
                },
              },
              "testFilePath": "test/playground-stream/.test-preview.sse-inline.ws.test.ts",
            },
          ],
          "splitFiles": true,
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
          "splitFiles": false,
        },
        {
          "jobCmd": "pnpm exec test-e2e",
          "jobName": "Cloudflare",
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
                  "job": "Cloudflare",
                },
              },
              "testFilePath": "examples/cloudflare-workers/.test-dev.test.ts",
            },
            {
              "localConfig": {
                "ci": {
                  "job": "Cloudflare",
                },
              },
              "testFilePath": "examples/cloudflare-workers/.test-wrangler.test.ts",
            },
            {
              "localConfig": {
                "ci": {
                  "job": "Cloudflare",
                },
              },
              "testFilePath": "test/@cloudflare_vite-plugin/emitted-modules.test.ts",
            },
            {
              "localConfig": {
                "ci": {
                  "job": "Cloudflare",
                },
              },
              "testFilePath": "test/@cloudflare_vite-plugin/test-dev.test.ts",
            },
            {
              "localConfig": {
                "ci": {
                  "job": "Cloudflare",
                },
              },
              "testFilePath": "test/@cloudflare_vite-plugin/test-preview.test.ts",
            },
          ],
          "splitFiles": false,
        },
        {
          "jobCmd": "pnpm exec vitest run --config vitest.cloudflare-room.config.ts",
          "jobName": "Cloudflare Room",
          "jobSetups": [
            {
              "node_version": "23",
              "os": "ubuntu-latest",
            },
          ],
          "jobTests": null,
          "splitFiles": false,
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
          "splitFiles": false,
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
          "splitFiles": false,
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
          "splitFiles": false,
        },
      ]
    `)
  })
})
