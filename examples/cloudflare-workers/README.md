Example of deploying telefunctions to [Cloudflare Workers](https://workers.cloudflare.com/).

Setup:
```bash
git clone git@github.com:telefunc/telefunc
cd telefunc/examples/cloudflare-workers/
npm install
```

To develop: (For increased development speed, we use an Express.js development server instead of a worker.)
```bash
npm run dev
```

To try the worker locally with miniflare: (No account needed.)
```bash
npm run preview
```

To be able to use `wrangler`, create a Cloudflare account and paste your account id in `wrangler.toml#account_id`.

To try the worker locally with wrangler:
```bash
npm run preview:wrangler
```

To deploy the worker to Cloudflare:
```bash
npm run deploy
```
