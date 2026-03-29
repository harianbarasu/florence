# Florence Web

Minimal Florence control-plane app.

This app is not the primary Florence product surface. Daily usage stays in iMessage via Linq.
The web app only owns:

- first-time onboarding
- Google account connection
- initial sync readiness
- extra Google accounts
- lightweight settings and later billing

## Local development

```bash
cd web
cp .env.example .env.local
pnpm install
pnpm dev
```

Required env vars:

```bash
FLORENCE_API_BASE_URL=http://127.0.0.1:8081
AUTH_SECRET=replace-me
GOOGLE_CLIENT_ID=...
GOOGLE_CLIENT_SECRET=...
NEXTAUTH_URL=http://localhost:3000
```

The Python Florence backend should also set:

```bash
FLORENCE_WEB_BASE_URL=http://localhost:3000
```

That causes DM onboarding links and the Google callback redirect to land in this Next app's `Setup` flow.
