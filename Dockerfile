ARG NODE_IMAGE=node:24.19.0-bookworm-slim@sha256:3638d9a6fe4030bd716be989438248074489337ba3275657f93595428be4fc03

FROM ${NODE_IMAGE} AS build
WORKDIR /app
RUN corepack enable
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
COPY apps/api/package.json apps/api/package.json
COPY apps/web/package.json apps/web/package.json
COPY apps/worker/package.json apps/worker/package.json
COPY packages/artifacts/package.json packages/artifacts/package.json
COPY packages/contracts/package.json packages/contracts/package.json
COPY packages/control-plane/package.json packages/control-plane/package.json
COPY packages/database/package.json packages/database/package.json
COPY packages/google/package.json packages/google/package.json
COPY packages/linq/package.json packages/linq/package.json
COPY packages/runtime/package.json packages/runtime/package.json
RUN pnpm install --frozen-lockfile
COPY . .
RUN pnpm build
RUN find apps packages -path '*/dist/*' -type f \( -name '*.test.*' -o -name '*.integration.test.*' \) -delete
RUN test -f apps/api/dist/server.js \
  && test -f apps/api/dist/smoke-production.js \
  && test -f apps/worker/dist/server.js \
  && test -f apps/web/dist/index.html \
  && test -f packages/database/dist/predeploy.js

FROM ${NODE_IMAGE} AS runtime
WORKDIR /app
ENV NODE_ENV=production \
    NODE_OPTIONS=--enable-source-maps
RUN corepack enable
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
COPY apps/api/package.json apps/api/package.json
COPY apps/web/package.json apps/web/package.json
COPY apps/worker/package.json apps/worker/package.json
COPY packages/artifacts/package.json packages/artifacts/package.json
COPY packages/contracts/package.json packages/contracts/package.json
COPY packages/control-plane/package.json packages/control-plane/package.json
COPY packages/database/package.json packages/database/package.json
COPY packages/google/package.json packages/google/package.json
COPY packages/linq/package.json packages/linq/package.json
COPY packages/runtime/package.json packages/runtime/package.json
RUN pnpm install --prod --frozen-lockfile
COPY --chown=node:node --from=build /app/apps/api/dist ./apps/api/dist
COPY --chown=node:node --from=build /app/apps/web/dist ./apps/web/dist
COPY --chown=node:node --from=build /app/apps/worker/dist ./apps/worker/dist
COPY --chown=node:node --from=build /app/packages/artifacts/dist ./packages/artifacts/dist
COPY --chown=node:node --from=build /app/packages/contracts/dist ./packages/contracts/dist
COPY --chown=node:node --from=build /app/packages/control-plane/dist ./packages/control-plane/dist
COPY --chown=node:node --from=build /app/packages/database/dist ./packages/database/dist
COPY --chown=node:node --from=build /app/packages/google/dist ./packages/google/dist
COPY --chown=node:node --from=build /app/packages/linq/dist ./packages/linq/dist
COPY --chown=node:node --from=build /app/packages/runtime/dist ./packages/runtime/dist
COPY --chown=node:node --from=build /app/migrations ./migrations
USER node
CMD ["node", "apps/api/dist/server.js"]
