ARG NODE_IMAGE=node:24.19.0-bookworm-slim@sha256:3638d9a6fe4030bd716be989438248074489337ba3275657f93595428be4fc03

FROM ${NODE_IMAGE} AS build
WORKDIR /app
RUN corepack enable
COPY package.json pnpm-lock.yaml ./
RUN pnpm install --frozen-lockfile
COPY . .
RUN pnpm build
RUN test -f dist/server.js && test -f dist/worker.js && test -f dist/cli/migrate.js \
  && test -f dist/ops/predeploy-production.js && test -f dist/ops/smoke-production.js \
  && test -f dist/public/index.html

FROM ${NODE_IMAGE} AS runtime
WORKDIR /app
ENV NODE_ENV=production \
    NODE_OPTIONS=--enable-source-maps
RUN corepack enable
COPY package.json pnpm-lock.yaml ./
RUN pnpm install --prod --frozen-lockfile
COPY --chown=node:node --from=build /app/dist ./dist
COPY --chown=node:node --from=build /app/migrations ./migrations
USER node
CMD ["node", "dist/server.js"]
