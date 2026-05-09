FROM node:22-bookworm AS builder

WORKDIR /app

RUN corepack enable && corepack prepare pnpm@9.15.9 --activate

COPY package.json pnpm-lock.yaml ./
RUN pnpm install --frozen-lockfile

COPY tsconfig.server.json ./
COPY server/ ./server/
COPY src/types/report.ts ./src/types/report.ts

RUN pnpm tsc -p tsconfig.server.json

# --- production ---
FROM node:22-bookworm-slim

WORKDIR /app

RUN corepack enable && corepack prepare pnpm@9.15.9 --activate

COPY package.json pnpm-lock.yaml ./
RUN pnpm install --frozen-lockfile --prod

COPY --from=builder /app/dist-server ./dist-server

USER node

EXPOSE 1555

CMD ["node", "dist-server/server/index.js"]
