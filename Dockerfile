FROM mcr.microsoft.com/playwright:v1.63.0-noble

WORKDIR /app

ENV NODE_ENV=production \
    PORT=4100 \
    HEADLESS=true \
    SLOW_MO_MS=0 \
    KEEP_BROWSER_OPEN_MS=0 \
    FETCH_WINDOW_CONCURRENCY=3 \
    PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1

COPY package.json package-lock.json ./
RUN npm ci --omit=dev

COPY tsconfig.json ./
COPY src ./src

EXPOSE 4100
CMD ["npx", "tsx", "src/index.ts"]
