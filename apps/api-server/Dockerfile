# ---- build stage ----
FROM node:20-alpine AS build
WORKDIR /app

# Copy workspace manifests for dependency install
COPY package.json package-lock.json tsconfig.base.json ./
COPY libs/shared/package.json ./libs/shared/package.json
COPY apps/api-server/package.json ./apps/api-server/package.json
COPY apps/consumer-worker/package.json ./apps/consumer-worker/package.json
RUN npm ci

# Copy source and build
COPY libs/shared ./libs/shared
COPY apps/api-server ./apps/api-server

RUN npm run build --workspace=libs/shared
RUN npm run build --workspace=apps/api-server

# ---- runtime stage ----
FROM node:20-alpine AS runtime
WORKDIR /app
ENV NODE_ENV=production

COPY --from=build /app/node_modules ./node_modules
COPY --from=build /app/libs/shared/package.json ./libs/shared/package.json
COPY --from=build /app/libs/shared/dist ./libs/shared/dist
COPY --from=build /app/apps/api-server/package.json ./apps/api-server/package.json
COPY --from=build /app/apps/api-server/dist ./apps/api-server/dist

EXPOSE 3000
CMD ["node", "apps/api-server/dist/main.js"]