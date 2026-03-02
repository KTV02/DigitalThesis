# syntax=docker/dockerfile:1.6
FROM node:20-alpine AS build
WORKDIR /app

# 1) Dependencies layer (stable)
COPY package.json package-lock.json ./
RUN --mount=type=cache,target=/root/.npm \
    npm ci

# 2) Build layer (changes often)
COPY tsconfig.json tsconfig.node.json vite.config.ts index.html ./
COPY src ./src
RUN npm run build

FROM node:20-alpine
WORKDIR /app
RUN npm install --global serve
COPY --from=build /app/dist ./dist
EXPOSE 5173
CMD ["serve", "-s", "dist", "-l", "5173"]