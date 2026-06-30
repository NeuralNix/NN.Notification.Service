FROM node:20-alpine AS builder
WORKDIR /app
RUN apk add --no-cache openssl
COPY package*.json ./
COPY prisma ./prisma
RUN npm ci
COPY . .
RUN npx prisma generate && npm run build

FROM node:20-alpine AS production
WORKDIR /app
RUN apk add --no-cache openssl
COPY package*.json ./
COPY prisma ./prisma
# Keep full deps (incl. prisma CLI) so the schema can self-apply (db push) on start.
RUN npm ci && npx prisma generate
COPY --from=builder /app/dist ./dist
RUN addgroup -g 1001 -S nodejs && adduser -S nodejs -u 1001 && chown -R nodejs:nodejs /app
USER nodejs
ARG PORT=5070
ENV PORT=${PORT}
ENV NODE_ENV=production
EXPOSE ${PORT}
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
  CMD node -e "require('http').get('http://localhost:'+(process.env.PORT||5070)+'/health',(r)=>{process.exit(r.statusCode===200?0:1)})"
CMD ["sh", "-c", "npx prisma db push --skip-generate && node dist/index.js"]
