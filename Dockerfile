# Stage 1: Build
FROM node:20-alpine AS builder

WORKDIR /build

# Copy package files
COPY package.json yarn.lock ./

# Install dependencies
RUN yarn install

# Copy source code
COPY . .

# Build the frontend
RUN yarn build

# Stage 2: Runtime
FROM node:20-alpine

WORKDIR /app

# Install runtime dependencies
# express: for auth_server and main server
# ws: for main server
RUN npm install express ws

# Copy built assets from builder
COPY --from=builder /build/dist /app/dist

# Copy Auth Server
COPY --from=builder /build/auth_server /app/auth_server

# Copy Main Server
COPY server /app/server

# Copy Entrypoint
COPY docker-entrypoint.sh /app/docker-entrypoint.sh
RUN chmod +x /app/docker-entrypoint.sh

# Expose ports
# 8080: Main Server (UI + Proxy)
# 9001: Auth Server
EXPOSE 8080 9001

# Set environment variables if needed
ENV NODE_ENV=production

# Start services
ENTRYPOINT ["/app/docker-entrypoint.sh"]
