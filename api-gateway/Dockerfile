FROM node:18-alpine
WORKDIR /app
COPY api-gateway/package.json .
RUN npm install
COPY api-gateway/ .
COPY proto/ ./proto/
ENV PROTO_DIR=/app/proto
EXPOSE 3000
CMD ["node", "index.js"]
 