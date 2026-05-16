FROM node:18-alpine
WORKDIR /app
COPY notification-service/package.json .
RUN npm install
COPY notification-service/ .
COPY proto/ ./proto/
ENV PROTO_DIR=/app/proto
EXPOSE 50053
CMD ["node", "index.js"]