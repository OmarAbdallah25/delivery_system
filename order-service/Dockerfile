FROM node:18-alpine
WORKDIR /app
COPY order-service/package.json .
RUN npm install
COPY order-service/ .
COPY proto/ ./proto/
ENV PROTO_DIR=/app/proto
EXPOSE 50051
CMD ["node", "index.js"]