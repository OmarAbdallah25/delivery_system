FROM node:18-alpine
WORKDIR /app
COPY driver-service/package.json .
RUN npm install
COPY driver-service/ .
COPY proto/ ./proto/
ENV PROTO_DIR=/app/proto
EXPOSE 50052
CMD ["node", "index.js"]