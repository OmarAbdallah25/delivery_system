const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');
const util = require('util');

const PROTO_DIR = process.env.PROTO_DIR || path.join(__dirname, '../../proto');

const loaderOptions = {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true
};

// ─── Chargement des proto files ───────────────────────────────────────────────
const orderPkg  = grpc.loadPackageDefinition(protoLoader.loadSync(path.join(PROTO_DIR, 'order.proto'),        loaderOptions)).order;
const driverPkg = grpc.loadPackageDefinition(protoLoader.loadSync(path.join(PROTO_DIR, 'driver.proto'),       loaderOptions)).driver;
const notifPkg  = grpc.loadPackageDefinition(protoLoader.loadSync(path.join(PROTO_DIR, 'notification.proto'), loaderOptions)).notification;

// ─── Création des clients gRPC (connexion aux microservices) ──────────────────
const orderClient = new orderPkg.OrderService(
  process.env.ORDER_SERVICE_ADDR || 'localhost:50051',
  grpc.credentials.createInsecure()
);

const driverClient = new driverPkg.DriverService(
  process.env.DRIVER_SERVICE_ADDR || 'localhost:50052',
  grpc.credentials.createInsecure()
);

const notifClient = new notifPkg.NotificationService(
  process.env.NOTIF_SERVICE_ADDR || 'localhost:50053',
  grpc.credentials.createInsecure()
);

// ─── Promisification des méthodes gRPC (pour utilisation async/await) ─────────
// Order Service
const grpcCreateOrder       = util.promisify(orderClient.createOrder.bind(orderClient));
const grpcGetOrder          = util.promisify(orderClient.getOrder.bind(orderClient));
const grpcUpdateOrderStatus = util.promisify(orderClient.updateOrderStatus.bind(orderClient));
const grpcListOrders        = util.promisify(orderClient.listOrders.bind(orderClient));
const grpcDeleteOrder       = util.promisify(orderClient.deleteOrder.bind(orderClient));

// Driver Service
const grpcRegisterDriver       = util.promisify(driverClient.registerDriver.bind(driverClient));
const grpcGetDriver            = util.promisify(driverClient.getDriver.bind(driverClient));
const grpcUpdateLocation       = util.promisify(driverClient.updateLocation.bind(driverClient));
const grpcUpdateAvailability   = util.promisify(driverClient.updateAvailability.bind(driverClient));
const grpcGetAvailableDrivers  = util.promisify(driverClient.getAvailableDrivers.bind(driverClient));
const grpcAssignDriver         = util.promisify(driverClient.assignDriver.bind(driverClient));

// Notification Service
const grpcSendNotification  = util.promisify(notifClient.sendNotification.bind(notifClient));
const grpcGetNotifications  = util.promisify(notifClient.getNotifications.bind(notifClient));

module.exports = {
  // Orders
  grpcCreateOrder,
  grpcGetOrder,
  grpcUpdateOrderStatus,
  grpcListOrders,
  grpcDeleteOrder,
  // Drivers
  grpcRegisterDriver,
  grpcGetDriver,
  grpcUpdateLocation,
  grpcUpdateAvailability,
  grpcGetAvailableDrivers,
  grpcAssignDriver,
  // Notifications
  grpcSendNotification,
  grpcGetNotifications
};