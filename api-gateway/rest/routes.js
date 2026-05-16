const express = require('express');
const router = express.Router();

const {
  grpcCreateOrder,
  grpcGetOrder,
  grpcUpdateOrderStatus,
  grpcListOrders,
  grpcDeleteOrder,
  grpcRegisterDriver,
  grpcGetDriver,
  grpcUpdateLocation,
  grpcUpdateAvailability,
  grpcGetAvailableDrivers,
  grpcAssignDriver,
  grpcSendNotification,
  grpcGetNotifications
} = require('../grpc/clients');

// ─── Middleware utilitaire ─────────────────────────────────────────────────────
const handleGrpcError = (res, err) => {
  console.error('[REST] Erreur gRPC:', err.message);
  const status = err.code === 5 ? 404 : err.code === 3 ? 400 : 500;
  res.status(status).json({ error: err.message });
};

// ════════════════════════════════════════════════════════════════════════════════
// ORDERS — Routes REST
// ════════════════════════════════════════════════════════════════════════════════

/**
 * POST /api/orders
 * Créer une nouvelle commande de livraison
 */
router.post('/orders', async (req, res) => {
  try {
    const order = await grpcCreateOrder(req.body);
    res.status(201).json(order);
  } catch (err) {
    handleGrpcError(res, err);
  }
});

/**
 * GET /api/orders/:id
 * Consulter une commande par son ID
 */
router.get('/orders/:id', async (req, res) => {
  try {
    const order = await grpcGetOrder({ order_id: req.params.id });
    res.json(order);
  } catch (err) {
    handleGrpcError(res, err);
  }
});

/**
 * PATCH /api/orders/:id/status
 * Modifier le statut d'une commande
 * Body: { status: "IN_PROGRESS" }
 */
router.patch('/orders/:id/status', async (req, res) => {
  try {
    const order = await grpcUpdateOrderStatus({
      order_id: req.params.id,
      status: req.body.status
    });
    res.json(order);
  } catch (err) {
    handleGrpcError(res, err);
  }
});

/**
 * GET /api/orders?client_id=xxx
 * Lister toutes les commandes d'un client
 */
router.get('/orders', async (req, res) => {
  try {
    const { orders } = await grpcListOrders({ client_id: req.query.client_id || '' });
    res.json(orders);
  } catch (err) {
    handleGrpcError(res, err);
  }
});

/**
 * DELETE /api/orders/:id
 * Annuler / supprimer une commande
 */
router.delete('/orders/:id', async (req, res) => {
  try {
    const result = await grpcDeleteOrder({ order_id: req.params.id });
    res.json(result);
  } catch (err) {
    handleGrpcError(res, err);
  }
});

// ════════════════════════════════════════════════════════════════════════════════
// DRIVERS — Routes REST
// ════════════════════════════════════════════════════════════════════════════════

/**
 * POST /api/drivers
 * Enregistrer un nouveau livreur
 */
router.post('/drivers', async (req, res) => {
  try {
    const driver = await grpcRegisterDriver(req.body);
    res.status(201).json(driver);
  } catch (err) {
    handleGrpcError(res, err);
  }
});

/**
 * GET /api/drivers/available
 * Lister les livreurs disponibles
 */
router.get('/drivers/available', async (req, res) => {
  try {
    const { drivers } = await grpcGetAvailableDrivers({});
    res.json(drivers);
  } catch (err) {
    handleGrpcError(res, err);
  }
});

/**
 * GET /api/drivers/:id
 * Consulter un livreur par son ID
 */
router.get('/drivers/:id', async (req, res) => {
  try {
    const driver = await grpcGetDriver({ driver_id: req.params.id });
    res.json(driver);
  } catch (err) {
    handleGrpcError(res, err);
  }
});

/**
 * PATCH /api/drivers/:id/location
 * Mettre à jour la position GPS du livreur
 * Body: { latitude: 36.8, longitude: 10.1 }
 */
router.patch('/drivers/:id/location', async (req, res) => {
  try {
    const driver = await grpcUpdateLocation({
      driver_id: req.params.id,
      latitude: parseFloat(req.body.latitude),
      longitude: parseFloat(req.body.longitude)
    });
    res.json(driver);
  } catch (err) {
    handleGrpcError(res, err);
  }
});

/**
 * PATCH /api/drivers/:id/availability
 * Modifier la disponibilité du livreur
 * Body: { available: true/false }
 */
router.patch('/drivers/:id/availability', async (req, res) => {
  try {
    const driver = await grpcUpdateAvailability({
      driver_id: req.params.id,
      available: req.body.available
    });
    res.json(driver);
  } catch (err) {
    handleGrpcError(res, err);
  }
});

/**
 * POST /api/drivers/:id/assign
 * Assigner manuellement un livreur à une commande
 * Body: { order_id: "xxx" }
 */
router.post('/drivers/:id/assign', async (req, res) => {
  try {
    const driver = await grpcAssignDriver({
      driver_id: req.params.id,
      order_id: req.body.order_id
    });
    res.json(driver);
  } catch (err) {
    handleGrpcError(res, err);
  }
});

// ════════════════════════════════════════════════════════════════════════════════
// NOTIFICATIONS — Routes REST
// ════════════════════════════════════════════════════════════════════════════════

/**
 * POST /api/notifications
 * Envoyer une notification manuelle
 */
router.post('/notifications', async (req, res) => {
  try {
    const notif = await grpcSendNotification(req.body);
    res.status(201).json(notif);
  } catch (err) {
    handleGrpcError(res, err);
  }
});

/**
 * GET /api/notifications/:recipientId
 * Récupérer les notifications d'un utilisateur
 */
router.get('/notifications/:recipientId', async (req, res) => {
  try {
    const { notifications } = await grpcGetNotifications({ recipient_id: req.params.recipientId });
    res.json(notifications);
  } catch (err) {
    handleGrpcError(res, err);
  }
});

module.exports = router;