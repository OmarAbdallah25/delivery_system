const { gql } = require('apollo-server-express');

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
  grpcAssignDriver,
  grpcGetAvailableDrivers,
  grpcGetNotifications
} = require('../grpc/clients');

// ─── Schéma GraphQL ────────────────────────────────────────────────────────────
const typeDefs = gql`

  """Représente une commande de livraison"""
  type Order {
    order_id:         ID!
    client_id:        String!
    client_name:      String!
    pickup_address:   String!
    delivery_address: String!
    items:            [String]
    status:           String!
    driver_id:        String
    created_at:       String
  }

  """Représente un livreur avec sa position GPS"""
  type Driver {
    driver_id:        ID!
    name:             String!
    phone:            String!
    vehicle:          String!
    available:        Boolean!
    latitude:         Float
    longitude:        Float
    current_order_id: String
    created_at:       String
  }

  """Représente une notification envoyée au client ou au livreur"""
  type Notification {
    notification_id: ID!
    recipient_id:    String!
    recipient_type:  String!
    message:         String!
    order_id:        String
    type:            String!
    created_at:      String
  }

  """Résultat d'une opération de suppression"""
  type DeleteResult {
    success: Boolean!
    message: String!
  }

  # ── Queries (lecture) ──────────────────────────────────────────────────────
  type Query {
    "Récupérer une commande par son ID"
    order(order_id: ID!): Order

    "Lister toutes les commandes d'un client"
    ordersByClient(client_id: ID!): [Order]

    "Récupérer un livreur par son ID"
    driver(driver_id: ID!): Driver

    "Lister tous les livreurs disponibles"
    availableDrivers: [Driver]

    "Récupérer les notifications d'un utilisateur"
    notifications(recipient_id: ID!): [Notification]
  }

  # ── Mutations (écriture) ───────────────────────────────────────────────────
  type Mutation {
    "Créer une nouvelle commande de livraison"
    createOrder(
      client_id:        String!
      client_name:      String!
      pickup_address:   String!
      delivery_address: String!
      items:            [String]
    ): Order

    "Mettre à jour le statut d'une commande"
    updateOrderStatus(order_id: ID!, status: String!): Order

    "Supprimer une commande"
    deleteOrder(order_id: ID!): DeleteResult

    "Enregistrer un nouveau livreur"
    registerDriver(
      name:    String!
      phone:   String!
      vehicle: String!
    ): Driver

    "Mettre à jour la position GPS d'un livreur"
    updateDriverLocation(driver_id: ID!, latitude: Float!, longitude: Float!): Driver

    "Modifier la disponibilité d'un livreur"
    updateDriverAvailability(driver_id: ID!, available: Boolean!): Driver

    "Assigner manuellement un livreur à une commande"
    assignDriver(driver_id: ID!, order_id: ID!): Driver
  }
`;

// ─── Résolveurs GraphQL ────────────────────────────────────────────────────────
const resolvers = {
  Query: {
    order: async (_, { order_id }) => {
      try {
        return await grpcGetOrder({ order_id });
      } catch (err) {
        throw new Error(err.message);
      }
    },

    ordersByClient: async (_, { client_id }) => {
      try {
        const { orders } = await grpcListOrders({ client_id });
        return orders;
      } catch (err) {
        throw new Error(err.message);
      }
    },

    driver: async (_, { driver_id }) => {
      try {
        return await grpcGetDriver({ driver_id });
      } catch (err) {
        throw new Error(err.message);
      }
    },

    availableDrivers: async () => {
      try {
        const { drivers } = await grpcGetAvailableDrivers({});
        return drivers;
      } catch (err) {
        throw new Error(err.message);
      }
    },

    notifications: async (_, { recipient_id }) => {
      try {
        const { notifications } = await grpcGetNotifications({ recipient_id });
        return notifications;
      } catch (err) {
        throw new Error(err.message);
      }
    }
  },

  Mutation: {
    createOrder: async (_, args) => {
      try {
        return await grpcCreateOrder(args);
      } catch (err) {
        throw new Error(err.message);
      }
    },

    updateOrderStatus: async (_, { order_id, status }) => {
      try {
        return await grpcUpdateOrderStatus({ order_id, status });
      } catch (err) {
        throw new Error(err.message);
      }
    },

    // ── AJOUT: deleteOrder ──────────────────────────────────────────────────
    deleteOrder: async (_, { order_id }) => {
      try {
        return await grpcDeleteOrder({ order_id });
      } catch (err) {
        throw new Error(err.message);
      }
    },

    registerDriver: async (_, args) => {
      try {
        return await grpcRegisterDriver(args);
      } catch (err) {
        throw new Error(err.message);
      }
    },

    updateDriverLocation: async (_, { driver_id, latitude, longitude }) => {
      try {
        return await grpcUpdateLocation({ driver_id, latitude, longitude });
      } catch (err) {
        throw new Error(err.message);
      }
    },

    // ── AJOUT: updateDriverAvailability ────────────────────────────────────
    updateDriverAvailability: async (_, { driver_id, available }) => {
      try {
        return await grpcUpdateAvailability({ driver_id, available });
      } catch (err) {
        throw new Error(err.message);
      }
    },

    assignDriver: async (_, { driver_id, order_id }) => {
      try {
        return await grpcAssignDriver({ driver_id, order_id });
      } catch (err) {
        throw new Error(err.message);
      }
    }
  }
};

module.exports = { typeDefs, resolvers };