const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');
const { ApolloServer } = require('apollo-server-express');

const { typeDefs, resolvers } = require('./graphql/schema');
const restRoutes = require('./rest/routes');

const PORT = process.env.PORT || 3000;

async function startServer() {
  const app = express();

  // ── Middlewares ──────────────────────────────────────────────────────────────
  app.use(cors());
  app.use(bodyParser.json());

  // ── Routes REST (/api/...) ───────────────────────────────────────────────────
  app.use('/api', restRoutes);

  // ── Endpoint de santé ────────────────────────────────────────────────────────
  app.get('/health', (req, res) => {
    res.json({
      status: 'OK',
      service: 'API Gateway',
      timestamp: new Date().toISOString(),
      endpoints: {
        rest: 'http://localhost:3000/api',
        graphql: 'http://localhost:3000/graphql'
      }
    });
  });

  // ── Serveur GraphQL (Apollo) ─────────────────────────────────────────────────
  const apolloServer = new ApolloServer({
    typeDefs,
    resolvers,
    // Désactiver les erreurs de production pour voir les détails en dev
    formatError: (err) => {
      console.error('[GraphQL Error]:', err.message);
      return err;
    }
  });

  await apolloServer.start();
  apolloServer.applyMiddleware({ app, path: '/graphql' });

  // ── Démarrage ────────────────────────────────────────────────────────────────
  app.listen(PORT, () => {
    console.log('\n╔══════════════════════════════════════════════════════╗');
    console.log('║         🚀 API GATEWAY - Système de Livraison        ║');
    console.log('╠══════════════════════════════════════════════════════╣');
    console.log(`║  REST    → http://localhost:${PORT}/api              ║`);
    console.log(`║  GraphQL → http://localhost:${PORT}/graphql          ║`);
    console.log(`║  Health  → http://localhost:${PORT}/health           ║`);
    console.log('╠══════════════════════════════════════════════════════╣');
    console.log('║  Microservices connectés via gRPC:                   ║');
    console.log('║    Order Service      → localhost:50051              ║');
    console.log('║    Driver Service     → localhost:50052              ║');
    console.log('║    Notification Svc   → localhost:50053              ║');
    console.log('╚══════════════════════════════════════════════════════╝\n');
  });
}

startServer().catch(err => {
  console.error('Erreur démarrage Gateway:', err);
  process.exit(1);
});