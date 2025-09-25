const express = require('express');
const path = require('path');

const app = express();
const PORT = Number(process.env.PORT || 6000);
const API_BASE = process.env.FRONTEND_API_BASE || process.env.API_BASE || 'http://localhost:8000';

// 1) health/test endpoints
app.get('/healthz', (_req, res) => res.send('ok'));
app.get('/__test', (_req, res) => res.type('text/plain').send('test-ok'));

// 2) config endpoint (voor de browser)
app.get('/config.js', (_req, res) => {
  res.type('application/javascript');
  res.send(`window.API_BASE = ${JSON.stringify(API_BASE)};`);
});

// 3) statische assets
app.use(express.static(path.join(__dirname, 'public')));

// 4) catch-all fallback (regex, express v5-proof)
app.get(/.*/, (_req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.listen(PORT, '0.0.0.0', () => {
  console.log(`Frontend listening on ${PORT}. Using API at ${API_BASE}`);
});
