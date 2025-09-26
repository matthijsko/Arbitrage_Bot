const express = require('express');
const path = require('path');
const fs = require('fs');

const app = express();
const PORT = Number(process.env.PORT || 6000);
const API_BASE = process.env.FRONTEND_API_BASE || process.env.API_BASE || 'http://localhost:8000';

// Debug bij start (laat in logs zien of index er echt is)
const staticDir = path.join(__dirname, 'public');
console.log('Static dir =', staticDir);
console.log('Has index  =', fs.existsSync(path.join(staticDir, 'index.html')));

// Health/test
app.get('/healthz', (_req, res) => res.send('ok'));
app.get('/__test', (_req, res) => res.type('text/plain').send('test-ok'));

// Config voor de browser
app.get('/config.js', (_req, res) => {
  res.type('application/javascript');
  res.send(`window.API_BASE = ${JSON.stringify(API_BASE)};`);
});

// Static assets
app.use(express.static(staticDir));

// Catch-all (regex, Express v5-proof)
app.get(/.*/, (_req, res) => {
  res.sendFile(path.join(staticDir, 'index.html'));
});

app.listen(PORT, '0.0.0.0', () => {
  console.log(`Frontend listening on ${PORT}. Using API at ${API_BASE}`);
});
