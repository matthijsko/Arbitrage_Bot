import express from 'express';
const app = express();
const PORT = process.env.PORT || 6000;

app.get('/health', (req, res) => res.json({ ok: true, service: 'frontend' }));
app.get('/', (req, res) => res.send('<h1>Arbitrage Frontend Stub</h1><p>Phase B: UI skeleton running.</p>'));

app.listen(PORT, () => {
  console.log(`Frontend stub listening on port ${PORT}`);
});
