require('dotenv').config();
const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');
const mongoose = require('mongoose');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const PORT = process.env.PORT || 3000;
const JWT_SECRET = process.env.JWT_SECRET || 'fallback-dev-secret-change-in-prod';
const MONGO_URI = process.env.MONGO_URI || 'mongodb://localhost:27017/airdrop_internet';

// ── MongoDB User Model ──────────────────────────────────────────────────────
const userSchema = new mongoose.Schema({
  name:         { type: String, required: true, trim: true, maxlength: 80 },
  email:        { type: String, required: true, unique: true, lowercase: true, trim: true },
  passwordHash: { type: String, required: true },
  createdAt:    { type: Date, default: Date.now }
});

const User = mongoose.model('User', userSchema);

// ── Connect MongoDB ─────────────────────────────────────────────────────────
mongoose.connect(MONGO_URI)
  .then(() => console.log('✅ MongoDB connected'))
  .catch(err => {
    console.warn('⚠ MongoDB connection failed:', err.message);
    console.warn('   The app will still run — auth endpoints will return 503 until DB connects.');
  });

// ── Middleware ──────────────────────────────────────────────────────────────
app.use(express.json({ limit: '10kb' }));
app.use(express.static(path.join(__dirname, 'public')));

// JWT auth middleware (for future protected API routes)
function requireAuth(req, res, next) {
  const authHeader = req.headers.authorization;
  if (!authHeader || !authHeader.startsWith('Bearer ')) {
    return res.status(401).json({ error: 'Missing or invalid Authorization header' });
  }
  try {
    req.user = jwt.verify(authHeader.slice(7), JWT_SECRET);
    next();
  } catch (e) {
    return res.status(401).json({ error: 'Token expired or invalid' });
  }
}

// ── Auth Routes ─────────────────────────────────────────────────────────────

// POST /api/auth/register
app.post('/api/auth/register', async (req, res) => {
  try {
    const { name, email, password } = req.body;

    if (!name || !email || !password) {
      return res.status(400).json({ error: 'Name, email and password are required.' });
    }
    if (password.length < 8) {
      return res.status(400).json({ error: 'Password must be at least 8 characters.' });
    }

    const lookupEmail = email.toLowerCase().trim();
    const existing = await User.findOne({ email: lookupEmail });
    if (existing) {
      return res.status(409).json({ error: 'An account with this email already exists.' });
    }

    const passwordHash = await bcrypt.hash(password, 12);
    const user = await User.create({ name, email: lookupEmail, passwordHash });

    const token = jwt.sign(
      { userId: user._id.toString(), email: user.email, name: user.name },
      JWT_SECRET,
      { expiresIn: '7d' }
    );

    res.status(201).json({ token, user: { name: user.name, email: user.email } });
  } catch (err) {
    console.error('Register error:', err);
    res.status(500).json({ error: 'Server error. Please try again.' });
  }
});

// POST /api/auth/login
app.post('/api/auth/login', async (req, res) => {
  try {
    const { email, password } = req.body;
    if (!email || !password) {
      return res.status(400).json({ error: 'Email and password are required.' });
    }

    const lookupEmail = email.toLowerCase().trim();
    const user = await User.findOne({ email: lookupEmail });
    if (!user) {
      return res.status(401).json({ error: 'Invalid email or password.' });
    }

    const valid = await bcrypt.compare(password, user.passwordHash);
    if (!valid) {
      return res.status(401).json({ error: 'Invalid email or password.' });
    }

    const token = jwt.sign(
      { userId: user._id.toString(), email: user.email, name: user.name },
      JWT_SECRET,
      { expiresIn: '7d' }
    );

    res.json({ token, user: { name: user.name, email: user.email } });
  } catch (err) {
    console.error('Login error:', err);
    res.status(500).json({ error: 'Server error. Please try again.' });
  }
});

// GET /api/auth/me — verify token + return user info
app.get('/api/auth/me', requireAuth, async (req, res) => {
  try {
    const user = await User.findById(req.user.userId).select('-passwordHash');
    if (!user) return res.status(404).json({ error: 'User not found.' });
    res.json({ user: { name: user.name, email: user.email, createdAt: user.createdAt } });
  } catch (err) {
    res.status(500).json({ error: 'Server error.' });
  }
});

// ── WebSocket Signaling ─────────────────────────────────────────────────────
// Store rooms and active connections
const rooms = new Map(); // roomId → Set<ws>

wss.on('connection', (ws) => {
  console.log('New WebSocket connection established.');
  let currentRoom = null;

  ws.on('message', (message) => {
    try {
      const parsed = JSON.parse(message);

      switch (parsed.type) {
        case 'join': {
          const roomId = parsed.room;
          if (!roomId) {
            ws.send(JSON.stringify({ type: 'error', message: 'Room ID is required.' }));
            return;
          }

          if (!rooms.has(roomId)) rooms.set(roomId, new Set());

          const clients = rooms.get(roomId);
          if (clients.size >= 2) {
            ws.send(JSON.stringify({ type: 'full', room: roomId }));
            return;
          }

          clients.add(ws);
          currentRoom = roomId;

          const isInitiator = clients.size === 1;
          ws.send(JSON.stringify({ type: 'joined', room: roomId, isInitiator }));

          console.log(`Client joined room ${roomId}. Size: ${clients.size}. Initiator: ${isInitiator}`);

          if (clients.size === 2) {
            for (const client of clients) {
              client.send(JSON.stringify({ type: 'ready' }));
            }
          }
          break;
        }

        case 'signal': {
          if (!currentRoom || !rooms.has(currentRoom)) {
            ws.send(JSON.stringify({ type: 'error', message: 'Not in a room.' }));
            return;
          }
          // Forward signal (SDP, ICE candidates, ECDH public keys) to the other peer
          const clients = rooms.get(currentRoom);
          for (const client of clients) {
            if (client !== ws && client.readyState === WebSocket.OPEN) {
              client.send(JSON.stringify({ type: 'signal', data: parsed.data }));
            }
          }
          break;
        }

        default:
          console.log('Unknown message type:', parsed.type);
      }
    } catch (err) {
      console.error('Error parsing WS message:', err);
    }
  });

  ws.on('close', () => {
    console.log('WebSocket connection closed.');
    if (currentRoom && rooms.has(currentRoom)) {
      const clients = rooms.get(currentRoom);
      clients.delete(ws);

      for (const client of clients) {
        if (client.readyState === WebSocket.OPEN) {
          client.send(JSON.stringify({ type: 'peer-left' }));
        }
      }

      if (clients.size === 0) {
        rooms.delete(currentRoom);
        console.log(`Room ${currentRoom} deleted (empty).`);
      }
    }
  });
});

// ── Start Server ────────────────────────────────────────────────────────────
server.listen(PORT, () => {
  console.log(`\n⚡ AirDrop for the Internet`);
  console.log(`   Server:    http://localhost:${PORT}`);
  console.log(`   MongoDB:   ${MONGO_URI.replace(/:\/\/.*@/, '://***@')}`);
  console.log(`   JWT:       ${JWT_SECRET.length} char secret\n`);
});
