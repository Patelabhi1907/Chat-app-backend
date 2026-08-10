const express = require('express');
const cors = require('cors');
const http = require('http');
const { Server } = require('socket.io');
const { createClient } = require('@libsql/client');
const bcrypt = require('bcryptjs');
require('dotenv').config();

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });

app.use(cors({ origin: '*', methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'], allowedHeaders: ['Content-Type', 'Authorization'] }));
app.use(express.json());
app.options('*', cors());

const PORT = process.env.PORT || 3000;
const onlineUsers = new Map();

const db = createClient({
    url: process.env.TURSO_URL || 'file:chat.db',
    authToken: process.env.TURSO_AUTH_TOKEN
});

async function initializeDB() {
    await db.execute(`CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE NOT NULL,
        password TEXT NOT NULL,
        createdAt DATETIME DEFAULT CURRENT_TIMESTAMP
    )`);
    await db.execute(`CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        senderId TEXT NOT NULL,
        receiverId TEXT NOT NULL,
        text TEXT NOT NULL,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        status TEXT DEFAULT 'sent'
    )`);
    console.log('Database initialized.');
}

app.get('/', (req, res) => res.json({ status: 'Chat API is running' }));

app.post('/api/auth/register', async (req, res, next) => {
    try {
        const { username, password } = req.body;
        if (!username || !password)
            return res.status(400).json({ error: 'Username and password are required' });

        const existing = await db.execute({ sql: 'SELECT id FROM users WHERE username = ?', args: [username] });
        if (existing.rows.length > 0)
            return res.status(409).json({ error: 'Username already taken' });

        const hashed = await bcrypt.hash(password, 10);
        await db.execute({ sql: 'INSERT INTO users (username, password) VALUES (?, ?)', args: [username, hashed] });
        res.status(201).json({ message: 'Account created', username });
    } catch (error) { next(error); }
});

app.post('/api/auth/login', async (req, res, next) => {
    try {
        const { username, password } = req.body;
        if (!username || !password)
            return res.status(400).json({ error: 'Username and password are required' });

        const result = await db.execute({ sql: 'SELECT * FROM users WHERE username = ?', args: [username] });
        const user = result.rows[0];
        if (!user)
            return res.status(401).json({ error: 'Invalid username or password' });

        const match = await bcrypt.compare(password, user.password);
        if (!match)
            return res.status(401).json({ error: 'Invalid username or password' });

        res.json({ message: 'Login successful', username: user.username });
    } catch (error) { next(error); }
});

app.get('/api/messages/unread/:userId', async (req, res, next) => {
    try {
        const { userId } = req.params;
        const { rows } = await db.execute({ sql: `SELECT senderId, COUNT(*) as count FROM messages WHERE receiverId = ? AND status != 'read' GROUP BY senderId`, args: [userId] });
        const result = {};
        rows.forEach(r => { result[r.senderId] = r.count; });
        res.json(result);
    } catch (error) { next(error); }
});

app.get('/api/messages/:userId1/:userId2', async (req, res, next) => {
    try {
        const { userId1, userId2 } = req.params;
        const { rows } = await db.execute({ sql: `SELECT * FROM messages WHERE (senderId = ? AND receiverId = ?) OR (senderId = ? AND receiverId = ?) ORDER BY timestamp ASC`, args: [userId1, userId2, userId2, userId1] });
        res.json(rows);
    } catch (error) { next(error); }
});

app.get('/api/users/all', async (req, res, next) => {
    try {
        const { rows } = await db.execute('SELECT username FROM users');
        res.json(rows.map(u => u.username));
    } catch (error) { next(error); }
});

app.delete('/api/users/:username', async (req, res, next) => {
    try {
        const { username } = req.params;
        await db.execute({ sql: 'DELETE FROM users WHERE username = ?', args: [username] });
        await db.execute({ sql: 'DELETE FROM messages WHERE senderId = ? OR receiverId = ?', args: [username, username] });
        res.json({ message: 'User deleted' });
    } catch (error) { next(error); }
});

app.get('/api/users/online', (req, res) => {
    res.json([...onlineUsers.values()]);
});

function getPrivateRoom(userId1, userId2) {
    return [userId1, userId2].sort().join('_');
}

io.on('connection', (socket) => {
    socket.on('register', (userId) => {
        for (const [sid, uid] of onlineUsers.entries()) {
            if (uid === userId && sid !== socket.id) onlineUsers.delete(sid);
        }
        onlineUsers.set(socket.id, userId);
        socket.userId = userId;
        socket.emit('online_users', [...onlineUsers.values()]);
        socket.broadcast.emit('online_users', [...onlineUsers.values()]);
    });

    socket.on('join_private', ({ userId, targetId }) => {
        socket.join(getPrivateRoom(userId, targetId));
    });

    socket.on('send_message', async ({ senderId, receiverId, text, tempId }) => {
        try {
            const receiverOnline = [...onlineUsers.values()].includes(receiverId);
            const status = receiverOnline ? 'delivered' : 'sent';
            const result = await db.execute({ sql: 'INSERT INTO messages (senderId, receiverId, text, status) VALUES (?, ?, ?, ?)', args: [senderId, receiverId, text, status] });
            const { rows } = await db.execute({ sql: 'SELECT * FROM messages WHERE id = ?', args: [result.lastInsertRowid] });
            const newMessage = rows[0];
            for (const [socketId, uid] of onlineUsers.entries()) {
                if (uid === receiverId) io.to(socketId).emit('receive_message', newMessage);
            }
            socket.emit('message_saved', { ...newMessage, tempId });
        } catch (error) {
            socket.emit('error', { message: 'Failed to save message' });
        }
    });

    socket.on('typing', ({ senderId, receiverId, typing }) => {
        for (const [socketId, uid] of onlineUsers.entries()) {
            if (uid === receiverId) io.to(socketId).emit('typing', { senderId, typing });
        }
    });

    socket.on('messages_read', async ({ readerId, senderId }) => {
        try {
            await db.execute({ sql: `UPDATE messages SET status = 'read' WHERE senderId = ? AND receiverId = ? AND status != 'read'`, args: [senderId, readerId] });
            for (const [socketId, uid] of onlineUsers.entries()) {
                if (uid === senderId) io.to(socketId).emit('messages_read', { readerId });
            }
        } catch (error) { console.error('messages_read error:', error); }
    });

    socket.on('disconnect', () => {
        if (socket.userId) {
            onlineUsers.delete(socket.id);
            io.emit('online_users', [...onlineUsers.values()]);
        }
    });
});

app.use((err, req, res, next) => {
    res.status(500).json({ error: 'Internal Server Error', message: err.message });
});

initializeDB().then(() => {
    server.listen(PORT, () => console.log(`Server running on http://localhost:${PORT}`));
}).catch(err => console.error('Failed to initialize database:', err));

module.exports = app;
