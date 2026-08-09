const express = require('express');
const cors = require('cors');
const http = require('http');
const { Server } = require('socket.io');
const sqlite3 = require('sqlite3');
const { open } = require('sqlite');
const path = require('path');
const bcrypt = require('bcryptjs');
require('dotenv').config();

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
    cors: { origin: '*' }
});

app.use(cors());
app.use(express.json());

const PORT = process.env.PORT || 3000;
let db;
const onlineUsers = new Map(); // socketId -> userId

async function initializeDB() {
    db = await open({
        filename: path.join(__dirname, 'chat.db'),
        driver: sqlite3.Database
    });

    await db.exec(`
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL,
            createdAt DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    `);

    await db.exec(`
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            senderId TEXT NOT NULL,
            receiverId TEXT NOT NULL,
            text TEXT NOT NULL,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            status TEXT DEFAULT 'sent'
        )
    `);
    console.log('SQLite Database initialized successfully.');
}

// REST Routes
app.get('/', (req, res) => res.json({ status: 'Chat API is running' }));

// Register
app.post('/api/auth/register', async (req, res, next) => {
    try {
        const { username, password } = req.body;
        if (!username || !password)
            return res.status(400).json({ error: 'Username and password are required' });

        const existing = await db.get('SELECT id FROM users WHERE username = ?', username);
        if (existing)
            return res.status(409).json({ error: 'Username already taken' });

        const hashed = await bcrypt.hash(password, 10);
        await db.run('INSERT INTO users (username, password) VALUES (?, ?)', [username, hashed]);
        res.status(201).json({ message: 'Account created', username });
    } catch (error) {
        next(error);
    }
});

// Login
app.post('/api/auth/login', async (req, res, next) => {
    try {
        const { username, password } = req.body;
        if (!username || !password)
            return res.status(400).json({ error: 'Username and password are required' });

        const user = await db.get('SELECT * FROM users WHERE username = ?', username);
        if (!user)
            return res.status(401).json({ error: 'Invalid username or password' });

        const match = await bcrypt.compare(password, user.password);
        if (!match)
            return res.status(401).json({ error: 'Invalid username or password' });

        res.json({ message: 'Login successful', username: user.username });
    } catch (error) {
        next(error);
    }
});

// Get unread message count per sender for a receiver
app.get('/api/messages/unread/:userId', async (req, res, next) => {
    try {
        const { userId } = req.params;
        const rows = await db.all(
            `SELECT senderId, COUNT(*) as count FROM messages WHERE receiverId = ? AND status != 'read' GROUP BY senderId`,
            [userId]
        );
        const result = {};
        rows.forEach(r => { result[r.senderId] = r.count; });
        res.json(result);
    } catch (error) {
        next(error);
    }
});

// Get private chat history between two users
app.get('/api/messages/:userId1/:userId2', async (req, res, next) => {
    try {
        const { userId1, userId2 } = req.params;
        const messages = await db.all(
            `SELECT * FROM messages
             WHERE (senderId = ? AND receiverId = ?) OR (senderId = ? AND receiverId = ?)
             ORDER BY timestamp ASC`,
            [userId1, userId2, userId2, userId1]
        );
        res.json(messages);
    } catch (error) {
        next(error);
    }
});

// Get all users
app.get('/api/users/all', async (req, res, next) => {
    try {
        const users = await db.all('SELECT username FROM users');
        res.json(users.map(u => u.username));
    } catch (error) {
        next(error);
    }
});

// Delete a user
app.delete('/api/users/:username', async (req, res, next) => {
    try {
        const { username } = req.params;
        await db.run('DELETE FROM users WHERE username = ?', [username]);
        await db.run('DELETE FROM messages WHERE senderId = ? OR receiverId = ?', [username, username]);
        res.json({ message: 'User deleted' });
    } catch (error) {
        next(error);
    }
});

// Get online users
app.get('/api/users/online', (req, res) => {
    res.json([...onlineUsers.values()]);
});

// Generate a consistent private room ID for any two users
function getPrivateRoom(userId1, userId2) {
    return [userId1, userId2].sort().join('_');
}

// Socket.io
io.on('connection', (socket) => {
    console.log(`User connected: ${socket.id}`);

    // Register user and broadcast updated online list
    socket.on('register', (userId) => {
        // Remove any previous entry for this userId to avoid duplicates
        for (const [sid, uid] of onlineUsers.entries()) {
            if (uid === userId && sid !== socket.id) {
                onlineUsers.delete(sid);
            }
        }
        onlineUsers.set(socket.id, userId);
        socket.userId = userId;
        console.log(`Registered: ${userId} -> ${socket.id}`);
        console.log('Online users now:', [...onlineUsers.entries()]);
        socket.emit('online_users', [...onlineUsers.values()]);
        socket.broadcast.emit('online_users', [...onlineUsers.values()]);
    });

    // Join a private room between two users
    socket.on('join_private', ({ userId, targetId }) => {
        const room = getPrivateRoom(userId, targetId);
        socket.join(room);
        console.log(`${userId} joined private room: ${room}`);
    });

    // Send a private message
    socket.on('send_message', async ({ senderId, receiverId, text, tempId }) => {
        try {
            const receiverOnline = [...onlineUsers.values()].includes(receiverId);
            const status = receiverOnline ? 'delivered' : 'sent';

            const result = await db.run(
                'INSERT INTO messages (senderId, receiverId, text, status) VALUES (?, ?, ?, ?)',
                [senderId, receiverId, text, status]
            );
            const newMessage = await db.get('SELECT * FROM messages WHERE id = ?', result.lastID);

            let delivered = false;
            for (const [socketId, uid] of onlineUsers.entries()) {
                if (uid === receiverId) {
                    io.to(socketId).emit('receive_message', newMessage);
                    delivered = true;
                }
            }
            console.log(`Message from ${senderId} to ${receiverId} — delivered: ${delivered}`);

            socket.emit('message_saved', { ...newMessage, tempId });
        } catch (error) {
            console.error('send_message error:', error);
            socket.emit('error', { message: 'Failed to save message' });
        }
    });

    // Typing indicator
    socket.on('typing', ({ senderId, receiverId, typing }) => {
        for (const [socketId, uid] of onlineUsers.entries()) {
            if (uid === receiverId) {
                io.to(socketId).emit('typing', { senderId, typing });
            }
        }
    });

    // Read receipts
    socket.on('messages_read', async ({ readerId, senderId }) => {
        try {
            await db.run(
                `UPDATE messages SET status = 'read' WHERE senderId = ? AND receiverId = ? AND status != 'read'`,
                [senderId, readerId]
            );
            for (const [socketId, uid] of onlineUsers.entries()) {
                if (uid === senderId) {
                    io.to(socketId).emit('messages_read', { readerId });
                }
            }
        } catch (error) {
            console.error('messages_read error:', error);
        }
    });

    socket.on('disconnect', () => {
        if (socket.userId) {
            onlineUsers.delete(socket.id);
            io.emit('online_users', [...onlineUsers.values()]);
            console.log(`${socket.userId} disconnected`);
        }
    });
});

// Global Error Handler
app.use((err, req, res, next) => {
    console.error('API Error:', err.stack);
    res.status(500).json({ error: 'Internal Server Error', message: err.message });
});

initializeDB().then(() => {
    server.listen(PORT, () => {
        console.log(`Server is running on http://localhost:${PORT}`);
    });
}).catch(err => {
    console.error('Failed to initialize database:', err);
});

module.exports = app;
