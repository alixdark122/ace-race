const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');
const { KickConnection, Events } = require('kick-live-connector');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

let gameClients = [];
let kickConnection = null;
let currentChannel = null;
let isConnected = false;

// Deduplication: store recent message hashes
const recentMessages = new Set();
const MAX_RECENT = 5000;

app.use(express.json());
app.use('/assets', express.static(path.join(__dirname, 'assets')));
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'game.html'));
});

async function startChatMonitor(channelName) {
    if (kickConnection) {
        console.log('Closing existing connection...');
        kickConnection.disconnect();
        kickConnection = null;
        isConnected = false;
    }
    // Clear dedup cache for new channel
    recentMessages.clear();

    console.log(`Connecting to channel: ${channelName}...`);
    kickConnection = new KickConnection(channelName);

    kickConnection.on(Events.ChatMessage, (data) => {
        const { content, sender, id } = data;
        const username = sender?.username;
        if (!username || !content) return;

        // Create unique key (message ID if available, else content + username + timestamp)
        const messageKey = id ? `${id}` : `${username}:${content}`;
        if (recentMessages.has(messageKey)) {
            // Duplicate, skip
            return;
        }
        recentMessages.add(messageKey);
        if (recentMessages.size > MAX_RECENT) {
            // Keep set manageable
            const toRemove = [...recentMessages].slice(0, MAX_RECENT / 2);
            toRemove.forEach(k => recentMessages.delete(k));
        }

        console.log(`📨 ${username}: ${content}`);
        const payload = JSON.stringify({ type: 'chat', username, content });
        gameClients.forEach(client => {
            if (client.readyState === WebSocket.OPEN) {
                client.send(payload);
            }
        });
    });

    kickConnection.on(Events.Connected, (state) => {
        console.log(`✅ Connected to chatroom: ${state.roomID}`);
        isConnected = true;
    });

    kickConnection.on(Events.Error, (err) => {
        console.error('❌ Chat connection error:', err);
        isConnected = false;
    });

    kickConnection.on(Events.Disconnected, () => {
        console.log('🛑 Disconnected from chat');
        isConnected = false;
    });

    try {
        await kickConnection.connect();
        console.log(`✅ Chat monitor active for ${channelName}`);
        return true;
    } catch (err) {
        console.error('❌ Failed to start chat monitor:', err);
        isConnected = false;
        return false;
    }
}

app.post('/set-channel', async (req, res) => {
    const channel = req.body.channel?.toLowerCase().trim();
    if (!channel) return res.status(400).json({ success: false, error: 'Channel required' });
    try {
        currentChannel = channel;
        const success = await startChatMonitor(channel);
        if (success) {
            res.json({ success: true, channel });
        } else {
            res.status(500).json({ success: false, error: 'Failed to connect to chat.' });
        }
    } catch (err) {
        console.error('❌ API error:', err);
        res.status(500).json({ success: false, error: err.message });
    }
});

app.get('/chat-status', (req, res) => {
    res.json({ connected: isConnected, channel: currentChannel });
});

wss.on('connection', (ws) => {
    console.log('🎮 Game client connected');
    gameClients.push(ws);
    ws.on('close', () => {
        gameClients = gameClients.filter(c => c !== ws);
        console.log('Game client disconnected');
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
    console.log(`✨ Ace Race server running at http://localhost:${PORT}`);
});

process.on('SIGINT', async () => {
    if (kickConnection) kickConnection.disconnect();
    process.exit();
});