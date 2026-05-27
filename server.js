const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

let gameClients = [];
let pusherWs = null;
let currentChannel = null;
let isConnected = false;
let reconnectTimer = null;
let pingInterval = null;

const recentMessages = new Set();
const MAX_RECENT = 5000;

app.use(express.json());
app.use('/assets', express.static(path.join(__dirname, 'assets')));
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'game.html'));
});

// Use a public proxy to get channel data without 403
async function getChatroomId(channelSlug) {
    // Try multiple proxy endpoints
    const proxyUrls = [
        `https://kick.com/api/v2/channels/${channelSlug}`,  // direct (will 403 but try anyway)
        `https://corsproxy.io/?url=https://kick.com/api/v2/channels/${channelSlug}`,
        `https://api.allorigins.win/raw?url=https://kick.com/api/v2/channels/${channelSlug}`
    ];
    for (const url of proxyUrls) {
        try {
            console.log(`Trying proxy: ${url.substring(0, 60)}...`);
            const res = await fetch(url, {
                headers: {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                }
            });
            if (res.ok) {
                const data = await res.json();
                const chatroomId = data.chatroom?.id;
                if (chatroomId) return chatroomId;
            }
        } catch (e) {}
    }
    throw new Error('Could not fetch chatroom ID via any proxy');
}

function connectToPusher(chatroomId) {
    if (pusherWs) {
        pusherWs.close();
        pusherWs = null;
    }
    if (pingInterval) clearInterval(pingInterval);
    const wsUrl = 'wss://ws-us2.pusher.com/app/32cbd69e4b950bf97679?protocol=7&client=js&version=8.4.0';
    pusherWs = new WebSocket(wsUrl);
    pusherWs.onopen = () => {
        console.log('Pusher WebSocket open');
        pusherWs.send(JSON.stringify({
            event: 'pusher:subscribe',
            data: { auth: '', channel: `chatrooms.${chatroomId}.v2` }
        }));
        pingInterval = setInterval(() => {
            if (pusherWs && pusherWs.readyState === WebSocket.OPEN) {
                pusherWs.send(JSON.stringify({ event: 'pusher:ping', data: {} }));
            }
        }, 30000);
    };
    pusherWs.onmessage = (event) => {
        try {
            const pkt = JSON.parse(event.data);
            if (pkt.event === 'pusher:connection_established') {
                console.log('Pusher connection established');
                isConnected = true;
            } else if (pkt.event === 'App\\Events\\ChatMessageEvent') {
                const msg = JSON.parse(pkt.data);
                const username = msg.sender?.username;
                const content = msg.content?.trim() || '';
                if (username && content) {
                    const key = `${username}:${content}`;
                    if (recentMessages.has(key)) return;
                    recentMessages.add(key);
                    if (recentMessages.size > MAX_RECENT) {
                        const toRemove = [...recentMessages].slice(0, MAX_RECENT/2);
                        toRemove.forEach(k => recentMessages.delete(k));
                    }
                    console.log(`📨 ${username}: ${content}`);
                    const payload = JSON.stringify({ type: 'chat', username, content });
                    gameClients.forEach(client => {
                        if (client.readyState === WebSocket.OPEN) client.send(payload);
                    });
                }
            }
        } catch (err) {}
    };
    pusherWs.onerror = (err) => {
        console.error('Pusher error:', err.message);
        isConnected = false;
    };
    pusherWs.onclose = () => {
        console.log('Pusher closed, reconnecting in 5s...');
        isConnected = false;
        if (reconnectTimer) clearTimeout(reconnectTimer);
        reconnectTimer = setTimeout(() => {
            if (currentChannel) {
                startChatMonitor(currentChannel).catch(console.error);
            }
        }, 5000);
    };
}

async function startChatMonitor(channelName) {
    currentChannel = channelName;
    recentMessages.clear();
    try {
        const chatroomId = await getChatroomId(channelName);
        console.log(`✅ Chatroom ID: ${chatroomId}`);
        connectToPusher(chatroomId);
        return true;
    } catch (err) {
        console.error('❌ Failed to start chat monitor:', err);
        return false;
    }
}

async function stopChatMonitor() {
    if (reconnectTimer) clearTimeout(reconnectTimer);
    if (pingInterval) clearInterval(pingInterval);
    if (pusherWs) pusherWs.close();
    pusherWs = null;
    isConnected = false;
    console.log('🛑 Chat monitor stopped');
}

app.post('/set-channel', async (req, res) => {
    const channel = req.body.channel?.toLowerCase().trim();
    if (!channel) return res.status(400).json({ success: false, error: 'Channel required' });
    try {
        await stopChatMonitor();
        const success = await startChatMonitor(channel);
        if (success) res.json({ success: true, channel });
        else res.status(500).json({ success: false, error: 'Failed to connect to chat' });
    } catch (err) {
        console.error('API error:', err);
        res.status(500).json({ success: false, error: err.message });
    }
});

app.get('/chat-status', (req, res) => {
    res.json({ connected: isConnected && pusherWs?.readyState === WebSocket.OPEN, channel: currentChannel });
});

wss.on('connection', (ws) => {
    console.log('🎮 Game client connected');
    gameClients.push(ws);
    ws.on('close', () => gameClients = gameClients.filter(c => c !== ws));
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
    console.log(`✨ Ace Race server running at http://localhost:${PORT}`);
});

process.on('SIGINT', async () => {
    await stopChatMonitor();
    process.exit();
});