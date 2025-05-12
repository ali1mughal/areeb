const express = require('express');
const WebSocket = require('ws');
const { Client, GatewayIntentBits, ActivityType } = require('discord.js');
const fetch = (...args) => import('node-fetch').then(({default: fetch}) => fetch(...args));
const { v4: uuidv4 } = require('uuid');
const rateLimit = require('express-rate-limit');
const helmet = require('helmet');
const cors = require('cors');
const morgan = require('morgan');
const { createHash } = require('crypto');
require('dotenv').config();

// Constants
const API_VERSION = '1.0.0';
const CACHE_EXPIRATION = 5 * 60 * 1000; // 5 minutes
const MAX_CONNECTIONS_PER_IP = 5;
const PRESENCE_UPDATE_INTERVAL = 30 * 1000; // 30 seconds
const STATUS_CHECK_INTERVAL = 60 * 1000; // 1 minute

// Initialize Express app
const app = express();
const port = process.env.PORT || 10000;

// Security middleware
app.use(helmet());
app.use(cors({
    origin: process.env.ALLOWED_ORIGINS?.split(',') || '*'
}));
app.use(morgan('combined'));
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Rate limiting
const limiter = rateLimit({
    windowMs: 15 * 60 * 1000, // 15 minutes
    max: 100, // Limit each IP to 100 requests per windowMs
    standardHeaders: true,
    legacyHeaders: false,
});
app.use(limiter);

// Create Discord client with all necessary intents
const discordClient = new Client({
    intents: [
        GatewayIntentBits.Guilds,
        GatewayIntentBits.GuildPresences,
        GatewayIntentBits.GuildMembers,
        GatewayIntentBits.DirectMessages
    ],
    presence: {
        status: 'online',
        activities: [{
            name: 'Presence Tracker',
            type: ActivityType.Watching
        }]
    }
});

// Initialize server
const server = app.listen(port, () => {
    console.log(`Server v${API_VERSION} running on http://localhost:${port}`);
});

// WebSocket server setup
const wss = new WebSocket.Server({ 
    server,
    clientTracking: true,
    maxPayload: 1024 * 1024 // 1MB
});

// Data stores
const userCache = new Map();
const lastOnlineData = {};
const userSubscriptions = {};
const offlineStatusStore = {};
const connectionStats = {
    totalConnections: 0,
    activeConnections: 0,
    peakConnections: 0,
    messagesProcessed: 0,
    errors: 0
};
const rateLimits = new Map();

// Status colors mapping
const statusColors = {
    idle: "#f0b232",
    dnd: "#f23f43",
    online: "#23a55a",
    offline: "#80848e",
    streaming: "#593695",
    invisible: "#747f8d"
};

// Badge information
const badgeInfo = {
    premium: {
        description: 'Nitro Subscriber',
        color: '#ff73fa',
        icon: 'https://discord.com/assets/6debd47ed13483642cf09e832ed0bc1b.png'
    },
    staff: {
        description: 'Discord Staff',
        color: '#5865F2',
        icon: 'https://discord.com/assets/5e8dd8a98d0c0e1e4f9a3c3b51c2e5f5.svg'
    },
    partner: {
        description: 'Partnered Server Owner',
        color: '#5865F2',
        icon: 'https://discord.com/assets/3f9748e53446a137a052f3454c2e949e.svg'
    },
    bug_hunter: {
        description: 'Bug Hunter',
        color: '#f0b232',
        icon: 'https://discord.com/assets/e04dd6d6b7a4e4e8ec6a4c8a5e8b9d9d.svg'
    },
    early_supporter: {
        description: 'Early Supporter',
        color: '#ff73fa',
        icon: 'https://discord.com/assets/6debd47ed13483642cf09e832ed0bc1b.png'
    },
    verified_bot_developer: {
        description: 'Verified Bot Developer',
        color: '#5865F2',
        icon: 'https://discord.com/assets/6debd47ed13483642cf09e832ed0bc1b.png'
    }
};

// Connect to Discord
discordClient.login(process.env.DISCORD_BOT_TOKEN)
    .then(() => {
        console.log(`Bot logged in as ${discordClient.user.tag}`);
        startBackgroundTasks();
    })
    .catch(err => {
        console.error('Login failed:', err);
        process.exit(1);
    });

// Presence update handler
discordClient.on('presenceUpdate', async (oldPresence, newPresence) => {
    try {
        const userId = newPresence.userId;
        if (!userId) return;

        const data = await formatPresenceData(newPresence);
        
        lastOnlinePlatformHandler(userId, data);
        
        if (userSubscriptions[userId]) {
            const fullData = await getFullUserData(userId, data);
            broadcastUpdate(userId, fullData);
        }
    } catch (error) {
        console.error('Error in presenceUpdate:', error);
        connectionStats.errors++;
    }
});

// Guild member update handler
discordClient.on('guildMemberUpdate', async (oldMember, newMember) => {
    try {
        const userId = newMember.user.id;
        if (userSubscriptions[userId]) {
            const presence = await fetchUserPresence(userId);
            const fullData = await getFullUserData(userId, presence);
            broadcastUpdate(userId, fullData);
        }
    } catch (error) {
        console.error('Error in guildMemberUpdate:', error);
        connectionStats.errors++;
    }
});

// User update handler
discordClient.on('userUpdate', async (oldUser, newUser) => {
    try {
        const userId = newUser.id;
        if (userSubscriptions[userId]) {
            const presence = await fetchUserPresence(userId);
            const fullData = await getFullUserData(userId, presence);
            broadcastUpdate(userId, fullData);
        }
    } catch (error) {
        console.error('Error in userUpdate:', error);
        connectionStats.errors++;
    }
});

// WebSocket connection handler
wss.on('connection', (ws, req) => {
    const connectionId = uuidv4();
    const ip = req.headers['x-forwarded-for'] || req.socket.remoteAddress;
    const ipHash = createHash('sha256').update(ip).digest('hex').substring(0, 8);
    
    // Track connection statistics
    connectionStats.totalConnections++;
    connectionStats.activeConnections++;
    if (connectionStats.activeConnections > connectionStats.peakConnections) {
        connectionStats.peakConnections = connectionStats.activeConnections;
    }
    
    console.log(`[${connectionId}] New WebSocket connection from ${ipHash}`);
    
    // Rate limiting per IP
    if (!rateLimits.has(ipHash)) {
        rateLimits.set(ipHash, {
            count: 1,
            lastReset: Date.now()
        });
    } else {
        const ipData = rateLimits.get(ipHash);
        if (Date.now() - ipData.lastReset > 60 * 1000) {
            ipData.count = 1;
            ipData.lastReset = Date.now();
        } else {
            ipData.count++;
            if (ipData.count > MAX_CONNECTIONS_PER_IP) {
                console.log(`[${connectionId}] Rate limit exceeded for ${ipHash}`);
                ws.send(JSON.stringify({
                    type: 'error',
                    code: 429,
                    message: 'Too many connections from your IP. Please try again later.'
                }));
                ws.close();
                return;
            }
        }
    }
    
    // Set up heartbeat
    let isAlive = true;
    const heartbeatInterval = setInterval(() => {
        if (!isAlive) {
            console.log(`[${connectionId}] Terminating connection due to heartbeat failure`);
            ws.terminate();
            return;
        }
        
        isAlive = false;
        ws.ping(null, false, (err) => {
            if (err) {
                console.log(`[${connectionId}] Ping failed: ${err.message}`);
                ws.terminate();
            }
        });
    }, 30000);
    
    ws.on('pong', () => {
        isAlive = true;
    });
    
    ws.on('message', async (message) => {
        try {
            connectionStats.messagesProcessed++;
            
            const data = parseMessage(message);
            if (!data) {
                throw new Error('Invalid message format');
            }
            
            switch (data.type) {
                case 'subscribe':
                    await handleSubscription(ws, data, connectionId);
                    break;
                case 'unsubscribe':
                    handleUnsubscription(ws, data.userId, connectionId);
                    break;
                case 'ping':
                    ws.send(JSON.stringify({ type: 'pong', timestamp: Date.now() }));
                    break;
                case 'stats':
                    sendConnectionStats(ws);
                    break;
                default:
                    throw new Error('Unknown message type');
            }
        } catch (error) {
            console.error(`[${connectionId}] Error processing message:`, error);
            connectionStats.errors++;
            
            ws.send(JSON.stringify({
                type: 'error',
                code: 400,
                message: error.message || 'Invalid message format'
            }));
        }
    });
    
    ws.on('close', (code, reason) => {
        console.log(`[${connectionId}] WebSocket connection closed (${code}): ${reason}`);
        clearInterval(heartbeatInterval);
        cleanupConnection(ws);
        connectionStats.activeConnections--;
    });
    
    ws.on('error', (error) => {
        console.error(`[${connectionId}] WebSocket error:`, error);
        connectionStats.errors++;
    });
});

// Main functions
async function handleSubscription(ws, data, connectionId) {
    try {
        const userId = data.userId;
        
        if (!isValidSnowflake(userId)) {
            throw new Error('Invalid User ID format');
        }
        
        // Check cache first
        const cacheKey = `user_${userId}`;
        const cachedData = userCache.get(cacheKey);
        
        if (cachedData && Date.now() - cachedData.timestamp < CACHE_EXPIRATION) {
            console.log(`[${connectionId}] Serving cached data for user ${userId}`);
            ws.send(JSON.stringify({
                type: 'initial',
                data: cachedData.data,
                cached: true
            }));
            return;
        }
        
        // Verify user is in guild
        const inGuild = await isUserInGuild(userId);
        if (!inGuild) {
            const inviteLink = process.env.INVITE || 'https://discord.gg/invite';
            throw new Error(`User not in server. Join our server to track: ${inviteLink}`);
        }
        
        // Initialize subscription tracking
        if (!userSubscriptions[userId]) {
            userSubscriptions[userId] = new Set();
        }
        userSubscriptions[userId].add(ws);
        
        // Fetch user data
        const presence = await fetchUserPresence(userId);
        const fullData = await getFullUserData(userId, presence);
        
        // Cache the data
        userCache.set(cacheKey, {
            timestamp: Date.now(),
            data: fullData
        });
        
        // Send initial data
        ws.send(JSON.stringify({
            type: 'initial',
            data: fullData,
            cached: false
        }));
        
        console.log(`[${connectionId}] Subscribed to user ${userId}`);
    } catch (error) {
        console.error(`[${connectionId}] Error in handleSubscription:`, error);
        connectionStats.errors++;
        
        ws.send(JSON.stringify({
            type: 'error',
            code: error.code || 500,
            message: error.message || 'Subscription failed'
        }));
        
        ws.close();
    }
}

function handleUnsubscription(ws, userId, connectionId) {
    try {
        if (!userId || !isValidSnowflake(userId)) {
            throw new Error('Invalid User ID format');
        }
        
        if (userSubscriptions[userId]?.has(ws)) {
            userSubscriptions[userId].delete(ws);
            if (userSubscriptions[userId].size === 0) {
                delete userSubscriptions[userId];
            }
            console.log(`[${connectionId}] Unsubscribed from user ${userId}`);
        }
        
        ws.send(JSON.stringify({
            type: 'unsubscribe',
            success: true,
            userId: userId
        }));
    } catch (error) {
        console.error(`[${connectionId}] Error in handleUnsubscription:`, error);
        connectionStats.errors++;
        
        ws.send(JSON.stringify({
            type: 'error',
            code: 400,
            message: error.message || 'Unsubscription failed'
        }));
    }
}

async function fetchUserPresence(userId) {
    try {
        // Try to get user from cache first
        const cacheKey = `presence_${userId}`;
        const cachedPresence = userCache.get(cacheKey);
        
        if (cachedPresence && Date.now() - cachedPresence.timestamp < PRESENCE_UPDATE_INTERVAL) {
            return cachedPresence.data;
        }
        
        // Try to get from Discord client first
        const user = await discordClient.users.fetch(userId).catch(() => null);
        if (user?.presence) {
            const presenceData = formatPresenceData(user.presence);
            userCache.set(cacheKey, {
                timestamp: Date.now(),
                data: presenceData
            });
            return presenceData;
        }
        
        // Fallback to API request
        const response = await fetch(`https://discord.com/api/v10/users/${userId}/profile`, {
            headers: { 
                Authorization: `Bot ${process.env.DISCORD_BOT_TOKEN}`,
                'Content-Type': 'application/json'
            }
        });

        if (!response.ok) {
            if (response.status === 403) {
                throw new Error('API request forbidden - check bot permissions');
            }
            if (response.status === 404) {
                throw new Error('User not found');
            }
            throw new Error(`API request failed: ${response.status} ${response.statusText}`);
        }

        const data = await response.json();
        const presenceData = {
            user: { id: userId },
            status: data.user?.presence?.status || 'offline',
            client_status: data.user?.presence?.client_status || {},
            activities: data.user?.presence?.activities || []
        };
        
        userCache.set(cacheKey, {
            timestamp: Date.now(),
            data: presenceData
        });
        
        return presenceData;

    } catch (error) {
        console.error(`Error fetching presence for user ${userId}:`, error);
        connectionStats.errors++;
        
        // Return offline status if we can't fetch presence
        return {
            user: { id: userId },
            status: 'offline',
            client_status: lastOnlineData[userId] || { desktop: 'offline' },
            activities: []
        };
    }
}

async function getFullUserData(userId, presenceData) {
    const cacheKey = `userdata_${userId}`;
    const cachedData = userCache.get(cacheKey);
    
    if (cachedData && Date.now() - cachedData.timestamp < CACHE_EXPIRATION) {
        return cachedData.data;
    }
    
    let userData = {
        user: { 
            id: userId,
            username: '',
            global_name: '',
            discriminator: '0',
            avatar: null,
            bot: false,
            system: false,
            flags: 0
        },
        badges: [],
        bio: null,
        pronouns: null,
        status: 'offline',
        activities: [],
        avatarURL: null,
        bannerURL: null,
        accent_color: null,
        banner_color: null,
        premium_since: null,
        last_updated: Date.now()
    };
    
    try {
        // Get basic user info
        const discordUser = await discordClient.users.fetch(userId).catch(() => null);
        if (discordUser) {
            userData.user = {
                id: discordUser.id,
                username: discordUser.username,
                global_name: discordUser.globalName || discordUser.username,
                discriminator: discordUser.discriminator,
                avatar: discordUser.avatar,
                bot: discordUser.bot,
                system: discordUser.system,
                flags: discordUser.flags?.bitfield || 0
            };
            
            userData.avatarURL = discordUser.displayAvatarURL({ format: 'png', dynamic: true, size: 256 });
            userData.bannerURL = discordUser.bannerURL({ format: 'png', size: 512 });
            userData.accent_color = discordUser.hexAccentColor;
            
            // Process user flags (badges)
            if (discordUser.flags) {
                const flags = discordUser.flags.toArray();
                flags.forEach(flag => {
                    if (badgeInfo[flag]) {
                        userData.badges.push({
                            id: flag,
                            description: badgeInfo[flag].description,
                            color: badgeInfo[flag].color,
                            icon: badgeInfo[flag].icon
                        });
                    }
                });
            }
        }

        // Get profile data
        const response = await fetch(`https://discord.com/api/v10/users/${userId}/profile`, {
            headers: { 
                Authorization: `Bot ${process.env.DISCORD_BOT_TOKEN}`,
                'Content-Type': 'application/json'
            }
        });
        
        if (response.ok) {
            const profileData = await response.json();
            userData.bio = profileData.user?.bio || null;
            userData.pronouns = profileData.user?.pronouns || null;
            userData.banner_color = profileData.user?.banner_color || null;
            userData.premium_since = profileData.user?.premium_since || null;
            
            if (profileData.user?.premium_since) {
                userData.badges.push({
                    id: 'premium',
                    description: badgeInfo.premium.description,
                    color: badgeInfo.premium.color,
                    icon: badgeInfo.premium.icon
                });
            }
            
            if (profileData.user?.banner) {
                userData.bannerURL = `https://cdn.discordapp.com/banners/${userId}/${profileData.user.banner}.png?size=512`;
            }
        }

        // Add presence info
        if (presenceData) {
            userData.status = presenceData.status || 'offline';
            userData.activities = presenceData.activities || [];
            
            const clientStatus = presenceData.client_status || {};
            Object.keys(clientStatus).forEach(platform => {
                const status = clientStatus[platform] || 'offline';
                userData.badges.push({
                    id: `platform_${platform}`,
                    description: status === 'offline' ? 
                        `Last online from ${capitalize(platform)}` : 
                        `Online from ${capitalize(platform)}`,
                    status: status,
                    color: statusColors[status] || statusColors.offline,
                    icon: getPlatformIcon(platform)
                });
            });
        }
        
        // Cache the full user data
        userCache.set(cacheKey, {
            timestamp: Date.now(),
            data: userData
        });
        
    } catch (error) {
        console.error(`Error getting full data for user ${userId}:`, error);
        connectionStats.errors++;
    }
    
    return userData;
}

// Helper functions
function capitalize(str) {
    return str.charAt(0).toUpperCase() + str.slice(1).toLowerCase();
}

function isValidSnowflake(id) {
    return /^\d{17,20}$/.test(id);
}

async function isUserInGuild(userId) {
    try {
        const guildId = process.env.GUILD_ID;
        
        if (guildId) {
            // Check specific guild if configured
            const guild = discordClient.guilds.cache.get(guildId);
            if (guild) {
                await guild.members.fetch(userId).catch(() => null);
                return guild.members.cache.has(userId);
            }
        }
        
        // Fallback to checking all guilds
        for (const guild of discordClient.guilds.cache.values()) {
            try {
                await guild.members.fetch(userId).catch(() => null);
                if (guild.members.cache.has(userId)) {
                    return true;
                }
            } catch (error) {
                continue;
            }
        }
        return false;
    } catch (error) {
        console.error('Error checking guild membership:', error);
        connectionStats.errors++;
        return false;
    }
}

function formatPresenceData(presence) {
    if (!presence) {
        return {
            user: { id: 'unknown' },
            status: 'offline',
            client_status: {},
            activities: []
        };
    }
    
    return {
        user: { id: presence.userId || presence.user?.id || 'unknown' },
        status: presence.status || 'offline',
        client_status: presence.clientStatus || {},
        activities: presence.activities || []
    };
}

function lastOnlinePlatformHandler(userId, data) {
    if (!userId) return;
    
    if (data.status !== 'offline') {
        const platforms = {};
        for (const platform in data.client_status) {
            platforms[platform] = data.client_status[platform] || 'offline';
        }
        lastOnlineData[userId] = platforms;
    } else {
        offlineStatusStore[userId] = {
            user: { id: userId },
            status: 'offline',
            client_status: lastOnlineData[userId],
            activities: []
        };
    }
}

function broadcastUpdate(userId, data) {
    if (!userSubscriptions[userId]) return;
    
    const message = JSON.stringify({
        type: 'update',
        data: data,
        timestamp: Date.now()
    });
    
    for (const ws of userSubscriptions[userId]) {
        if (ws.readyState === WebSocket.OPEN) {
            ws.send(message, (err) => {
                if (err) {
                    console.error('Error sending update:', err);
                    connectionStats.errors++;
                }
            });
        }
    }
}

function cleanupConnection(ws) {
    for (const userId in userSubscriptions) {
        if (userSubscriptions[userId].has(ws)) {
            userSubscriptions[userId].delete(ws);
            if (userSubscriptions[userId].size === 0) {
                delete userSubscriptions[userId];
            }
        }
    }
}

function parseMessage(message) {
    try {
        if (typeof message !== 'string') {
            if (message instanceof Buffer || message instanceof ArrayBuffer) {
                message = message.toString('utf8');
            } else {
                throw new Error('Invalid message type');
            }
        }
        
        const data = JSON.parse(message);
        if (!data || typeof data !== 'object') {
            throw new Error('Invalid message format');
        }
        
        return data;
    } catch (error) {
        throw new Error('Failed to parse message: ' + error.message);
    }
}

function sendConnectionStats(ws) {
    if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({
            type: 'stats',
            data: {
                ...connectionStats,
                uptime: process.uptime(),
                memoryUsage: process.memoryUsage(),
                userCacheSize: userCache.size,
                subscribedUsers: Object.keys(userSubscriptions).length,
                version: API_VERSION
            }
        }));
    }
}

function getPlatformIcon(platform) {
    const icons = {
        desktop: 'https://discord.com/assets/5d6a5e9d7d77ac29116e.png',
        mobile: 'https://discord.com/assets/6f26a1c34b3cee8e1c97.png',
        web: 'https://discord.com/assets/6c5e6c8d5d6a5e9d7d77ac29116e.png'
    };
    return icons[platform] || 'https://discord.com/assets/6f26a1c34b3cee8e1c97.png';
}

function startBackgroundTasks() {
    // Clean up old cache entries
    setInterval(() => {
        const now = Date.now();
        let cleared = 0;
        
        for (const [key, value] of userCache.entries()) {
            if (now - value.timestamp > CACHE_EXPIRATION) {
                userCache.delete(key);
                cleared++;
            }
        }
        
        if (cleared > 0) {
            console.log(`Cleared ${cleared} expired cache entries`);
        }
    }, CACHE_EXPIRATION);
    
    // Check for inactive subscriptions
    setInterval(() => {
        const now = Date.now();
        let cleaned = 0;
        
        for (const userId in userSubscriptions) {
            const subscriptions = Array.from(userSubscriptions[userId]);
            let hasActive = false;
            
            for (const ws of subscriptions) {
                if (ws.readyState === WebSocket.OPEN) {
                    hasActive = true;
                    break;
                }
            }
            
            if (!hasActive) {
                delete userSubscriptions[userId];
                cleaned++;
            }
        }
        
        if (cleaned > 0) {
            console.log(`Cleaned up ${cleaned} inactive subscriptions`);
        }
    }, STATUS_CHECK_INTERVAL);
    
    // Reset rate limits
    setInterval(() => {
        const now = Date.now();
        let reset = 0;
        
        for (const [ip, data] of rateLimits.entries()) {
            if (now - data.lastReset > 60 * 1000) {
                rateLimits.delete(ip);
                reset++;
            }
        }
        
        if (reset > 0) {
            console.log(`Reset ${reset} rate limits`);
        }
    }, 60 * 1000);
}

// REST API endpoints
app.get('/api/version', (req, res) => {
    res.json({
        version: API_VERSION,
        status: 'online',
        timestamp: Date.now()
    });
});

app.get('/api/user/:id', async (req, res) => {
    try {
        const userId = req.params.id;
        
        if (!isValidSnowflake(userId)) {
            return res.status(400).json({
                error: 'Invalid user ID format'
            });
        }
        
        const presence = await fetchUserPresence(userId);
        const fullData = await getFullUserData(userId, presence);
        
        res.json(fullData);
    } catch (error) {
        console.error('API error:', error);
        res.status(500).json({
            error: error.message || 'Internal server error'
        });
    }
});

app.get('/api/stats', (req, res) => {
    res.json({
        ...connectionStats,
        uptime: process.uptime(),
        memoryUsage: process.memoryUsage(),
        version: API_VERSION
    });
});

// Error handling middleware
app.use((err, req, res, next) => {
    console.error('Unhandled error:', err);
    connectionStats.errors++;
    
    res.status(500).json({
        error: 'Internal server error',
        message: err.message
    });
});

// Process error handling
process.on('unhandledRejection', error => {
    console.error('Unhandled promise rejection:', error);
    connectionStats.errors++;
});

process.on('uncaughtException', error => {
    console.error('Uncaught exception:', error);
    connectionStats.errors++;
});

// Graceful shutdown
process.on('SIGTERM', () => {
    console.log('SIGTERM received. Shutting down gracefully...');
    
    // Close all WebSocket connections
    wss.clients.forEach(client => {
        client.close(1001, 'Server is shutting down');
    });
    
    // Close the WebSocket server
    wss.close(() => {
        console.log('WebSocket server closed');
    });
    
    // Close the HTTP server
    server.close(() => {
        console.log('HTTP server closed');
        process.exit(0);
    });
    
    // Force exit after timeout
    setTimeout(() => {
        console.error('Could not close connections in time, forcefully shutting down');
        process.exit(1);
    }, 10000);
});
