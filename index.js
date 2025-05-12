const express = require('express');
const WebSocket = require('ws');
const { Client, GatewayIntentBits } = require('discord.js');
const fetch = (...args) => import('node-fetch').then(({default: fetch}) => fetch(...args));
require('dotenv').config();

const app = express();
const port = process.env.PORT || 10000;

// Create Discord client with all necessary intents
const discordClient = new Client({
    intents: [
        GatewayIntentBits.Guilds,
        GatewayIntentBits.GuildPresences,
        GatewayIntentBits.GuildMembers
    ]
});

// Initialize server
const server = app.listen(port, () => {
    console.log(`Server running on http://localhost:${port}`);
});

// WebSocket server setup
const wss = new WebSocket.Server({ server });

// Data stores
const userCache = new Map();
const lastOnlineData = {};
const userSubscriptions = {};
const offlineStatusStore = {};

// Status colors mapping
const statusColors = {
    idle: "#f0b232",
    dnd: "#f23f43",
    online: "#23a55a",
    offline: "#80848e",
    streaming: "#593695"
};

// Connect to Discord
discordClient.login(process.env.DISCORD_BOT_TOKEN)
    .then(() => console.log(`Bot logged in as ${discordClient.user.tag}`))
    .catch(err => console.error('Login failed:', err));

// Presence update handler
discordClient.on('presenceUpdate', async (oldPresence, newPresence) => {
    try {
        const userId = newPresence.userId;
        const data = await formatPresenceData(newPresence);
        
        lastOnlinePlatformHandler(userId, data);
        
        if (userSubscriptions[userId]) {
            const fullData = await getFullUserData(userId, data);
            broadcastUpdate(userId, fullData);
        }
    } catch (error) {
        console.error('Error in presenceUpdate:', error);
    }
});

// WebSocket connection handler
wss.on('connection', (ws) => {
    console.log('New WebSocket connection');
    
    ws.on('message', async (message) => {
        try {
            const data = JSON.parse(message);
            
            if (data.type === 'subscribe') {
                await handleSubscription(ws, data);
            }
        } catch (error) {
            console.error('Error processing message:', error);
            ws.send(JSON.stringify({
                type: 'error',
                code: 400,
                message: 'Invalid message format'
            }));
        }
    });

    ws.on('close', () => {
        cleanupConnection(ws);
        console.log('WebSocket connection closed');
    });
});

// Main functions
async function handleSubscription(ws, data) {
    try {
        const userId = data.userId;
        
        if (!isValidSnowflake(userId)) {
            ws.send(JSON.stringify({
                type: 'error',
                code: 400,
                message: 'Invalid User ID format'
            }));
            return ws.close();
        }
        
        if (!await isUserInGuild(userId)) {
            const inviteLink = process.env.INVITE || 'https://discord.gg/invite';
            ws.send(JSON.stringify({
                type: 'error',
                code: 404,
                message: `User not in server. Join our server to track: ${inviteLink}`
            }));
            return ws.close();
        }
        
        if (!userSubscriptions[userId]) {
            userSubscriptions[userId] = new Set();
        }
        userSubscriptions[userId].add(ws);
        
        const presence = await fetchUserPresence(userId);
        const fullData = await getFullUserData(userId, presence);
        
        ws.send(JSON.stringify({
            type: 'initial',
            data: fullData
        }));
        
        console.log(`Subscribed to user ${userId}`);
    } catch (error) {
        console.error('Error in handleSubscription:', error);
    }
}

async function fetchUserPresence(userId) {
    try {
        // Try to get user from cache first
        const user = await discordClient.users.fetch(userId);
        if (user && user.presence) {
            return formatPresenceData(user.presence);
        }
        
        // Fallback to API request
        const response = await fetch(`https://discord.com/api/v10/users/${userId}/profile`, {
            headers: { 
                Authorization: `Bot ${process.env.DISCORD_BOT_TOKEN}`,
                'Content-Type': 'application/json'
            }
        });

        if (!response.ok) {
            throw new Error(`API request failed: ${response.status} ${response.statusText}`);
        }

        const data = await response.json();
        return {
            user: { id: userId },
            status: data.user.presence.status || 'offline',
            client_status: data.user.presence.client_status || {},
            activities: data.user.presence.activities || []
        };

    } catch (error) {
        console.error(`Error fetching presence for user ${userId}:`, error);
        return {
            user: { id: userId },
            status: 'offline',
            client_status: lastOnlineData[userId] || { desktop: 'offline' },
            activities: []
        };
    }
}

async function getFullUserData(userId, presenceData) {
    let userData = {
        user: { 
            id: userId,
            username: '',
            discriminator: '',
            avatar: null,
            bot: false
        },
        badges: [],
        bio: null,
        pronouns: null,
        status: 'offline',
        activities: [],
        avatarURL: null
    };
    
    try {
        // Get basic user info
        const discordUser = await discordClient.users.fetch(userId);
        if (discordUser) {
            userData.user = {
                id: discordUser.id,
                username: discordUser.username,
                discriminator: discordUser.discriminator,
                avatar: discordUser.avatar,
                bot: discordUser.bot
            };
            userData.avatarURL = discordUser.displayAvatarURL({ format: 'png', dynamic: true, size: 256 });
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
            userData.bio = profileData.user.bio || null;
            userData.pronouns = profileData.user.pronouns || null;
            
            if (profileData.user.premium_since) {
                userData.badges.push({
                    id: 'premium',
                    description: 'Nitro Subscriber',
                    color: '#ff73fa'
                });
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
                    id: platform,
                    description: status === 'offline' ? 
                        `Last online from ${capitalize(platform)}` : 
                        `Online from ${capitalize(platform)}`,
                    status: status,
                    color: statusColors[status] || statusColors.offline
                });
            });
        }
        
    } catch (error) {
        console.error(`Error getting full data for user ${userId}:`, error);
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
        for (const guild of discordClient.guilds.cache.values()) {
            try {
                await guild.members.fetch(userId);
                return true;
            } catch {
                continue;
            }
        }
        return false;
    } catch (error) {
        console.error('Error checking guild membership:', error);
        return false;
    }
}

function formatPresenceData(presence) {
    return {
        user: { id: presence.userId },
        status: presence.status,
        client_status: presence.clientStatus || {},
        activities: presence.activities || []
    };
}

function lastOnlinePlatformHandler(userId, data) {
    if (data.status !== 'offline') {
        const platforms = {};
        for (const platform in data.client_status) {
            platforms[platform] = 'offline';
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
    
    for (const ws of userSubscriptions[userId]) {
        if (ws.readyState === WebSocket.OPEN) {
            ws.send(JSON.stringify({
                type: 'update',
                data: data
            }));
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

// Basic route
app.get('/', (req, res) => {
    res.send('Discord Presence WebSocket Server is running');
});

// Error handling
process.on('unhandledRejection', error => {
    console.error('Unhandled promise rejection:', error);
});
