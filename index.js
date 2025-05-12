const express = require('express');
const WebSocket = require('ws');
const { Client, GatewayIntentBits } = require('discord.js');
const fetch = require('node-fetch');
require('dotenv').config();

const app = express();
const port = process.env.PORT || 10000;

// Create Discord client with necessary intents
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
discordClient.login(process.env.DISCORD_BOT_TOKEN);
discordClient.on('ready', () => {
    console.log(`Discord bot logged in as ${discordClient.user.tag}`);
});

// Presence update handler
discordClient.on('presenceUpdate', async (oldPresence, newPresence) => {
    const userId = newPresence.userId;
    const data = await formatPresenceData(newPresence);
    
    lastOnlinePlatformHandler(userId, data);
    
    if (userSubscriptions[userId]) {
        broadcastUpdate(userId, await getFullUserData(data));
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
    const userId = data.userId;
    
    // Validate user ID
    if (!isValidSnowflake(userId)) {
        ws.send(JSON.stringify({
            type: 'error',
            code: 400,
            message: 'Invalid User ID format'
        }));
        return ws.close();
    }
    
    // Check if user is in guild
    if (!await isUserInGuild(userId)) {
        const inviteLink = process.env.INVITE || 'https://discord.gg/invite';
        ws.send(JSON.stringify({
            type: 'error',
            code: 404,
            message: `User not in server. Join our server to track: ${inviteLink}`
        }));
        return ws.close();
    }
    
    // Initialize subscription
    if (!userSubscriptions[userId]) {
        userSubscriptions[userId] = new Set();
    }
    userSubscriptions[userId].add(ws);
    
    // Send initial data
    const presence = await fetchUserPresence(userId);
    const fullData = await getFullUserData(presence);
    ws.send(JSON.stringify({
        type: 'initial',
        data: fullData
    }));
    
    console.log(`Subscribed to user ${userId}`);
}

async function fetchUserPresence(userId) {
    try {
        // Try to get from Discord.js cache first
        const user = discordClient.users.cache.get(userId);
        if (user && user.presence) {
            return formatPresenceData(user.presence);
        }
        
        // Fallback to API request if not in cache
        const response = await fetch(`https://discord.com/api/v9/users/${userId}/profile`, {
            headers: { Authorization: process.env.DISCORD_TOKEN }
        });
        
        if (!response.ok) {
            throw new Error(`API request failed with status ${response.status}`);
        }
        
        return await response.json();
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

async function getFullUserData(presenceData) {
    const userId = presenceData.user.id;
    let userData;
    
    // Check cache first
    if (userCache.has(userId)) {
        const cached = userCache.get(userId);
        if (Date.now() - cached.timestamp < 300000) { // 5 minute cache
            userData = cached.data;
        }
    }
    
    // Fetch fresh data if not in cache
    if (!userData) {
        try {
            const response = await fetch(`https://discord.com/api/v9/users/${userId}/profile`, {
                headers: { Authorization: process.env.DISCORD_TOKEN }
            });
            
            if (response.ok) {
                userData = await response.json();
                // Clean up unnecessary data
                delete userData.mutual_guilds;
                delete userData.guild_badges;
                
                // Cache the data
                userCache.set(userId, {
                    data: userData,
                    timestamp: Date.now()
                });
            }
        } catch (error) {
            console.error(`Error fetching full profile for user ${userId}:`, error);
            // Fallback to basic data
            userData = {
                user: { id: userId },
                badges: [],
                bio: null,
                pronouns: null
            };
        }
    }
    
    // Add presence information
    try {
        const clientStatus = presenceData.client_status || {};
        
        // Add platform badges
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
        
        userData.status = presenceData.status || 'offline';
        userData.activities = presenceData.activities || [];
        
    } catch (error) {
        console.error(`Error processing presence data for user ${userId}:`, error);
        // Send error to webhook if configured
        if (process.env.ERROR_WEBHOOK) {
            await sendErrorToWebhook(userId, error);
        }
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
        // Check if user is in any shared guild
        for (const guild of discordClient.guilds.cache.values()) {
            if (guild.members.cache.has(userId)) {
                return true;
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
            
            // Clean up empty subscriptions
            if (userSubscriptions[userId].size === 0) {
                delete userSubscriptions[userId];
            }
        }
    }
}

async function sendErrorToWebhook(userId, error) {
    try {
        await fetch(process.env.ERROR_WEBHOOK, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                embeds: [{
                    title: "Presence Tracking Error",
                    description: `Error processing data for user <@${userId}>:\n\`\`\`${error.stack || error.message}\`\`\``,
                    color: 0xff0000,
                    timestamp: new Date().toISOString()
                }]
            })
        });
    } catch (webhookError) {
        console.error('Failed to send error to webhook:', webhookError);
    }
}

// Basic route
app.get('/', (req, res) => {
    res.send('Discord Presence WebSocket Server is running');
});