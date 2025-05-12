require('dotenv').config();
const express = require('express');
const WebSocket = require('ws');
const { Client, GatewayIntentBits, ActivityType } = require('discord.js');
const fetch = (...args) => import('node-fetch').then(({ default: fetch }) => fetch(...args));
const { v4: uuidv4 } = require('uuid');
const helmet = require('helmet');
const cors = require('cors');
const morgan = require('morgan');
const rateLimit = require('express-rate-limit');
const { createHash } = require('crypto');

// Configuration
const config = {
  PORT: process.env.PORT || 10000,
  CACHE_EXPIRATION: 5 * 60 * 1000, // 5 minutes
  PRESENCE_UPDATE_INTERVAL: 30 * 1000, // 30 seconds
  STATUS_CHECK_INTERVAL: 60 * 1000, // 1 minute
  MAX_CONNECTIONS_PER_IP: 5,
  VERSION: '1.2.0'
};

// Initialize Express
const app = express();
app.use(helmet());
app.use(cors({
  origin: process.env.ALLOWED_ORIGINS?.split(',') || '*'
}));
app.use(morgan('combined'));
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Rate limiting
const limiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 100,
  standardHeaders: true,
  legacyHeaders: false,
});
app.use(limiter);

// Discord Client Setup
const discordClient = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildPresences,
    GatewayIntentBits.GuildMembers,
    GatewayIntentBits.DirectMessages
  ],
  partials: ['USER', 'GUILD_MEMBER', 'PRESENCE'],
  presence: {
    status: 'online',
    activities: [{
      name: 'Presence Tracker',
      type: ActivityType.Watching
    }]
  }
});

// Server Initialization
const server = app.listen(config.PORT, () => {
  console.log(`Server v${config.VERSION} running on http://localhost:${config.PORT}`);
});

// WebSocket Server
const wss = new WebSocket.Server({ 
  server,
  clientTracking: true,
  maxPayload: 1024 * 1024 // 1MB
});

// Data Stores
const dataStores = {
  userCache: new Map(),
  lastOnlineData: {},
  userSubscriptions: {},
  offlineStatusStore: {},
  connectionStats: {
    totalConnections: 0,
    activeConnections: 0,
    peakConnections: 0,
    messagesProcessed: 0,
    errors: 0
  },
  rateLimits: new Map()
};

// Status Configuration
const statusConfig = {
  colors: {
    idle: "#f0b232",
    dnd: "#f23f43",
    online: "#23a55a",
    offline: "#80848e",
    streaming: "#593695",
    invisible: "#747f8d"
  },
  badges: {
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
    }
  },
  platformIcons: {
    desktop: 'https://discord.com/assets/5d6a5e9d7d77ac29116e.png',
    mobile: 'https://discord.com/assets/6f26a1c34b3cee8e1c97.png',
    web: 'https://discord.com/assets/6c5e6c8d5d6a5e9d7d77ac29116e.png'
  }
};

// Utility Functions
const utils = {
  capitalize: (str) => str.charAt(0).toUpperCase() + str.slice(1).toLowerCase(),
  isValidSnowflake: (id) => /^\d{17,20}$/.test(id),
  parseMessage: (message) => {
    try {
      if (typeof message !== 'string') {
        message = message.toString('utf8');
      }
      return JSON.parse(message);
    } catch (error) {
      throw new Error('Failed to parse message: ' + error.message);
    }
  },
  getPlatformIcon: (platform) => statusConfig.platformIcons[platform] || statusConfig.platformIcons.mobile
};

// Discord Client Events
discordClient.on('ready', () => {
  console.log(`Bot logged in as ${discordClient.user.tag}`);
  startBackgroundTasks();
});

discordClient.on('presenceUpdate', async (oldPresence, newPresence) => {
  try {
    const userId = newPresence.userId;
    if (!userId) return;

    const data = await presenceManager.formatPresenceData(newPresence);
    presenceManager.updateLastOnlinePlatform(userId, data);
    
    if (dataStores.userSubscriptions[userId]) {
      const fullData = await userDataManager.getFullUserData(userId, data);
      webSocketManager.broadcastUpdate(userId, fullData);
    }
  } catch (error) {
    console.error('Error in presenceUpdate:', error);
    dataStores.connectionStats.errors++;
  }
});

// WebSocket Manager
const webSocketManager = {
  handleConnection: (ws, req) => {
    const connectionId = uuidv4();
    const ip = req.headers['x-forwarded-for'] || req.socket.remoteAddress;
    const ipHash = createHash('sha256').update(ip).digest('hex').substring(0, 8);
    
    // Update connection stats
    dataStores.connectionStats.totalConnections++;
    dataStores.connectionStats.activeConnections++;
    if (dataStores.connectionStats.activeConnections > dataStores.connectionStats.peakConnections) {
      dataStores.connectionStats.peakConnections = dataStores.connectionStats.activeConnections;
    }
    
    console.log(`[${connectionId}] New connection from ${ipHash}`);
    
    // Rate limiting
    if (!this.checkRateLimit(ipHash)) {
      ws.send(JSON.stringify({
        type: 'error',
        code: 429,
        message: 'Too many connections from your IP'
      }));
      ws.close();
      return;
    }
    
    // Heartbeat
    let isAlive = true;
    const heartbeatInterval = setInterval(() => {
      if (!isAlive) return ws.terminate();
      isAlive = false;
      ws.ping(null, false, (err) => err && ws.terminate());
    }, 30000);
    
    ws.on('pong', () => { isAlive = true; });
    
    ws.on('message', async (message) => {
      try {
        dataStores.connectionStats.messagesProcessed++;
        const data = utils.parseMessage(message);
        
        switch (data.type) {
          case 'subscribe':
            await subscriptionManager.handleSubscription(ws, data, connectionId);
            break;
          case 'unsubscribe':
            subscriptionManager.handleUnsubscription(ws, data.userId, connectionId);
            break;
          case 'ping':
            ws.send(JSON.stringify({ type: 'pong', timestamp: Date.now() }));
            break;
          case 'stats':
            this.sendConnectionStats(ws);
            break;
          default:
            throw new Error('Unknown message type');
        }
      } catch (error) {
        console.error(`[${connectionId}] Message error:`, error);
        dataStores.connectionStats.errors++;
        ws.send(JSON.stringify({
          type: 'error',
          code: 400,
          message: error.message || 'Invalid message'
        }));
      }
    });
    
    ws.on('close', () => {
      clearInterval(heartbeatInterval);
      this.cleanupConnection(ws);
      dataStores.connectionStats.activeConnections--;
    });
    
    ws.on('error', (error) => {
      console.error(`[${connectionId}] WebSocket error:`, error);
      dataStores.connectionStats.errors++;
    });
  },
  
  checkRateLimit: (ipHash) => {
    if (!dataStores.rateLimits.has(ipHash)) {
      dataStores.rateLimits.set(ipHash, { count: 1, lastReset: Date.now() });
      return true;
    }
    
    const ipData = dataStores.rateLimits.get(ipHash);
    if (Date.now() - ipData.lastReset > 60000) {
      ipData.count = 1;
      ipData.lastReset = Date.now();
      return true;
    }
    
    if (ipData.count++ >= config.MAX_CONNECTIONS_PER_IP) {
      return false;
    }
    return true;
  },
  
  broadcastUpdate: (userId, data) => {
    if (!dataStores.userSubscriptions[userId]) return;
    
    const message = JSON.stringify({
      type: 'update',
      data: data,
      timestamp: Date.now()
    });
    
    for (const ws of dataStores.userSubscriptions[userId]) {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(message, (err) => err && console.error('Send error:', err));
      }
    }
  },
  
  cleanupConnection: (ws) => {
    for (const userId in dataStores.userSubscriptions) {
      if (dataStores.userSubscriptions[userId].has(ws)) {
        dataStores.userSubscriptions[userId].delete(ws);
        if (dataStores.userSubscriptions[userId].size === 0) {
          delete dataStores.userSubscriptions[userId];
        }
      }
    }
  },
  
  sendConnectionStats: (ws) => {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({
        type: 'stats',
        data: {
          ...dataStores.connectionStats,
          uptime: process.uptime(),
          memoryUsage: process.memoryUsage(),
          userCacheSize: dataStores.userCache.size,
          subscribedUsers: Object.keys(dataStores.userSubscriptions).length,
          version: config.VERSION
        }
      }));
    }
  }
};

// Presence Manager
const presenceManager = {
  formatPresenceData: (presence) => {
    if (!presence) return {
      user: { id: 'unknown' },
      status: 'offline',
      client_status: {},
      activities: []
    };
    
    return {
      user: { id: presence.userId || presence.user?.id || 'unknown' },
      status: presence.status || 'offline',
      client_status: presence.clientStatus || {},
      activities: presence.activities || []
    };
  },
  
  updateLastOnlinePlatform: (userId, data) => {
    if (!userId) return;
    
    if (data.status !== 'offline') {
      const platforms = {};
      for (const platform in data.client_status) {
        platforms[platform] = data.client_status[platform] || 'offline';
      }
      dataStores.lastOnlineData[userId] = platforms;
    } else {
      dataStores.offlineStatusStore[userId] = {
        user: { id: userId },
        status: 'offline',
        client_status: dataStores.lastOnlineData[userId],
        activities: []
      };
    }
  },
  
  fetchUserPresence: async (userId) => {
    try {
      // Check cache first
      const cacheKey = `presence_${userId}`;
      const cached = dataStores.userCache.get(cacheKey);
      if (cached && Date.now() - cached.timestamp < config.PRESENCE_UPDATE_INTERVAL) {
        return cached.data;
      }
      
      // Try Discord client first
      const user = await discordClient.users.fetch(userId).catch(() => null);
      if (user?.presence) {
        const presenceData = this.formatPresenceData(user.presence);
        dataStores.userCache.set(cacheKey, {
          timestamp: Date.now(),
          data: presenceData
        });
        return presenceData;
      }
      
      // Fallback to API
      const response = await fetch(`https://discord.com/api/v10/users/${userId}/profile`, {
        headers: { 
          Authorization: `Bot ${process.env.DISCORD_BOT_TOKEN}`,
          'Content-Type': 'application/json'
        }
      });

      if (!response.ok) {
        if (response.status === 403) {
          throw new Error('Missing required permissions. Ensure bot has "View Presence" and "View Guild Members" permissions.');
        }
        throw new Error(`API request failed: ${response.status} ${response.statusText}`);
      }

      const profileData = await response.json();
      const presenceData = {
        user: { id: userId },
        status: profileData.user?.presence?.status || 'offline',
        client_status: profileData.user?.presence?.client_status || {},
        activities: profileData.user?.presence?.activities || []
      };

      dataStores.userCache.set(cacheKey, {
        timestamp: Date.now(),
        data: presenceData
      });

      return presenceData;
    } catch (error) {
      console.error(`Error fetching presence for user ${userId}:`, error);
      dataStores.connectionStats.errors++;
      
      return {
        user: { id: userId },
        status: 'offline',
        client_status: dataStores.lastOnlineData[userId] || {},
        activities: []
      };
    }
  }
};

// User Data Manager
const userDataManager = {
  getFullUserData: async (userId, presenceData) => {
    const cacheKey = `userdata_${userId}`;
    const cached = dataStores.userCache.get(cacheKey);
    if (cached && Date.now() - cached.timestamp < config.CACHE_EXPIRATION) {
      return cached.data;
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
          discordUser.flags.toArray().forEach(flag => {
            if (statusConfig.badges[flag]) {
              userData.badges.push({
                id: flag,
                description: statusConfig.badges[flag].description,
                color: statusConfig.badges[flag].color,
                icon: statusConfig.badges[flag].icon
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
            description: statusConfig.badges.premium.description,
            color: statusConfig.badges.premium.color,
            icon: statusConfig.badges.premium.icon
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
              `Last online from ${utils.capitalize(platform)}` : 
              `Online from ${utils.capitalize(platform)}`,
            status: status,
            color: statusConfig.colors[status] || statusConfig.colors.offline,
            icon: utils.getPlatformIcon(platform)
          });
        });
      }
      
      // Cache the data
      dataStores.userCache.set(cacheKey, {
        timestamp: Date.now(),
        data: userData
      });
      
    } catch (error) {
      console.error(`Error getting full data for user ${userId}:`, error);
      dataStores.connectionStats.errors++;
    }
    
    return userData;
  }
};

// Subscription Manager
const subscriptionManager = {
  handleSubscription: async (ws, data, connectionId) => {
    try {
      const userId = data.userId;
      
      if (!utils.isValidSnowflake(userId)) {
        throw new Error('Invalid User ID format');
      }
      
      // Check cache first
      const cacheKey = `user_${userId}`;
      const cached = dataStores.userCache.get(cacheKey);
      if (cached && Date.now() - cached.timestamp < config.CACHE_EXPIRATION) {
        console.log(`[${connectionId}] Serving cached data for ${userId}`);
        ws.send(JSON.stringify({
          type: 'initial',
          data: cached.data,
          cached: true
        }));
        return;
      }
      
      // Verify guild membership
      const inGuild = await this.isUserInSharedGuild(userId);
      if (!inGuild) {
        throw new Error(`User not in any shared server. ${process.env.INVITE ? `Join our server: ${process.env.INVITE}` : ''}`);
      }
      
      // Initialize subscription
      if (!dataStores.userSubscriptions[userId]) {
        dataStores.userSubscriptions[userId] = new Set();
      }
      dataStores.userSubscriptions[userId].add(ws);
      
      // Fetch data
      const presence = await presenceManager.fetchUserPresence(userId);
      const fullData = await userDataManager.getFullUserData(userId, presence);
      
      // Cache and send
      dataStores.userCache.set(cacheKey, {
        timestamp: Date.now(),
        data: fullData
      });
      
      ws.send(JSON.stringify({
        type: 'initial',
        data: fullData,
        cached: false
      }));
      
      console.log(`[${connectionId}] Subscribed to ${userId}`);
    } catch (error) {
      console.error(`[${connectionId}] Subscription error:`, error);
      dataStores.connectionStats.errors++;
      
      ws.send(JSON.stringify({
        type: 'error',
        code: error.code || 500,
        message: error.message || 'Subscription failed'
      }));
      
      ws.close();
    }
  },
  
  handleUnsubscription: (ws, userId, connectionId) => {
    try {
      if (!userId || !utils.isValidSnowflake(userId)) {
        throw new Error('Invalid User ID');
      }
      
      if (dataStores.userSubscriptions[userId]?.has(ws)) {
        dataStores.userSubscriptions[userId].delete(ws);
        if (dataStores.userSubscriptions[userId].size === 0) {
          delete dataStores.userSubscriptions[userId];
        }
        console.log(`[${connectionId}] Unsubscribed from ${userId}`);
      }
      
      ws.send(JSON.stringify({
        type: 'unsubscribe',
        success: true,
        userId: userId
      }));
    } catch (error) {
      console.error(`[${connectionId}] Unsubscription error:`, error);
      dataStores.connectionStats.errors++;
      
      ws.send(JSON.stringify({
        type: 'error',
        code: 400,
        message: error.message || 'Unsubscription failed'
      }));
    }
  },
  
  isUserInSharedGuild: async (userId) => {
    try {
      const guildId = process.env.GUILD_ID;
      
      if (guildId) {
        const guild = discordClient.guilds.cache.get(guildId);
        if (guild) {
          await guild.members.fetch(userId).catch(() => null);
          return guild.members.cache.has(userId);
        }
      }
      
      for (const guild of discordClient.guilds.cache.values()) {
        try {
          await guild.members.fetch(userId).catch(() => null);
          if (guild.members.cache.has(userId)) {
            return true;
          }
        } catch {
          continue;
        }
      }
      return false;
    } catch (error) {
      console.error('Guild check error:', error);
      dataStores.connectionStats.errors++;
      return false;
    }
  }
};

// Background Tasks
function startBackgroundTasks() {
  // Cache cleanup
  setInterval(() => {
    const now = Date.now();
    let cleared = 0;
    
    for (const [key, value] of dataStores.userCache.entries()) {
      if (now - value.timestamp > config.CACHE_EXPIRATION) {
        dataStores.userCache.delete(key);
        cleared++;
      }
    }
    
    if (cleared > 0) {
      console.log(`Cleared ${cleared} expired cache entries`);
    }
  }, config.CACHE_EXPIRATION);
  
  // Subscription cleanup
  setInterval(() => {
    let cleaned = 0;
    
    for (const userId in dataStores.userSubscriptions) {
      const subscriptions = Array.from(dataStores.userSubscriptions[userId]);
      let hasActive = false;
      
      for (const ws of subscriptions) {
        if (ws.readyState === WebSocket.OPEN) {
          hasActive = true;
          break;
        }
      }
      
      if (!hasActive) {
        delete dataStores.userSubscriptions[userId];
        cleaned++;
      }
    }
    
    if (cleaned > 0) {
      console.log(`Cleaned ${cleaned} inactive subscriptions`);
    }
  }, config.STATUS_CHECK_INTERVAL);
  
  // Rate limit reset
  setInterval(() => {
    const now = Date.now();
    let reset = 0;
    
    for (const [ip, data] of dataStores.rateLimits.entries()) {
      if (now - data.lastReset > 60000) {
        dataStores.rateLimits.delete(ip);
        reset++;
      }
    }
    
    if (reset > 0) {
      console.log(`Reset ${reset} rate limits`);
    }
  }, 60000);
}

// REST API Routes
app.get('/', (req, res) => {
  res.status(200).json({
    status: 'online',
    service: 'Discord Presence Tracker',
    version: config.VERSION,
    endpoints: {
      websocket: `ws://${req.headers.host}`,
      api: {
        presence: '/api/presence/:userId',
        user: '/api/user/:userId',
        stats: '/api/stats'
      }
    }
  });
});

app.get('/api/presence/:userId', async (req, res) => {
  try {
    const userId = req.params.userId;
    if (!utils.isValidSnowflake(userId)) {
      return res.status(400).json({ error: 'Invalid user ID' });
    }

    const presence = await presenceManager.fetchUserPresence(userId);
    res.json(presence);
  } catch (error) {
    console.error('API error:', error);
    dataStores.connectionStats.errors++;
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.get('/api/user/:userId', async (req, res) => {
  try {
    const userId = req.params.userId;
    if (!utils.isValidSnowflake(userId)) {
      return res.status(400).json({ error: 'Invalid user ID' });
    }

    const presence = await presenceManager.fetchUserPresence(userId);
    const userData = await userDataManager.getFullUserData(userId, presence);
    res.json(userData);
  } catch (error) {
    console.error('API error:', error);
    dataStores.connectionStats.errors++;
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.get('/api/stats', (req, res) => {
  res.json({
    ...dataStores.connectionStats,
    uptime: process.uptime(),
    memoryUsage: process.memoryUsage(),
    version: config.VERSION
  });
});

// Error Handling
app.use((err, req, res, next) => {
  console.error('Unhandled error:', err);
  dataStores.connectionStats.errors++;
  res.status(500).json({ error: 'Internal server error' });
});

process.on('unhandledRejection', error => {
  console.error('Unhandled rejection:', error);
  dataStores.connectionStats.errors++;
});

process.on('uncaughtException', error => {
  console.error('Uncaught exception:', error);
  dataStores.connectionStats.errors++;
});

// Graceful Shutdown
process.on('SIGTERM', () => {
  console.log('Shutting down gracefully...');
  
  wss.clients.forEach(client => {
    client.close(1001, 'Server shutdown');
  });
  
  wss.close(() => {
    server.close(() => {
      discordClient.destroy();
      process.exit(0);
    });
  });
  
  setTimeout(() => {
    console.error('Forcing shutdown');
    process.exit(1);
  }, 10000);
});

// Initialize WebSocket
wss.on('connection', webSocketManager.handleConnection);

// Login to Discord
discordClient.login(process.env.DISCORD_BOT_TOKEN)
  .catch(err => {
    console.error('Login failed:', err);
    process.exit(1);
  });
