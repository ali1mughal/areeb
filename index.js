require('dotenv').config();
const express = require('express');
const WebSocket = require('ws');
const { Client, GatewayIntentBits, ActivityType, PermissionsBitField } = require('discord.js');
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
  VERSION: '1.4.1',
  REQUIRED_PERMISSIONS: [
    'ViewChannel',
    'ReadMessageHistory',
    'ViewGuildInsights',
    'ManageWebhooks',
    'ViewPresence',
    'ViewGuildMembers'
  ],
  REQUIRED_INTENTS: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildPresences,
    GatewayIntentBits.GuildMembers,
    GatewayIntentBits.DirectMessages,
    GatewayIntentBits.MessageContent
  ]
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

// Discord Client Setup with all required intents
const discordClient = new Client({
  intents: config.REQUIRED_INTENTS,
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
  console.log('Required Intents:', config.REQUIRED_INTENTS.map(i => GatewayIntentBits[i]).join(', '));
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
  getPlatformIcon: (platform) => statusConfig.platformIcons[platform] || statusConfig.platformIcons.mobile,
  checkPermissions: (guild) => {
    if (!guild) return false;
    const permissions = guild.members.me?.permissions;
    if (!permissions) return false;
    return config.REQUIRED_PERMISSIONS.every(perm => permissions.has(perm));
  },
  generateBotInvite: () => {
    try {
      const permissions = new PermissionsBitField([
        PermissionsBitField.Flags.ViewChannel,
        PermissionsBitField.Flags.ReadMessageHistory,
        PermissionsBitField.Flags.ViewGuildInsights,
        PermissionsBitField.Flags.ManageWebhooks,
        PermissionsBitField.Flags.ViewPresence,
        PermissionsBitField.Flags.ViewGuildMembers
      ]);
      
      const clientId = discordClient.user?.id || process.env.CLIENT_ID;
      if (!clientId) {
        throw new Error('Client ID not available');
      }
      
      return `https://discord.com/api/oauth2/authorize?client_id=${clientId}&permissions=${permissions.bitfield}&scope=bot%20applications.commands`;
    } catch (error) {
      console.error('Error generating bot invite:', error);
      return null;
    }
  }
};

// [Rest of your classes (WebSocketManager, PresenceManager, UserDataManager, SubscriptionManager) remain the same...]

// Initialize Managers
const presenceManager = new PresenceManager();
const userDataManager = new UserDataManager();
const subscriptionManager = new SubscriptionManager();
const webSocketManager = new WebSocketManager(wss);

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
  try {
    const inviteLink = utils.generateBotInvite();
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
      },
      botInvite: inviteLink,
      requiredPermissions: config.REQUIRED_PERMISSIONS,
      requiredIntents: config.REQUIRED_INTENTS.map(i => GatewayIntentBits[i])
    });
  } catch (error) {
    console.error('Root endpoint error:', error);
    res.status(500).json({
      error: 'Internal server error',
      message: error.message
    });
  }
});

// [Rest of your API routes remain the same...]

// Error Handling
app.use((err, req, res, next) => {
  console.error('Unhandled error:', err);
  dataStores.connectionStats.errors++;
  res.status(500).json({ 
    error: 'Internal server error',
    botInvite: utils.generateBotInvite(),
    requiredPermissions: config.REQUIRED_PERMISSIONS
  });
});

// [Rest of your process event handlers remain the same...]

// Discord Client Events
discordClient.on('ready', () => {
  console.log(`Logged in as ${discordClient.user.tag}`);
  console.log(`Bot is in ${discordClient.guilds.cache.size} servers:`);
  
  // Verify permissions in each guild
  discordClient.guilds.cache.forEach(guild => {
    const permissions = guild.members.me?.permissions;
    console.log(`- ${guild.name} (${guild.id})`);
    console.log(`  Permissions: ${permissions?.toArray().join(', ')}`);
    
    if (!utils.checkPermissions(guild)) {
      console.warn(`  WARNING: Missing required permissions in ${guild.name}`);
      console.warn(`  Required: ${config.REQUIRED_PERMISSIONS.join(', ')}`);
      console.warn(`  Invite with correct permissions: ${utils.generateBotInvite()}`);
    }
  });
  
  startBackgroundTasks();
});

// Login to Discord
discordClient.login(process.env.DISCORD_BOT_TOKEN)
  .catch(err => {
    console.error('Login failed:', err);
    process.exit(1);
  });
