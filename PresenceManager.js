// managers/PresenceManager.js
class PresenceManager {
  constructor(dataStores, config) {
    this.userCache = dataStores.userCache;
    this.lastOnlineData = dataStores.lastOnlineData;
    this.config = config;
  }

  updatePresence(userId, presence) {
    if (!userId || !presence) return;

    const timestamp = Date.now();
    this.userCache.set(userId, {
      presence,
      timestamp
    });

    // Save last online timestamp for offline fallback
    if (presence.status !== 'offline') {
      this.lastOnlineData[userId] = timestamp;
    }

    console.log(`Updated presence for ${userId}: ${presence.status}`);
  }

  getPresence(userId) {
    const data = this.userCache.get(userId);
    if (data) {
      return data.presence;
    }

    return {
      status: 'offline',
      lastSeen: this.lastOnlineData[userId] || null
    };
  }
}

module.exports = PresenceManager;
