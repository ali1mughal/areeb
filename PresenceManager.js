// managers/PresenceManager.js
class PresenceManager {
  constructor(dataStores, config) {
    if (!dataStores || typeof dataStores.userCache === 'undefined') {
      throw new Error('PresenceManager: Invalid dataStores object');
    }

    this.userCache = dataStores.userCache;
    this.lastOnlineData = dataStores.lastOnlineData || {};
    this.config = config;
  }

  updatePresence(userId, presence) {
    if (!userId || !presence) return;

    const timestamp = Date.now();
    this.userCache.set(userId, { presence, timestamp });

    if (presence.status !== 'offline') {
      this.lastOnlineData[userId] = timestamp;
    }
  }

  getPresence(userId) {
    const data = this.userCache.get(userId);
    return data
      ? data.presence
      : { status: 'offline', lastSeen: this.lastOnlineData[userId] || null };
  }
}

module.exports = PresenceManager;
