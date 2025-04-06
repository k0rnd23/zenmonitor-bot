TELEGRAM_BOT_TOKEN = "TOKEN"
DEFAULT_CHECK_INTERVAL_SECONDS = 2 * 60 # check every 2 minutes
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
ADMIN_CHAT_IDS = [6331888034]

REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_DB = 0
REDIS_PASSWORD = None

# helps avoid collisions if Redis is used for other things
REDIS_PREFIX = "zenmonitor:"

# how many notified item URLs to remember per task (to prevent unbounded memory growth)
# Set to 0 or None for no limit (use with caution)
MAX_NOTIFIED_HISTORY_PER_TASK = 0
