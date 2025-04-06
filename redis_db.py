import redis
import json
import logging
from config import (
    REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_PASSWORD,
    REDIS_PREFIX, MAX_NOTIFIED_HISTORY_PER_TASK
)
logger = logging.getLogger(__name__)
try:
    pool = redis.ConnectionPool(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        password=REDIS_PASSWORD,
        decode_responses=True
    )
    r = redis.Redis(connection_pool=pool)
    r.ping()
    logger.info(f"Successfully connected to Redis at {REDIS_HOST}:{REDIS_PORT} DB {REDIS_DB}")
except redis.exceptions.ConnectionError as e:
    logger.fatal(f"Could not connect to Redis: {e}", exc_info=True)
    raise SystemExit(f"Fatal: Unable to connect to Redis - {e}")
except Exception as e:
    logger.fatal(f"An unexpected error occurred setting up Redis connection: {e}", exc_info=True)
    raise SystemExit(f"Fatal: Unexpected Redis setup error - {e}")

def key_task(task_id):
    return f"{REDIS_PREFIX}task:{task_id}"

def key_notified(task_id):
    return f"{REDIS_PREFIX}notified:{task_id}"

def key_chat_tasks(chat_id):
    return f"{REDIS_PREFIX}chat_tasks:{chat_id}"

def key_all_tasks():
    return f"{REDIS_PREFIX}all_tasks"

def key_next_task_id():
    return f"{REDIS_PREFIX}next_task_id"

def key_all_chats():
    return f"{REDIS_PREFIX}all_chats"

def _parse_task_hash(task_hash):
    """Converts a Redis hash (dict of strings) back into a task dict with correct types"""
    if not task_hash:
        return None
    try:
        parsed = task_hash.copy()
        parsed['id'] = int(parsed.get('id', 0))
        parsed['chat_id'] = int(parsed.get('chat_id', 0))
        parsed['max_price'] = float(parsed.get('max_price', 0.0))
        parsed['sort_options'] = parsed.get('sort_options') if parsed.get('sort_options') else None
        max_mins_str = parsed.get('max_minutes_left')
        parsed['max_minutes_left'] = int(max_mins_str) if max_mins_str and max_mins_str.isdigit() else None
        return parsed
    except (ValueError, TypeError, KeyError) as e:
        logger.error(f"Error parsing task hash data: {task_hash}. Error: {e}", exc_info=True)
        return None

def add_task(chat_id, platform, query, max_price, sort_options=None, max_minutes_left=None):
    """Adds a new task to Redis"""
    task_id = -1
    try:
        task_id = r.incr(key_next_task_id())
        logger.info(f"Generated new task ID: {task_id}")

        task_data = {
            'id': str(task_id),
            'chat_id': str(chat_id),
            'platform': platform,
            'query': query,
            'max_price': str(max_price),
            'sort_options': sort_options or "",
            'max_minutes_left': str(max_minutes_left) if max_minutes_left is not None else "",
        }
        num_fields_expected = len(task_data)

        pipe = r.pipeline()
        pipe.hset(key_task(task_id), mapping=task_data)
        pipe.sadd(key_chat_tasks(chat_id), task_id)
        pipe.sadd(key_all_tasks(), task_id)
        pipe.sadd(key_all_chats(), chat_id)
        results = pipe.execute()

        if (results[0] == num_fields_expected and
            results[1] == 1 and
            results[2] == 1):
            logger.info(f"Successfully added task {task_id} for chat {chat_id} via Redis pipeline. Results: {results}")
            return task_id
        else:

            reason = []
            if results[0] != num_fields_expected: reason.append(f"HSET fields added {results[0]} != expected {num_fields_expected}")
            if results[1] != 1: reason.append("SADD chat_tasks failed")
            if results[2] != 1: reason.append("SADD all_tasks failed")
            logger.warning(f"Redis pipeline execution for adding task {task_id} failed critical checks. Reason(s): {'; '.join(reason)}. Full Results: {results}")

            try:
                logger.warning(f"Attempting cleanup for failed task add {task_id}")
                r.delete(key_task(task_id))
                r.srem(key_chat_tasks(chat_id), task_id)
                r.srem(key_all_tasks(), task_id)

                if r.scard(key_chat_tasks(chat_id)) == 0:
                    r.srem(key_all_chats(), chat_id)
            except Exception as cleanup_err:
                 logger.error(f"Error during cleanup after failed task add {task_id}: {cleanup_err}")
            return None
    except redis.RedisError as e:
        logger.error(f"Redis error adding task {task_id if task_id > 0 else '(pre-ID)'} for chat {chat_id}: {e}", exc_info=True)
        return None
    except Exception as e:
        logger.error(f"Unexpected error adding task {task_id if task_id > 0 else '(pre-ID)'} for chat {chat_id}: {e}", exc_info=True)
        return None

def remove_task(task_id, chat_id):
    """Removes a specific task belonging to a chat from Redis"""
    task_key = key_task(task_id)
    notified_key = key_notified(task_id)
    chat_tasks_key = key_chat_tasks(chat_id)
    all_tasks_key = key_all_tasks()
    all_chats_key = key_all_chats()

    try:
        task_id_str = str(task_id)
        if not r.sismember(chat_tasks_key, task_id_str):
            logger.warning(f"Task removal fail: Task {task_id_str} not found in set for chat {chat_id}.")
            return False

        pipe = r.pipeline()
        pipe.delete(task_key)
        pipe.delete(notified_key)
        pipe.srem(chat_tasks_key, task_id_str)
        pipe.srem(all_tasks_key, task_id_str)
        pre_results = pipe.execute()

        if r.scard(key_chat_tasks(chat_id)) == 0:
             logger.info(f"Chat {chat_id} has no tasks left. Removing from {all_chats_key}.")
             r.srem(key_all_chats(), chat_id)

        if pre_results[0] > 0 :
            logger.info(f"Successfully removed task {task_id_str} for chat {chat_id} from Redis.")
            return True
        else:
             logger.warning(f"Task removal: Task key {task_key} was already gone after sismember check for task {task_id_str}, chat {chat_id}. SREM results: {pre_results[2:]}")
             return True

    except redis.RedisError as e:
        logger.error(f"Redis error removing task {task_id} chat {chat_id}: {e}", exc_info=True)
        return False
    except Exception as e:
        logger.error(f"Unexpected error removing task {task_id} chat {chat_id}: {e}", exc_info=True)
        return False

def get_tasks_for_chat(chat_id):
    """Retrieves all tasks (details only) for a specific chat ID from Redis"""
    tasks = []
    chat_tasks_key_val = key_chat_tasks(chat_id)
    try:

        task_ids = r.smembers(chat_tasks_key_val)
        if not task_ids:
            return []

        pipe = r.pipeline()
        for task_id_str in task_ids:
            pipe.hgetall(key_task(task_id_str))
        task_hashes = pipe.execute()

        for task_hash in task_hashes:
            parsed_task = _parse_task_hash(task_hash)
            if parsed_task:
                tasks.append(parsed_task)
            else:

                logger.warning(f"Failed to parse task hash during get_tasks_for_chat {chat_id}. Data: {task_hash}")

    except redis.RedisError as e:
        logger.error(f"Redis error fetching tasks chat {chat_id}: {e}", exc_info=True)
        return []
    except Exception as e:
        logger.error(f"Unexpected error fetching tasks chat {chat_id}: {e}", exc_info=True)
        return []
    return tasks

def get_all_task_ids():
    """Retrieves a set of all active task IDs (as strings)"""
    try:
        return r.smembers(key_all_tasks())
    except redis.RedisError as e:
        logger.error(f"Redis error fetching all task IDs: {e}", exc_info=True)
        return set()
    except Exception as e:
        logger.error(f"Unexpected error fetching all task IDs: {e}", exc_info=True)
        return set()

def get_task_details(task_id):
    """Retrieves the details hash for a single task (pass ID as string or int)"""
    try:
        task_hash = r.hgetall(key_task(task_id))
        return _parse_task_hash(task_hash)
    except redis.RedisError as e:
        logger.error(f"Redis error fetching details for task {task_id}: {e}", exc_info=True)
        return None
    except Exception as e:
        logger.error(f"Unexpected error fetching details for task {task_id}: {e}", exc_info=True)
        return None

def get_notified_items(task_id):
    """Retrieves the set of notified item URLs for a task (pass ID as string or int)"""
    try:
        return r.smembers(key_notified(task_id))
    except redis.RedisError as e:
        logger.error(f"Redis error fetching notified items for task {task_id}: {e}", exc_info=True)
        return set()
    except Exception as e:
        logger.error(f"Unexpected error fetching notified items for task {task_id}: {e}", exc_info=True)
        return set()

def add_notified_items(task_id, notified_items_list):
    """
    Adds item URLs to the notified set for a task. Handles history limit
    Returns the number of items successfully added (can be 0 if all were duplicates)
    Task ID can be string or int
    """
    if not notified_items_list:
        return 0
    notified_key_val = key_notified(task_id)
    added_count = 0
    try:
        string_items = [str(item) for item in notified_items_list]
        added_count = r.sadd(notified_key_val, *string_items)
        if MAX_NOTIFIED_HISTORY_PER_TASK and MAX_NOTIFIED_HISTORY_PER_TASK > 0:
            current_size = r.scard(notified_key_val)
            if current_size > MAX_NOTIFIED_HISTORY_PER_TASK:
                all_items = list(r.smembers(notified_key_val))
                items_to_keep = all_items[-MAX_NOTIFIED_HISTORY_PER_TASK:]
                logger.info(f"Task {task_id}: Notified history limit ({MAX_NOTIFIED_HISTORY_PER_TASK}) exceeded ({current_size}). Trimming...")
                pipe = r.pipeline()
                pipe.delete(notified_key_val)
                if items_to_keep:
                    pipe.sadd(notified_key_val, *items_to_keep)
                pipe.execute()
                logger.info(f"Task {task_id}: Notified history trimmed to {len(items_to_keep)} items.")
        return added_count
    except redis.RedisError as e:
        logger.error(f"Redis error adding notified items for task {task_id}: {e}", exc_info=True)
        return 0
    except Exception as e:
        logger.error(f"Unexpected error adding notified items for task {task_id}: {e}", exc_info=True)
        return 0

def get_distinct_chat_ids():
    """Retrieves a list of unique chat IDs (as integers) that have tasks"""
    try:
        chat_ids_str = r.smembers(key_all_chats())
        return [int(cid) for cid in chat_ids_str if cid.isdigit()]
    except redis.RedisError as e:
        logger.error(f"Redis error fetching distinct chat IDs from {key_all_chats()}: {e}", exc_info=True)
        return []
    except Exception as e:
        logger.error(f"Unexpected error fetching distinct chat IDs: {e}", exc_info=True)
        return []
