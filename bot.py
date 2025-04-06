import asyncio
import logging
import re
import json
from telegram import Update, BotCommand, InlineKeyboardButton, InlineKeyboardMarkup, Bot, User
from telegram.ext import (
    Application, ApplicationBuilder, CommandHandler, ContextTypes,
    MessageHandler, filters, CallbackQueryHandler
)
from telegram.constants import ParseMode
from telegram.error import BadRequest, Forbidden, TimedOut
from telegram.helpers import escape_markdown
import config
import redis_db as database
import scraper


logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram.ext").setLevel(logging.INFO)
logging.getLogger("apscheduler").setLevel(logging.WARNING)

logging.getLogger("redis").setLevel(logging.INFO)
logger = logging.getLogger(__name__)

def is_admin(update: Update) -> bool:
    """Checks if the user initiating the update is an admin"""
    return update.effective_user and update.effective_user.id in config.ADMIN_CHAT_IDS


async def set_bot_commands(application: Application):
    """Sets the bot commands visible in Telegram"""
    commands = [
        BotCommand("start", "Start interacting with the bot"),
        BotCommand("help", "Show help message"),
        BotCommand("monitor", "Monitor price below X (e.g., /monitor mercari 'holo plush' 5000)"),
        BotCommand("monitor_ending", "Monitor Yahoo Auctions ending soon (e.g., /monitor_ending 'holo figure' 5000 20)"),
        BotCommand("list", "List your active monitoring tasks"),
        BotCommand("stop", "Stop a specific monitoring task (use ID from /list)"),
        BotCommand("support", "Send a message to the bot admin (e.g., /support My issue is...)"),
        BotCommand("list_all", "[Admin] List all tasks from all users."),
        BotCommand("announce_wipe", "[Admin] Announce DB wipe to users."),
    ]
    try:
        await application.bot.set_my_commands(commands)
        logger.info("Bot commands set successfully.")
    except TimedOut:
        logger.error("Timed out while setting bot commands. Check network connectivity.")
    except Exception as e:
        logger.error(f"Failed to set bot commands: {e}", exc_info=True)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Sends a welcome message"""
    user = update.effective_user
    await update.message.reply_html(
        f"Hi {user.mention_html()}! I can monitor ZenMarket for you.\n"
        "Use /help to see available commands."
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Displays help information using MarkdownV2"""

    help_text = f"""
*ZenMarket Monitoring Bot*

Use these commands:

• `/monitor <platform> '<query>' <max_price>`
  Monitor items on Mercari, Rakuten, or Yahoo below a specific price\.
  `<platform>`: `mercari`, `rakuten`, or `yahoo`\.
  `<query>`: Search term\(s\)\. Use single quotes `' '` for multiple words\.
  `<max_price>`: Maximum price in JPY\.
  *Example:* `/monitor mercari 'hololive aqua' 6000`

• `/monitor_ending '<query>' <max_price> <max_minutes>`
  Monitor Yahoo Auctions ending within a time limit and below a price\.
  `<query>`: Search term\(s\)\. Use single quotes `' '` for multiple words\.
  `<max_price>`: Maximum price \(current bid/BIN\) in JPY\.
  `<max_minutes>`: Notify if auction ends within this many minutes or less\.
  *Example:* `/monitor_ending 'korone figure' 10000 30` \(Notify if ≤ ¥10,000 and ending in ≤ 30 mins\)

• `/list`
  Show all your currently active monitoring tasks with their IDs\.

• `/stop <task_id>`
  Stop monitoring the task with the specified ID\.
  *Example:* `/stop 12`

• `/support <your message>`
  Send a message directly to the bot administrator for help or feedback\.
  *Example:* `/support The scraper for Mercari seems broken\.`

• `/help`
  Show this help message\.

*Note:* The bot checks periodically \(approx\. every {int(config.DEFAULT_CHECK_INTERVAL_SECONDS / 60)} minutes\)\. Web scraping depends on ZenMarket's website structure\. Prices are checked in JPY\. Time remaining is approximate\.
"""

    if is_admin(update):
        admin_help = """

*Admin Commands:*
• `/list_all`
  Show all active tasks for *all* users\.
• `/announce_wipe`
  Notify all users about an upcoming database wipe \(Redis flush\)\.
"""
        help_text += admin_help

    try:
        await update.message.reply_text(help_text, parse_mode=ParseMode.MARKDOWN_V2)
    except BadRequest as e:

        logger.error(f"Failed to send help message with MarkdownV2 despite escaping: {e}. Sending plain text.")
        plain_text = re.sub(r'[*`\\_\[\]\(\)~>
        await update.message.reply_text(plain_text)
    except Exception as e:
        logger.error(f"Unexpected error sending help message: {e}", exc_info=True)
        await update.message.reply_text("Sorry, I couldn't display the help message right now.")


async def monitor(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Adds a new standard monitoring task"""
    chat_id = update.effective_chat.id
    args = context.args
    usage = "Usage: /monitor <platform> '<query>' <max_price>\nExample: /monitor mercari 'hololive plush' 5000"
    platform = ""
    query = ""
    max_price_str = ""
    try:
        if len(args) < 3: raise ValueError("Incorrect number of arguments.")
        platform = args[0].lower()
        if platform not in ['mercari', 'rakuten', 'yahoo']: raise ValueError("Invalid platform.")
        max_price_str = args[-1]
        if not max_price_str.strip().replace('.', '', 1).isdigit(): raise ValueError("Max price must be a number.")
        raw_query_parts = args[1:-1]
        if len(raw_query_parts) == 1 and raw_query_parts[0].startswith("'") and raw_query_parts[0].endswith("'"):
            query = raw_query_parts[0][1:-1].strip()
        elif raw_query_parts and raw_query_parts[0].startswith("'") and args[-2].endswith("'"):
            query = " ".join(raw_query_parts)[1:-1].strip()
        else:
             query = " ".join(raw_query_parts).strip()
             if query and ' ' in query and not (query.startswith("'") and query.endswith("'")):
                 logger.warning(f"Parsing multi-word query '{query}' without quotes for /monitor.")
        if not query: raise ValueError("Query cannot be empty.")
        max_price = float(max_price_str.strip())
        if max_price <= 0: raise ValueError("Max price must be positive.")
    except (IndexError, ValueError) as e:
        await update.message.reply_text(f"Error: {e}\n{usage}")
        return
    except Exception as e:
        logger.error(f"Error parsing /monitor: {e}", exc_info=True)
        await update.message.reply_text(f"An unexpected error occurred.\n{usage}")
        return

    sort_options = None
    if platform == 'mercari': sort_options = 'LaunchDate'
    elif platform == 'yahoo': sort_options = 'new&order=desc'

    task_id = database.add_task(chat_id, platform, query, max_price, sort_options, max_minutes_left=None)

    if task_id:
        await update.message.reply_text(f"✅ Standard monitoring started for '{escape_markdown(query, version=2)}' on {platform.capitalize()} \(Max Price: ¥{max_price:,.0f}\)\. Task ID: `{task_id}`\nChecking every {int(config.DEFAULT_CHECK_INTERVAL_SECONDS / 60)} mins\.", parse_mode=ParseMode.MARKDOWN_V2)
    else:
        await update.message.reply_text(f"⚠️ Could not add task due to a database error\. Please try again later or contact support if the issue persists\.", parse_mode=ParseMode.MARKDOWN_V2)

async def monitor_ending(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Adds a task to monitor Yahoo auctions ending soon"""
    chat_id = update.effective_chat.id
    args = context.args
    usage = "Usage: /monitor_ending '<query>' <max_price> <max_minutes>\nExample: /monitor_ending 'holo figure' 5000 20"
    platform = 'yahoo'
    query = ""
    max_price_str = ""
    max_minutes_str = ""
    try:
        if len(args) < 3: raise ValueError("Incorrect number of arguments.")
        max_price_str = args[-2]
        max_minutes_str = args[-1]
        if not max_price_str.strip().replace('.', '', 1).isdigit(): raise ValueError("Max price must be a number.")
        if not max_minutes_str.strip().isdigit(): raise ValueError("Max minutes must be an integer.")
        raw_query_parts = args[0:-2]
        if len(raw_query_parts) == 1 and raw_query_parts[0].startswith("'") and raw_query_parts[0].endswith("'"):
             query = raw_query_parts[0][1:-1].strip()
        elif raw_query_parts and raw_query_parts[0].startswith("'") and args[-3].endswith("'"):
             query = " ".join(raw_query_parts)[1:-1].strip()
        else:
            query = " ".join(raw_query_parts).strip()
            if query and ' ' in query and not (query.startswith("'") and query.endswith("'")):
                 logger.warning(f"Parsing multi-word query '{query}' without quotes for /monitor_ending.")
        if not query: raise ValueError("Query cannot be empty.")
        max_price = float(max_price_str.strip())
        max_minutes = int(max_minutes_str.strip())
        if max_price <= 0: raise ValueError("Max price must be positive.")
        if max_minutes <= 0: raise ValueError("Max minutes must be positive.")
    except (IndexError, ValueError) as e:
        await update.message.reply_text(f"Error parsing command: {e}\n{usage}")
        return
    except Exception as e:
        logger.error(f"Unexpected error parsing /monitor_ending command: {e}", exc_info=True)
        await update.message.reply_text(f"An unexpected error occurred.\n{usage}")
        return

    sort_options = 'sort=endtime&order=asc'
    task_id = database.add_task(chat_id, platform, query, max_price, sort_options, max_minutes_left=max_minutes)

    if task_id:
        await update.message.reply_text(f"✅ Yahoo Auction monitoring started for '{escape_markdown(query, version=2)}' \(Max Price: ¥{max_price:,.0f}, Ending within: {max_minutes} min\)\. Task ID: `{task_id}`\nChecking every {int(config.DEFAULT_CHECK_INTERVAL_SECONDS / 60)} mins\.", parse_mode=ParseMode.MARKDOWN_V2)
    else:
        await update.message.reply_text(f"⚠️ Could not add task due to a database error\. Please try again later or contact support if the issue persists\.", parse_mode=ParseMode.MARKDOWN_V2)


async def list_tasks(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Lists active monitoring tasks for the chat using MarkdownV2"""
    chat_id = update.effective_chat.id
    tasks = database.get_tasks_for_chat(chat_id)
    if not tasks:
        await update.message.reply_text("You have no active monitoring tasks.")
        return

    message_parts = ["*Your active monitoring tasks:*\n"]
    for task in tasks:
        task_id = task['id']; platform = task.get('platform', 'N/A'); query = task.get('query', 'N/A')
        max_price = task.get('max_price', 0.0); sort_options = task.get('sort_options')
        max_minutes_left = task.get('max_minutes_left')
        safe_query = escape_markdown(query, version=2)
        safe_platform = escape_markdown(platform.capitalize(), version=2)
        safe_sort = escape_markdown(str(sort_options) or 'None', version=2) if sort_options else "Default"
        sort_info = f" \(Sort: `{safe_sort}`\)" if sort_options else ""
        condition = f"\n  *Condition:* Ending ≤ {max_minutes_left} min" if max_minutes_left is not None else ""
        task_str = (f"\n• *ID:* `{task_id}`\n"
                    f"  *Platform:* {safe_platform}\n"
                    f"  *Query:* `{safe_query}`\n"
                    f"  *Max Price:* `¥{max_price:,.0f}`{condition}{sort_info}")
        message_parts.append(task_str)
    message_parts.append("\n\nUse `/stop <task_id>` to remove a task\.")
    full_message = "".join(message_parts)
    max_length = 4096
    try:
        if len(full_message) > max_length:
            logger.info(f"Long /list message for chat {chat_id} ({len(full_message)} chars). Splitting...")
            parts_to_send = []
            current_part = "*Your active monitoring tasks:*\n"
            footer = "\n\nUse `/stop <task_id>` to remove a task\."
            max_part_len = max_length - len(footer)
            for task_str in message_parts[1:-1]:
                if len(current_part) + len(task_str) > (max_length if not parts_to_send else max_part_len):
                    parts_to_send.append(current_part)
                    current_part = task_str
                else:
                    current_part += task_str
            parts_to_send.append(current_part)
            if parts_to_send: parts_to_send[-1] += footer
            for i, part_message in enumerate(parts_to_send):
                 if part_message.strip():
                    await update.message.reply_text(part_message, parse_mode=ParseMode.MARKDOWN_V2)
                    if i < len(parts_to_send) - 1: await asyncio.sleep(0.6)
        else:
            await update.message.reply_text(full_message, parse_mode=ParseMode.MARKDOWN_V2)
    except BadRequest as e:
        logger.error(f"Error sending /list message with MarkdownV2: {e}. Length: {len(full_message)}. Sending plain.")
        plain_text_message = re.sub(r'[*`\\_\[\]\(\)~>
        await update.message.reply_text(plain_text_message)
    except Exception as e:
        logger.error(f"Unexpected error sending /list message: {e}", exc_info=True)
        await update.message.reply_text("An error occurred while trying to list your tasks.")

async def stop_task(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Stops a specific monitoring task."""
    chat_id = update.effective_chat.id
    args = context.args
    if len(args) != 1:
        await update.message.reply_text("Usage: /stop <task_id>\nGet the Task ID from /list.")
        return
    try:
        task_id_to_stop = int(args[0])
    except ValueError:
        await update.message.reply_text("Invalid Task ID. It should be a number. Get the ID from /list.")
        return

    if database.remove_task(task_id_to_stop, chat_id):
        await update.message.reply_text(f"✅ Stopped monitoring task with ID: `{task_id_to_stop}`", parse_mode=ParseMode.MARKDOWN_V2)
    else:
        await update.message.reply_text(f"⚠️ Could not stop task with ID: `{task_id_to_stop}`\. It might not exist, not belong to this chat, or a database error occurred\. Use `/list` to check\.", parse_mode=ParseMode.MARKDOWN_V2)


async def list_all_tasks(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """[Admin] Lists all active tasks from all users using MarkdownV2."""
    if not is_admin(update):
        await update.message.reply_text("⛔ This command is restricted to bot administrators.")
        logger.warning(f"Unauthorized attempt to use /list_all by user {update.effective_user.id}")
        return

    logger.info(f"Admin {update.effective_user.id} requested /list_all")
    all_task_ids = database.get_all_task_ids()

    if not all_task_ids:
        await update.message.reply_text("No active monitoring tasks found in the database for any user.")
        return

    all_tasks_details = []
    logger.info(f"Fetching details for {len(all_task_ids)} tasks...")
    fetch_errors = 0
    pipe = database.r.pipeline()
    for task_id in all_task_ids:
        pipe.hgetall(database.key_task(task_id))
    try:
        task_hashes = pipe.execute()
    except Exception as pipe_err:
         logger.error(f"Redis pipeline error fetching all task details: {pipe_err}", exc_info=True)
         await update.message.reply_text("Error fetching task details from database.")
         return

    task_id_list = list(all_task_ids)
    for i, task_hash in enumerate(task_hashes):
        task_id = task_id_list[i]
        parsed_task = database._parse_task_hash(task_hash)
        if parsed_task:
            all_tasks_details.append(parsed_task)
        else:
            logger.warning(f"Failed to parse task hash for task ID {task_id} during /list_all")
            fetch_errors += 1

    if not all_tasks_details:
         await update.message.reply_text("Found task IDs but failed to fetch details.")
         return

    tasks_by_chat = {}
    for task in all_tasks_details:
        chat_id = task.get('chat_id')
        if chat_id:
            if chat_id not in tasks_by_chat: tasks_by_chat[chat_id] = []
            tasks_by_chat[chat_id].append(task)

    message_parts = ["*All Active Monitoring Tasks \(Grouped by User\):*\n"]
    total_task_count = len(all_tasks_details)
    sorted_chat_ids = sorted(tasks_by_chat.keys())

    for chat_id in sorted_chat_ids:
        tasks = tasks_by_chat[chat_id]
        user_info_parts = [f"User Chat ID: `{chat_id}` \({len(tasks)} tasks\)"]
        try:
            chat = await context.bot.get_chat(chat_id)
            if chat.username: user_info_parts.append(f"@{escape_markdown(chat.username, version=2)}")
            elif chat.full_name: user_info_parts.append(f"\({escape_markdown(chat.full_name, version=2)}\)")
        except Forbidden: user_info_parts.append("\(Info Unavailable / Blocked\)")
        except Exception as e: logger.warning(f"Could not fetch chat details for {chat_id} during /list_all: {type(e).__name__}")

        user_info = ' '.join(user_info_parts)
        message_parts.append(f"\n\n---\n*Chat:* {user_info}\n---")

        for task in tasks:
            task_id = task['id']; platform = task.get('platform', 'N/A'); query = task.get('query', 'N/A')
            max_price = task.get('max_price', 0.0); sort_options = task.get('sort_options')
            max_minutes_left = task.get('max_minutes_left')
            safe_query = escape_markdown(query, version=2)
            safe_platform = escape_markdown(platform.capitalize(), version=2)
            safe_sort = escape_markdown(str(sort_options) or 'None', version=2) if sort_options else "Default"
            sort_info = f" \(Sort: `{safe_sort}`\)" if sort_options else ""
            condition = f"\n    *Condition:* Ending ≤ {max_minutes_left} min" if max_minutes_left is not None else ""
            task_str = (f"\n  • *ID:* `{task_id}`\n"
                        f"    *Platform:* {safe_platform}\n"
                        f"    *Query:* `{safe_query}`\n"
                        f"    *Max Price:* `¥{max_price:,.0f}`{condition}{sort_info}")
            message_parts.append(task_str)

    message_parts.insert(1, f"\n*Total Tasks:* {total_task_count} (Details fetch errors: {fetch_errors})\n")
    full_message = "".join(message_parts)
    max_length = 4096
    try:
        if len(full_message) > max_length:
            logger.info(f"/list_all message is long ({len(full_message)} chars). Sending in parts.")
            parts_to_send = [full_message[i:i + max_length] for i in range(0, len(full_message), max_length)]
            for i, part_message in enumerate(parts_to_send):
                if part_message.strip():
                    await update.message.reply_text(part_message, parse_mode=ParseMode.MARKDOWN_V2)
                    if i < len(parts_to_send) - 1: await asyncio.sleep(0.8)
        else:
            await update.message.reply_text(full_message, parse_mode=ParseMode.MARKDOWN_V2)
    except BadRequest as e:
        logger.error(f"Error sending /list_all message with MarkdownV2: {e}. Length: {len(full_message)}. Sending plain.")
        plain_text_message = re.sub(r'[*`\\_\[\]\(\)~>
        await update.message.reply_text(plain_text_message)
    except Exception as e:
        logger.error(f"Unexpected error sending /list_all message: {e}", exc_info=True)
        await update.message.reply_text("An error occurred while trying to list all tasks.")



async def support_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    chat = update.effective_chat
    if not user or not chat: return
    user_message = " ".join(context.args).strip()
    if not user_message:
        await update.message.reply_text("Please include your message after the `/support` command\.\n*Example:* `/support I need help with task 123`", parse_mode=ParseMode.MARKDOWN_V2)
        return
    logger.info(f"Support request from user {user.id} ({user.username or 'no_username'}) in chat {chat.id}: {user_message[:100]}...")
    admin_message_text = (
        f"🆘 <b>Support Request</b>\n\n"
        f"<b>From User:</b> {user.mention_html()} (ID: <code>{user.id}</code>)\n"
        f"<b>User Chat ID:</b> <code>{chat.id}</code>\n\n"
        f"<b>Message:</b>\n<pre>{escape_markdown(user_message, entity_type='pre')}</pre>"
    )
    message_sent_to_admin = False; failed_admin_ids = []; sent_admin_ids = []
    for admin_id in config.ADMIN_CHAT_IDS:
        try:
            await context.bot.send_message(chat_id=admin_id, text=admin_message_text, parse_mode=ParseMode.HTML)
            message_sent_to_admin = True; sent_admin_ids.append(admin_id)
        except Forbidden: logger.warning(f"Failed to send support msg to admin {admin_id}: Blocked."); failed_admin_ids.append(admin_id)
        except Exception as e: logger.error(f"Failed to send support msg to admin {admin_id}: {e}"); failed_admin_ids.append(admin_id)
        await asyncio.sleep(0.2)
    if message_sent_to_admin:
        await update.message.reply_text("✅ Your message has been sent to the administrator\. They will reply here if needed\.", parse_mode=ParseMode.MARKDOWN_V2)
        logger.info(f"Support message from {user.id} forwarded to admins: {sent_admin_ids}")
        if failed_admin_ids: await update.message.reply_text(f"ℹ️ Note: Could not reach all administrators, but your message was sent\.", disable_notification=True, parse_mode=ParseMode.MARKDOWN_V2); logger.warning(f"Failed forward support msg from {user.id} to some admins: {failed_admin_ids}")
    else: await update.message.reply_text("❌ Sorry, I could not forward your message to the administrator at this time\.", parse_mode=ParseMode.MARKDOWN_V2)

async def handle_admin_support_reply(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    admin_user = update.effective_user
    if not (update.message.reply_to_message and admin_user and is_admin(update) and update.message.text): return
    replied_message = update.message.reply_to_message
    if not replied_message.from_user.is_bot: return
    original_message_html = replied_message.text_html
    user_chat_id_match = re.search(r'<b>User Chat ID:</b>\s*<code>\s*(\d+)\s*</code>', original_message_html, re.IGNORECASE)
    if not user_chat_id_match:
        logger.warning(f"Admin {admin_user.id} replied, but couldn't extract original user chat ID. Regex failed. HTML: '{original_message_html}'")
        await update.message.reply_text("⚠️ Couldn't identify user chat ID\. Reply not sent\.", parse_mode=ParseMode.MARKDOWN_V2); return
    try: original_user_chat_id = int(user_chat_id_match.group(1))
    except (ValueError, IndexError): logger.error(f"Failed convert user chat ID '{user_chat_id_match.group(1)}' to int."); await update.message.reply_text("⚠️ Error parsing user chat ID\. Reply not sent\.", parse_mode=ParseMode.MARKDOWN_V2); return
    admin_reply_text = update.message.text
    logger.info(f"Admin {admin_user.id} replying to user chat {original_user_chat_id}. Msg: {admin_reply_text[:100]}...")
    user_reply_text = f"✉️ *Reply from Administrator:*\n\n{escape_markdown(admin_reply_text, version=2)}"
    try:
        await context.bot.send_message(chat_id=original_user_chat_id, text=user_reply_text, parse_mode=ParseMode.MARKDOWN_V2)
        await context.bot.send_message(chat_id=update.effective_chat.id, text=f"✅ Reply sent to user chat `{original_user_chat_id}`\.", reply_to_message_id=update.message.message_id, parse_mode=ParseMode.MARKDOWN_V2)
        logger.info(f"Admin reply from {admin_user.id} sent successfully to chat {original_user_chat_id}.")
    except Forbidden: logger.warning(f"Failed send admin reply to {original_user_chat_id}: Blocked."); await context.bot.send_message(chat_id=update.effective_chat.id, text=f"❌ Failed send reply: User `{original_user_chat_id}` may have blocked bot\.", reply_to_message_id=update.message.message_id, parse_mode=ParseMode.MARKDOWN_V2)
    except BadRequest as e:
         if "can't parse entities" in str(e).lower():
             logger.error(f"Failed send admin reply to {original_user_chat_id} due to MDv2 error: {e}. Sending plain.")
             await context.bot.send_message(chat_id=update.effective_chat.id, text=f"⚠️ Your reply caused format error\. Sending plain text\.", reply_to_message_id=update.message.message_id, parse_mode=ParseMode.MARKDOWN_V2)
             try:
                 plain_reply = f"Reply from Administrator:\n\n{admin_reply_text}"
                 await context.bot.send_message(chat_id=original_user_chat_id, text=plain_reply)
                 await context.bot.send_message(chat_id=update.effective_chat.id, text=f"✅ Plain text reply sent to `{original_user_chat_id}`\.", reply_to_message_id=update.message.message_id, parse_mode=ParseMode.MARKDOWN_V2)
             except Exception as plain_e: logger.error(f"Plain text fallback also failed {original_user_chat_id}: {plain_e}"); await context.bot.send_message(chat_id=update.effective_chat.id, text=f"❌ Plain text fallback also failed for `{original_user_chat_id}`\.", reply_to_message_id=update.message.message_id, parse_mode=ParseMode.MARKDOWN_V2)
         else: logger.error(f"BadRequest admin reply {original_user_chat_id}: {e}"); await context.bot.send_message(chat_id=update.effective_chat.id, text=f"❌ Failed send reply due to Telegram error `{original_user_chat_id}`\.", reply_to_message_id=update.message.message_id, parse_mode=ParseMode.MARKDOWN_V2)
    except Exception as e: logger.error(f"Unexpected error admin reply {original_user_chat_id}: {e}"); await context.bot.send_message(chat_id=update.effective_chat.id, text=f"❌ Unexpected error sending reply `{original_user_chat_id}`\.", reply_to_message_id=update.message.message_id, parse_mode=ParseMode.MARKDOWN_V2)

async def check_monitoring_tasks(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Periodic job to check all active monitoring tasks using Redis"""
    logger.info("Running periodic check for monitoring tasks (Redis)...")
    bot: Bot = context.bot

    task_ids = database.get_all_task_ids()
    if not task_ids:
        logger.info("No active tasks found in Redis to check.")
        return

    logger.info(f"Found {len(task_ids)} active task IDs in Redis.")

    task_details_map = {}
    notified_items_map = {}
    fetch_errors = 0
    pipe = database.r.pipeline(transaction=False)
    for task_id in task_ids:
        pipe.hgetall(database.key_task(task_id))
        pipe.smembers(database.key_notified(task_id))

    try:
        results = pipe.execute()
    except redis.RedisError as e:
        logger.error(f"Redis pipeline error fetching task data: {e}", exc_info=True)
        return
    except Exception as e:
        logger.error(f"Unexpected error during Redis pipeline execution: {e}", exc_info=True)
        return

    if len(results) != len(task_ids) * 2:
        logger.error(f"CRITICAL: Mismatch in Redis pipeline results length! Expected {len(task_ids)*2}, got {len(results)}. Aborting cycle.")
        return

    task_id_list = list(task_ids)
    for i in range(len(task_id_list)):
        task_id = task_id_list[i]
        task_hash = results[i * 2]
        notified_set = results[i * 2 + 1]
        parsed_task = database._parse_task_hash(task_hash)
        if parsed_task:
            task_details_map[task_id] = parsed_task
            notified_items_map[task_id] = notified_set if notified_set else set()
        else:
            logger.warning(f"Failed to parse task hash for task {task_id} during check cycle. Skipping task.")
            fetch_errors += 1

    if not task_details_map:
        logger.info("No valid task details could be fetched. Ending check cycle.")
        return

    logger.info(f"Successfully fetched data for {len(task_details_map)} tasks ({fetch_errors} errors).")

    tasks_to_scrape = {}
    for task_id, task_data in task_details_map.items():
        normalized_sort = task_data.get('sort_options')
        scrape_key = (task_data.get('platform'), task_data.get('query'), normalized_sort)
        if not all(scrape_key[:2]):
             logger.warning(f"Skipping task {task_id} due to invalid scrape key components: {scrape_key}")
             continue
        if scrape_key not in tasks_to_scrape: tasks_to_scrape[scrape_key] = []
        tasks_to_scrape[scrape_key].append(task_id)

    if not tasks_to_scrape:
        logger.info("No valid tasks found to scrape after grouping.")
        return
    logger.info(f"Need to perform {len(tasks_to_scrape)} unique scrapes for {len(task_details_map)} tasks.")

    keys_in_order = list(tasks_to_scrape.keys())
    coroutines_to_run = []
    for scrape_key in keys_in_order:
        platform, query, sort_options = scrape_key
        coroutines_to_run.append(asyncio.to_thread(scraper.scrape_zenmarket, platform, query, sort_options))

    scrape_results_raw = []
    try:
        scrape_results_raw = await asyncio.wait_for(
             asyncio.gather(*coroutines_to_run, return_exceptions=True), timeout=240.0)
    except asyncio.TimeoutError:
         logger.error("Scraping gather operation timed out. Skipping processing this cycle.")
         return
    except Exception as gather_err:
         logger.error(f"Unexpected error during scraping gather: {gather_err}", exc_info=True)
         return

    scraped_items_map = {}
    if len(keys_in_order) != len(scrape_results_raw):
        logger.error(f"CRITICAL: Mismatch scrape keys ({len(keys_in_order)}) and results ({len(scrape_results_raw)})! Aborting.")
        return
    for i, result_or_exc in enumerate(scrape_results_raw):
        scrape_key = keys_in_order[i]
        scraped_items_map[scrape_key] = result_or_exc
        if isinstance(result_or_exc, Exception): logger.warning(f"Scrape failed for group {scrape_key}: {result_or_exc}")
        elif result_or_exc is None: logger.error(f"CRITICAL: Scraper returned None for group {scrape_key}. Treating as []."); scraped_items_map[scrape_key] = []

    active_chat_ids_notified = set()
    tasks_to_remove_later = []

    for task_id_str, task in task_details_map.items():
        chat_id = task.get('chat_id')
        task_max_price = task.get('max_price')
        task_max_minutes = task.get('max_minutes_left')
        platform = task.get('platform')
        query = task.get('query')
        sort_options = task.get('sort_options')
        notified_items_set = notified_items_map.get(task_id_str, set())

        if not all([chat_id, platform, query, task_max_price is not None]):
             logger.error(f"Skipping task {task_id_str}: missing essential data in map.")
             continue

        normalized_sort = sort_options
        scrape_key = (platform, query, normalized_sort)
        scraped_result = scraped_items_map.get(scrape_key)

        if isinstance(scraped_result, Exception) or scraped_result is None or not scraped_result:
            continue
        items_to_notify_this_task = []
        urls_found_matching_this_task = []

        for item in scraped_result:
            item_url = item.get('url'); item_price = item.get('price')
            item_minutes_left = item.get('minutes_left')
            if not item_url or item_price is None: continue

            match = False
            price_match = item_price <= task_max_price
            time_match = False
            if task_max_minutes is not None:
                if item_minutes_left is not None and item_minutes_left != -1:
                    time_match = item_minutes_left <= task_max_minutes
                match = price_match and time_match
            else:
                match = price_match

            if match:
                 urls_found_matching_this_task.append(item_url)
                 if item_url not in notified_items_set:
                     items_to_notify_this_task.append(item)

        newly_notified_urls_for_db = []
        if items_to_notify_this_task:
            logger.info(f"Found {len(items_to_notify_this_task)} NEW items matching task {task_id_str} criteria (Chat: {chat_id})")
            for item in items_to_notify_this_task:
                item_name = item.get('name', 'N/A')
                item_price_val = item.get('price')
                item_link = item.get('url')
                image_url = item.get('image_url')
                item_mins = item.get('minutes_left')

                safe_query = escape_markdown(query or 'N/A', version=2)
                safe_item_name = escape_markdown(item_name or 'N/A', version=2)

                caption_lines = [
                    f"✨ *New Item Found*\n",
                    f"*Query:* `{safe_query}` \({escape_markdown(platform.capitalize(), version=2)}\)",
                    f"*Item:* {safe_item_name}",
                    f"*Price:* `¥{item_price_val:,.0f}` \(Task Max: `¥{task_max_price:,.0f}`\)"
                ]

                if task_max_minutes is not None:
                    time_info = "*Ending In:* `?`"
                    time_req = f"\(Task Req: `≤ {task_max_minutes} min`\)"
                    if item_mins is not None and item_mins != -1:
                        time_info = f"*Ending In:* `≈ {item_mins} min`"
                    elif item_mins == -1:
                        time_info = f"*Ending In:* `Ended`"
                    caption_lines.append(f"{time_info} {time_req}")

                if item_link:
                    caption_lines.append(f"\n*Link:* [View Item]({item_link})")
                else:
                    caption_lines.append("\n*Link:* `Not available`")

                caption = "\n".join(caption_lines)
                message_sent_successfully = False
                send_photo_attempted = False
                try:

                    if platform != 'yahoo' and image_url and image_url.startswith('http'):
                        send_photo_attempted = True
                        await bot.send_photo(
                            chat_id=chat_id,
                            photo=image_url,
                            caption=caption,
                            parse_mode=ParseMode.MARKDOWN_V2
                        )
                        message_sent_successfully = True
                    else:

                        await bot.send_message(
                            chat_id=chat_id,
                            text=caption,
                            parse_mode=ParseMode.MARKDOWN_V2,
                            disable_web_page_preview=False
                        )
                        message_sent_successfully = True
                except BadRequest as e:
                    error_str = str(e).lower()
                    photo_error_indicators = [
                        "failed to get http url content",
                        "wrong file identifier",
                        "photo_invalid",
                        "wrong type of the web page content"
                    ]
                    is_common_photo_error = any(indicator in error_str for indicator in photo_error_indicators)

                    if send_photo_attempted and is_common_photo_error:
                        logger.warning(f"Failed PHOTO (Task {task_id_str}, Item: {item_link}, Img: {image_url}), attempting TEXT fallback: {e}")
                        try:
                            await bot.send_message(
                                chat_id=chat_id,
                                text=caption,
                                parse_mode=ParseMode.MARKDOWN_V2,
                                disable_web_page_preview=False
                            )
                            message_sent_successfully = True
                        except Exception as fallback_text_err:
                            logger.error(f"Fallback TEXT failed after photo error (Task {task_id_str}, Item: {item_link}): {fallback_text_err}")
                            message_sent_successfully = False

                    elif "entity" in error_str or "can't parse entities" in error_str:
                        logger.error(f"MarkdownV2 BadRequest sending notification (Task {task_id_str}, Item: {item_link}): {e}. Caption: '{caption[:100]}...' Trying plain text fallback.")

                        plain_caption_lines = [
                            f"✨ New Item Found!",
                            f"Query: {query} ({platform.capitalize()})",
                            f"Item: {item_name}",
                            f"Price: ¥{item_price_val:,.0f} (Task Max: ¥{task_max_price:,.0f})"
                        ]
                        if task_max_minutes is not None:
                            time_info_plain = "Ending In: ?"
                            if item_mins is not None and item_mins != -1:
                                time_info_plain = f"Ending In: ~{item_mins} min"
                            elif item_mins == -1:
                                time_info_plain = f"Ending In: Ended"
                            plain_caption_lines.append(f"{time_info_plain} (Task Req: <= {task_max_minutes} min)")
                        if item_link:
                             plain_caption_lines.append(f"\nLink: {item_link}")

                        plain_caption = "\n".join(plain_caption_lines)
                        try:
                            await bot.send_message(
                                chat_id=chat_id,
                                text=plain_caption,
                                disable_web_page_preview=False
                            )
                            message_sent_successfully = True
                        except Exception as plain_fallback_e:
                            logger.error(f"Plain text fallback failed (Task {task_id_str}, Item: {item_link}): {plain_fallback_e}")
                            message_sent_successfully = False
                    else:
                        error_context = "photo" if send_photo_attempted else "text"
                        logger.error(f"Unhandled BadRequest sending ({error_context}) (Task {task_id_str}, Item: {item_link}): {e}", exc_info=True)
                        message_sent_successfully = False

                except Forbidden as e:
                    logger.error(f"Forbidden error sending to {chat_id} (Task {task_id_str}): {e}. Schedule removal.")
                    if (task_id_str, chat_id) not in tasks_to_remove_later:
                        tasks_to_remove_later.append((task_id_str, chat_id))
                    active_chat_ids_notified.discard(chat_id)
                    message_sent_successfully = False
                    break

                except TimedOut as e:
                    logger.error(f"Timeout error sending notification (Task {task_id_str}, Item: {item_link}): {e}")
                    message_sent_successfully = False

                except Exception as e:
                    logger.error(f"Unexpected error sending notification (Task {task_id_str}, Item: {item_link}): {e}", exc_info=True)
                    message_sent_successfully = False
                if message_sent_successfully:
                    newly_notified_urls_for_db.append(item_link)
                    active_chat_ids_notified.add(chat_id)
                    await asyncio.sleep(1.2)
                else:
                     logger.warning(f"Notify FAILED/SKIPPED Task {task_id_str}, Item: {item_link}. Not adding to notified set.")

        if (task_id_str, chat_id) not in tasks_to_remove_later and newly_notified_urls_for_db:
            logger.info(f"Task {task_id_str}: Adding {len(newly_notified_urls_for_db)} newly notified URLs to Redis set.")
            try:
                added_count = database.add_notified_items(task_id_str, newly_notified_urls_for_db)
                if added_count > 0:
                    logger.info(f"Task {task_id_str}: Successfully added {added_count} new URLs to Redis notified set.")
                elif added_count == 0 and newly_notified_urls_for_db:
                    logger.warning(f"Task {task_id_str}: add_notified_items reported 0 added, but list was not empty. Maybe Redis error or all duplicates?")
            except Exception as e:
                 logger.error(f"Error updating notified items in Redis for task {task_id_str}: {e}", exc_info=True)

    if tasks_to_remove_later:
        logger.info(f"Removing {len(tasks_to_remove_later)} tasks due to Forbidden errors.")
        removed_count = 0
        for task_id_str, chat_id_int in tasks_to_remove_later:
            if database.remove_task(task_id_str, chat_id_int):
                removed_count += 1
            await asyncio.sleep(0.1)
        logger.info(f"Successfully removed {removed_count} tasks via Redis due to Forbidden errors.")

    logger.info(f"Periodic check finished. Successfully notified {len(active_chat_ids_notified)} unique chats this cycle.")

async def announce_wipe(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Sends a DB wipe announcement and task list to all users with tasks using MarkdownV2"""
    if not is_admin(update):
        await update.message.reply_text("⛔ This command is restricted to bot administrators.")
        logger.warning(f"Unauthorized /announce_wipe by chat ID: {update.effective_chat.id}")
        return

    admin_id = update.effective_chat.id
    logger.info(f"Admin {admin_id} initiated DB wipe announcement.")
    await update.message.reply_text("Fetching users with tasks from Redis...")

    user_chat_ids = database.get_distinct_chat_ids()
    if not user_chat_ids:
        await update.message.reply_text("No users with active tasks found in Redis.")
        return

    total_users = len(user_chat_ids)
    success_count, fail_count, blocked_count = 0, 0, 0
    logger.info(f"Found {total_users} unique users with tasks in Redis. Starting announcement...")

    announcement_prefix = ("🚨 *Bot Maintenance Announcement* 🚨\n\n"
                           "Hello\\! The monitoring bot database needs to be reset for maintenance/updates\\.\n"
                           "*All your current monitoring tasks will be removed shortly\\.*\n\n"
                           "Please save the details below if you wish to re\\-add them after the maintenance\\.\n"
                           "We apologize for the inconvenience\\!\n\n"
                           "*Your current tasks:*\n")

    bot = context.bot
    for i, chat_id in enumerate(user_chat_ids):
        tasks = database.get_tasks_for_chat(chat_id)
        if not tasks:
             logger.debug(f"Skipping announce for chat {chat_id}: No tasks found in Redis.")
             continue

        message_parts = [announcement_prefix]
        for task in tasks:
            task_id = task['id']; platform = task.get('platform', 'N/A'); query = task.get('query', 'N/A')
            max_price = task.get('max_price', 0.0); sort_options = task.get('sort_options')
            max_minutes_left = task.get('max_minutes_left')
            safe_query = escape_markdown(query, version=2); safe_platform = escape_markdown(platform.capitalize(), version=2)
            safe_sort = escape_markdown(str(sort_options) or 'None', version=2) if sort_options else "Default"
            sort_info = f" \(Sort: `{safe_sort}`\)" if sort_options else ""
            condition = f"\n   *Condition:* Ending ≤ {max_minutes_left} min" if max_minutes_left is not None else ""
            task_str = (f"\n• *ID:* `{task_id}`\n"
                        f"   *Platform:* {safe_platform}\n"
                        f"   *Query:* `{safe_query}`\n"
                        f"   *Max Price:* `¥{max_price:,.0f}`{condition}{sort_info}")
            message_parts.append(task_str)
        full_message = "".join(message_parts)
        try:
            max_length=4096
            if len(full_message) > max_length:
                logger.warning(f"Announce for {chat_id} exceeds length ({len(full_message)}). Splitting.")
                parts = [full_message[k:k+max_length] for k in range(0, len(full_message), max_length)]
                for pi, part in enumerate(parts):
                    if part.strip(): await bot.send_message(chat_id=chat_id, text=part, parse_mode=ParseMode.MARKDOWN_V2); await asyncio.sleep(1.0)
            else: await bot.send_message(chat_id=chat_id, text=full_message, parse_mode=ParseMode.MARKDOWN_V2)
            success_count += 1
        except Forbidden: logger.warning(f"Failed announce {chat_id}: Blocked ({i+1}/{total_users})."); blocked_count += 1
        except BadRequest as e:
            error_str = str(e).lower()
            if "can't parse entities" in error_str:
                 logger.error(f"Failed announce {chat_id}: MDv2 parse error - {e}. Len: {len(full_message)}. ({i+1}/{total_users}). Sending plain.")
                 try:
                     plain_msg = re.sub(r'[*`\\_\[\]\(\)~>
                     await bot.send_message(chat_id=chat_id, text=plain_msg)
                     success_count += 1; logger.info(f"Sent plain fallback {chat_id}")
                 except Exception as pe:
                     logger.error(f"Plain fallback failed {chat_id}: {pe}")
                     fail_count += 1
            else: logger.error(f"Failed announce {chat_id}: Unhandled BadRequest - {e}. Len: {len(full_message)} ({i+1}/{total_users})."); fail_count += 1
        except TimedOut: logger.error(f"Failed announce {chat_id}: Timed out ({i+1}/{total_users})."); fail_count += 1
        except Exception as e: logger.error(f"Failed announce {chat_id}: {type(e).__name__} - {e} ({i+1}/{total_users})", exc_info=False); fail_count += 1
        await asyncio.sleep(1.5)

    final_msg = f"Redis wipe announcement finished for {total_users} users.\nSent successfully: {success_count}\nBlocked: {blocked_count}\nOther failures: {fail_count}"
    logger.info(final_msg)
    await update.message.reply_text(final_msg)
    if total_users > 0 and success_count > 0:
         await update.message.reply_text("You can now proceed with wiping the Redis database if needed (e.g., `FLUSHDB` or `FLUSHALL` in redis-cli). Be careful!")
    elif total_users == 0: await update.message.reply_text("No users required notification.")
    else: await update.message.reply_text("Please review logs. Wiping Redis might not be advisable.")



async def post_init(application: Application):
    """Actions to run after the bot has started and initialized"""
    await set_bot_commands(application)
    job_queue = application.job_queue
    if job_queue:
        existing_jobs = job_queue.get_jobs_by_name("periodic_check")
        if existing_jobs: logger.info(f"Removing {len(existing_jobs)} existing job(s) 'periodic_check'..."); [job.schedule_removal() for job in existing_jobs]
        job_queue.run_repeating(check_monitoring_tasks, interval=config.DEFAULT_CHECK_INTERVAL_SECONDS, first=15, name="periodic_check")
        logger.info(f"Scheduled monitoring job 'periodic_check' (Redis) every {config.DEFAULT_CHECK_INTERVAL_SECONDS}s.")
    else: logger.error("JobQueue not available. Periodic checks won't run.")


def main() -> None:
    """Start the bot"""
    logger.info("Starting bot with Redis backend...")
    application = (
        ApplicationBuilder()
        .token(config.TELEGRAM_BOT_TOKEN)
        .connect_timeout(30.0).read_timeout(40.0).pool_timeout(30.0)
        .post_init(post_init)
        .build()
    )

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("monitor", monitor))
    application.add_handler(CommandHandler("monitor_ending", monitor_ending))
    application.add_handler(CommandHandler("list", list_tasks))
    application.add_handler(CommandHandler("stop", stop_task))
    application.add_handler(CommandHandler("support", support_command))

    application.add_handler(CommandHandler("list_all", list_all_tasks))
    application.add_handler(CommandHandler("announce_wipe", announce_wipe))

    admin_reply_filter = filters.Chat(chat_id=config.ADMIN_CHAT_IDS) & filters.REPLY & filters.TEXT & ~filters.COMMAND
    application.add_handler(MessageHandler(admin_reply_filter, handle_admin_support_reply))

    logger.info("Bot handlers registered. Starting polling...")
    try:
        application.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)
    except SystemExit as e:
         logger.fatal(f"SystemExit caught: {e}. Check Redis connection.")
    except TimedOut: logger.fatal("Connection timed out repeatedly. Exiting.")
    except KeyboardInterrupt: logger.info("Bot stopped manually.")
    except Exception as e: logger.fatal(f"Critical error in main poll loop: {e}", exc_info=True)
    finally: logger.info("Bot polling stopped.")

if __name__ == "__main__":
    main()
