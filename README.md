# ZenMonitor Bot

A Telegram bot for monitoring Japanese marketplace platforms through ZenMarket.

## About

ZenMonitor helps you hunt for items on Japanese marketplaces without constantly refreshing pages. It monitors Mercari, Rakuten, and Yahoo Auctions through ZenMarket and sends you instant notifications when items matching your criteria appear.

## Features

- Search across Mercari, Rakuten, and Yahoo Auctions
- Filter by price to stay within budget
- Track auctions ending soon
- Receive item images in notifications
- Customize sorting options
- Manage all your monitoring tasks through simple commands
- Get support from administrators when needed

## Requirements

- Python 3.7+
- Redis server
- Telegram bot token from BotFather
- Internet connection

## Installation

```
# Clone the repository
git clone https://github.com/k0rnd23/zenmonitor-bot.git
cd zenmonitor-bot

# Create a virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Configure settings
cp config.example.py config.py
# Edit config.py with your TELEGRAM_BOT_TOKEN and other settings
```

## Configuration

Edit the `config.py` file:

```python
# Insert your token from BotFather
TELEGRAM_BOT_TOKEN = "YOUR_TOKEN"

# Admin IDs (can be obtained through @userinfobot)
ADMIN_CHAT_IDS =[123456789]

# Redis settings (defaults for local server)
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
```

## Running the Bot

```
# First start Redis server
redis-server

# In a separate terminal, start the bot
python bot.py
```

## Bot Commands

- `/start` - Begin using the bot
- `/help` - List all available commands
- `/monitor [platform] [query] [max_price]` - Create a monitoring task
- `/monitor_ending [platform] [query] [max_price] [max_minutes]` - Monitor items ending soon
- `/list` - List your active tasks
- `/stop [task_number]` - Stop a monitoring task
- `/support [message]` - Send a message to the administrator

## Usage Examples

```
/monitor yahoo "figma hatsune miku" 10000
```
This will search for Hatsune Miku figures on Yahoo Auctions under 10000 yen

```
/monitor_ending mercari "pokemon card charizard" 5000 60
```
This will find Charizard Pokemon cards on Mercari under 5000 yen ending within 60 minutes

## Technologies

- Python - Core programming language
- python-telegram-bot - Library for Telegram API
- Redis - For task and state storage
- Beautiful Soup - For web page parsing
- Requests - For HTTP requests

## Contributing

Found a bug or want to add a feature? Pull requests are welcome:

1. Fork the repository
2. Create a branch for your feature (`git checkout -b feature-name`)
3. Commit your changes (`git commit -m 'Add new feature'`)
4. Push to the branch (`git push origin feature-name`)
5. Open a Pull Request

## License

Distributed under the MIT License. See LICENSE file for more information.

## Support

Having trouble with the bot? Use the `/support` command directly in the bot to contact administrators.
