import sqlite3
import hashlib
import secrets
import random
import logging
from telethon import TelegramClient, events, Button
from telethon.tl.functions.channels import GetParticipantRequest
from telethon.errors import UserNotParticipantError
from datetime import datetime, timedelta
import asyncio
import json
import os

# Enable logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------- CONFIG ----------
API_ID = 34429773
API_HASH = 'beb9b44b772aade26a1798c7efe9e04e'
BOT_TOKEN = '8610275312:AAFPh56syjUkzDE_XcNY2YqsPA1UXpx5hTo'  # CHANGE THIS
MASTER_IDS = [8512461438]

# Master UPI
MASTER_UPI = "master@okhdfcbank"
MASTER_QR_URL = "https://your-qr-code-url.com/qr.jpg"

# Withdrawal Rules
WITHDRAWAL_REQUEST_DAY = 1
PAYMENT_START_DAY = 7
PAYMENT_END_DAY = 14

# Tax Settings
GST_PERCENT = 18
TDS_PERCENT = 2
NO_TAX_LIMIT = 100

# Security Settings
MAX_REFERRALS_PER_DAY = 5
SAME_IP_LIMIT = 3
REQUIRED_TASKS_BEFORE_WITHDRAW = 5
REQUIRED_REFERRALS_BEFORE_WITHDRAW = 10

# Database setup
conn = sqlite3.connect('secure_refer_bot.db', check_same_thread=False)
c = conn.cursor()

# Settings table
c.execute('''
    CREATE TABLE IF NOT EXISTS bot_settings (
        key TEXT PRIMARY KEY,
        value TEXT,
        updated_by INTEGER,
        updated_date TEXT
    )
''')

# Insert default settings
default_settings = [
    ('referral_reward', '10'),
    ('referred_bonus', '5'),
    ('min_referrals', str(REQUIRED_REFERRALS_BEFORE_WITHDRAW)),
    ('withdrawal_min', '50'),
    ('gst_percent', str(GST_PERCENT)),
    ('tds_percent', str(TDS_PERCENT)),
    ('no_tax_limit', str(NO_TAX_LIMIT)),
    ('master_upi', MASTER_UPI),
    ('master_qr', MASTER_QR_URL),
    ('required_tasks', str(REQUIRED_TASKS_BEFORE_WITHDRAW)),
    ('max_ref_per_day', str(MAX_REFERRALS_PER_DAY)),
]
for key, value in default_settings:
    c.execute('INSERT OR IGNORE INTO bot_settings (key, value) VALUES (?, ?)', (key, value))

# Users table
c.execute('''
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        username TEXT,
        balance INTEGER DEFAULT 0,
        referred_by INTEGER DEFAULT NULL,
        tasks_completed TEXT DEFAULT '',
        total_earned INTEGER DEFAULT 0,
        total_referrals INTEGER DEFAULT 0,
        join_date TEXT,
        referral_code TEXT UNIQUE,
        upi_id TEXT,
        phone TEXT,
        is_verified INTEGER DEFAULT 0,
        is_banned INTEGER DEFAULT 0,
        last_withdrawal_month TEXT,
        total_withdrawn INTEGER DEFAULT 0,
        total_tasks_completed INTEGER DEFAULT 0,
        ip_address TEXT,
        daily_referrals INTEGER DEFAULT 0,
        last_reset_date TEXT,
        captcha_code TEXT,
        captcha_expiry TEXT
    )
''')

# Add missing columns
for col in ['ip_address', 'daily_referrals', 'last_reset_date', 'captcha_code', 'captcha_expiry']:
    try:
        c.execute(f'ALTER TABLE users ADD COLUMN {col} TEXT')
    except:
        pass

# Tasks table
c.execute('''
    CREATE TABLE IF NOT EXISTS tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        task_name TEXT,
        task_type TEXT DEFAULT 'join',
        reward INTEGER,
        task_data TEXT DEFAULT '',
        is_active INTEGER DEFAULT 1,
        created_by INTEGER,
        created_date TEXT
    )
''')

# Withdrawals table
c.execute('''
    CREATE TABLE IF NOT EXISTS withdrawals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        amount INTEGER,
        tax_amount INTEGER DEFAULT 0,
        net_amount INTEGER DEFAULT 0,
        upi_id TEXT,
        status TEXT DEFAULT 'pending',
        tax_paid INTEGER DEFAULT 0,
        tax_payment_screenshot TEXT,
        request_date TEXT,
        request_month TEXT,
        payment_date TEXT,
        transaction_id TEXT,
        approved_by INTEGER,
        verification_deadline TEXT,
        cancel_reason TEXT
    )
''')

# Referral earnings table
c.execute('''
    CREATE TABLE IF NOT EXISTS referral_earnings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        referrer_id INTEGER,
        referred_id INTEGER,
        amount INTEGER,
        date TEXT,
        referrer_ip TEXT,
        referred_ip TEXT
    )
''')

# IP tracking table
c.execute('''
    CREATE TABLE IF NOT EXISTS ip_tracking (
        ip_address TEXT,
        user_id INTEGER,
        first_seen TEXT,
        last_seen TEXT,
        referral_count INTEGER DEFAULT 0
    )
''')

# Suspicious activity table
c.execute('''
    CREATE TABLE IF NOT EXISTS suspicious_activity (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        ip_address TEXT,
        activity_type TEXT,
        description TEXT,
        timestamp TEXT
    )
''')

# Broadcast history table
c.execute('''
    CREATE TABLE IF NOT EXISTS broadcast_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        message TEXT,
        sent_by INTEGER,
        sent_date TEXT,
        total_received INTEGER,
        total_failed INTEGER
    )
''')

# Insert default tasks if empty
c.execute('SELECT COUNT(*) FROM tasks')
if c.fetchone()[0] == 0:
    default_tasks = [
        ('📢 Join Telegram Channel', 'join', 10, '{"chat_id": "https://t.me/yourchannel"}'),
        ('🔗 Refer a Friend', 'referral', 20, '{"required_referrals": 1}'),
    ]
    for name, ttype, reward, data in default_tasks:
        c.execute('INSERT INTO tasks (task_name, task_type, reward, task_data, created_date) VALUES (?, ?, ?, ?, ?)',
                  (name, ttype, reward, data, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    conn.commit()

conn.commit()

# Connect to Telegram
print("🔄 Connecting to Telegram...")
try:
    bot = TelegramClient('bot_session', API_ID, API_HASH)
    bot.start(bot_token=BOT_TOKEN)
    print("✅ Connected to Telegram!")
except Exception as e:
    print(f"⚠️ Connection warning: {e}")
    bot = TelegramClient('bot_session', API_ID, API_HASH).start(bot_token=BOT_TOKEN)

# Helper Functions
def get_setting(key):
    c.execute('SELECT value FROM bot_settings WHERE key = ?', (key,))
    result = c.fetchone()
    return result[0] if result else None

def update_setting(key, value, user_id):
    c.execute('UPDATE bot_settings SET value = ?, updated_by = ?, updated_date = ? WHERE key = ?',
              (str(value), user_id, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), key))
    conn.commit()

def is_master(user_id):
    return user_id in MASTER_IDS

def get_client_ip(event):
    return hashlib.md5(str(event.sender_id).encode()).hexdigest()[:16]

def generate_captcha():
    num1 = random.randint(1, 20)
    num2 = random.randint(1, 20)
    answer = num1 + num2
    return f"🔐 *VERIFICATION REQUIRED*\n\nWhat is {num1} + {num2}?\n\nSend the answer to continue.", str(answer)

def log_suspicious(user_id, ip, activity_type, description):
    c.execute('''
        INSERT INTO suspicious_activity (user_id, ip_address, activity_type, description, timestamp)
        VALUES (?, ?, ?, ?, ?)
    ''', (user_id, ip, activity_type, description, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    conn.commit()

def reset_daily_referrals():
    today = datetime.now().strftime('%Y-%m-%d')
    c.execute('UPDATE users SET daily_referrals = 0, last_reset_date = ? WHERE last_reset_date != ?', (today, today))
    conn.commit()

def get_user(user_id):
    c.execute('SELECT * FROM users WHERE user_id = ?', (user_id,))
    user = c.fetchone()
    if not user:
        referral_code = secrets.token_hex(8)
        c.execute('''INSERT INTO users 
                    (user_id, join_date, referral_code, is_verified)
                    VALUES (?, ?, ?, ?)''',
                  (user_id, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), referral_code, 0))
        conn.commit()
        return get_user(user_id)
    return user

def add_balance(user_id, amount):
    c.execute('UPDATE users SET balance = balance + ?, total_earned = total_earned + ? WHERE user_id = ?', 
              (amount, amount, user_id))
    conn.commit()

def get_completed_tasks(user_id):
    user = get_user(user_id)
    completed = user[4].split(',') if user[4] else []
    return [int(t) for t in completed if t]

def get_max_withdrawal_amount(tasks_completed):
    limits = {0: 50, 5: 50, 6: 100, 10: 100, 11: 200, 15: 200, 16: 350, 20: 350, 21: 500, 25: 500, 26: 1000}
    for threshold in sorted(limits.keys(), reverse=True):
        if tasks_completed >= threshold:
            return limits[threshold]
    return 50

def calculate_tax(amount):
    no_tax_limit = int(get_setting('no_tax_limit'))
    if amount <= no_tax_limit:
        return 0, 0, amount
    
    gst_percent = int(get_setting('gst_percent'))
    tds_percent = int(get_setting('tds_percent'))
    
    gst = int(amount * gst_percent / 100)
    tds = int(amount * tds_percent / 100)
    net = amount - gst - tds
    
    return gst, tds, net

def get_current_month():
    return datetime.now().strftime('%Y-%m')

def get_next_month_name():
    next_month = datetime.now().replace(day=28) + timedelta(days=4)
    return next_month.strftime('%B')

# ============== FIXED JOIN VERIFICATION (Works with Bot Admin) ==============

async def check_user_in_group(user_id, group_username):
    """Check if user is in the group using GetParticipantRequest (Bot must be admin)"""
    try:
        # Get the group entity
        entity = await bot.get_entity(group_username)
        # Check if user is participant
        participant = await bot(GetParticipantRequest(entity, user_id))
        return True, "You are a member!"
    except UserNotParticipantError:
        return False, "❌ You haven't joined the group yet!"
    except Exception as e:
        print(f"Check error: {e}")
        return False, f"❌ Error checking membership. Make sure bot is admin!"

async def auto_verify_join(user_id, task_id, group_link, reward):
    """Auto verify if user joined the group"""
    # Extract username from link
    group_username = group_link.replace('https://t.me/', '').replace('http://t.me/', '').split('?')[0]
    if not group_username.startswith('@'):
        group_username = '@' + group_username
    
    has_joined, msg = await check_user_in_group(user_id, group_username)
    
    if has_joined:
        completed = get_completed_tasks(user_id)
        if task_id not in completed:
            completed.append(task_id)
            c.execute('UPDATE users SET tasks_completed = ? WHERE user_id = ?', 
                      (','.join(map(str, completed)), user_id))
            c.execute('UPDATE users SET total_tasks_completed = total_tasks_completed + 1 WHERE user_id = ?', (user_id,))
            add_balance(user_id, reward)
            conn.commit()
            return True, f"✅ Task verified! +{reward}₹ added!"
        else:
            return False, "Task already completed!"
    else:
        return False, msg

# ============== CAPTCHA VERIFICATION ==============
async def verify_captcha(event, user_id):
    user = get_user(user_id)
    
    if user[11] == 1:
        return True
    
    captcha_text, answer = generate_captcha()
    expiry = datetime.now() + timedelta(minutes=5)
    
    c.execute('UPDATE users SET captcha_code = ?, captcha_expiry = ? WHERE user_id = ?',
              (answer, expiry.strftime('%Y-%m-%d %H:%M:%S'), user_id))
    conn.commit()
    
    await event.reply(captcha_text)
    return False

# ============== MAIN MENU ==============
async def main_menu(event, user_id):
    user = get_user(user_id)
    
    if user[11] == 0:
        await verify_captcha(event, user_id)
        return
    
    if user[12] == 1:
        await event.reply("🚫 *You are banned from using this bot!*")
        return
    
    tasks_completed = len(get_completed_tasks(user_id))
    max_withdrawal = get_max_withdrawal_amount(tasks_completed)
    required_tasks = int(get_setting('required_tasks'))
    min_referrals = int(get_setting('min_referrals'))
    
    c.execute('SELECT COUNT(*) FROM tasks WHERE is_active = 1')
    total_tasks = c.fetchone()[0] or 1
    
    progress_percent = min(10, (tasks_completed * 10) // total_tasks)
    progress_bar = "▓" * progress_percent + "░" * (10 - progress_percent)
    
    c.execute('SELECT COUNT(*) FROM users WHERE referred_by = ?', (user_id,))
    total_refs = c.fetchone()[0]
    
    req_text = ""
    if tasks_completed < required_tasks:
        req_text += f"\n⚠️ Need {required_tasks - tasks_completed} more tasks for withdrawal"
    if total_refs < min_referrals:
        req_text += f"\n⚠️ Need {min_referrals - total_refs} more referrals for withdrawal"
    
    menu_text = f"""
🔒 *SECURE TASK MASTER PRO* 🔒

┌─────────────────────────┐
│ 👤 ID: `{user_id}`          │
│ 💰 Balance: `{user[2]}₹`     │
│ 💵 Earned: `{user[5]}₹`      │
│ 👥 Referrals: `{total_refs}`     │
└─────────────────────────┘

📊 *Progress* {progress_bar} {tasks_completed}/{total_tasks}
💰 *Withdrawal Limit:* {max_withdrawal}₹
📅 *Next Withdrawal:* 1st {get_next_month_name()}{req_text}

✨ *Choose an option:*
"""
    
    buttons = [
        [Button.inline("📋 TASKS", b"tasks"), Button.inline("🔗 REFER", b"refer")],
        [Button.inline("💰 BALANCE", b"balance"), Button.inline("📊 STATS", b"stats")],
        [Button.inline("💸 WITHDRAW", b"withdraw"), Button.inline("❓ HELP", b"help")]
    ]
    
    if is_master(user_id):
        buttons.append([Button.inline("👑 MASTER PANEL", b"master_panel")])
    
    await event.respond(menu_text, buttons=buttons)

# ============== TASKS WITH JOIN BUTTONS ==============
async def show_tasks(event, user_id):
    user = get_user(user_id)
    
    if user[11] == 0:
        await verify_captcha(event, user_id)
        return
    
    if user[12] == 1:
        await event.reply("🚫 You are banned!")
        return
    
    completed_tasks = get_completed_tasks(user_id)
    
    c.execute('SELECT id, task_name, task_type, reward, task_data FROM tasks WHERE is_active = 1')
    tasks = c.fetchall()
    
    if not tasks:
        await event.answer("❌ No tasks available!", alert=True)
        return
    
    msg = "📋 *AVAILABLE TASKS*\n\n"
    msg += "🤖 *How to Complete:*\n"
    msg += "• Click on the JOIN button below\n"
    msg += "• Join the group/channel\n"
    msg += "• Then click '✅ Check & Verify'\n"
    msg += "• Bot will auto-verify and add money\n\n"
    msg += "━━━━━━━━━━━━━━━━━━━━\n\n"
    
    buttons = []
    
    for task_id, task_name, task_type, reward, task_data in tasks:
        if task_id in completed_tasks:
            msg += f"✅ ~~{task_name}~~\n   💰 +{reward}₹ (Completed)\n\n"
        else:
            msg += f"📌 *{task_name}*\n"
            msg += f"   💰 Reward: +{reward}₹\n"
            
            if task_type == 'join':
                try:
                    data = json.loads(task_data) if task_data else {}
                    chat_link = data.get('chat_id', '')
                    msg += f"   🔗 Group: {chat_link}\n\n"
                    buttons.append([Button.url(f"🔗 JOIN: {task_name[:20]}", chat_link)])
                    buttons.append([Button.inline(f"✅ Check & Verify", f"verify_task_{task_id}")])
                except:
                    msg += f"   🔗 Invalid link\n\n"
                    buttons.append([Button.inline(f"✅ Check & Verify", f"verify_task_{task_id}")])
            
            elif task_type == 'referral':
                msg += f"   👥 Refer a friend to complete this task\n\n"
                buttons.append([Button.inline(f"✅ Check & Verify", f"verify_task_{task_id}")])
            
            else:
                buttons.append([Button.inline(f"✅ Check & Verify", f"verify_task_{task_id}"]))
            
            msg += "\n"
    
    msg += f"━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"📈 *Progress:* {len(completed_tasks)}/{len(tasks)} tasks\n"
    msg += f"💰 *Current Limit:* {get_max_withdrawal_amount(len(completed_tasks))}₹\n\n"
    msg += "⚠️ *Note:* Bot must be admin in the group to verify automatically!"
    
    buttons.append([Button.inline("🔙 BACK", b"main_menu")])
    
    try:
        await event.edit(msg, buttons=buttons, link_preview=False)
    except:
        await event.reply(msg, buttons=buttons, link_preview=False)


async def verify_task(event, user_id, task_id):
    """Auto verify task and add money"""
    user = get_user(user_id)
    completed = get_completed_tasks(user_id)
    
    if task_id in completed:
        await event.answer("❌ Task already completed!", alert=True)
        await show_tasks(event, user_id)
        return
    
    c.execute('SELECT task_name, task_type, reward, task_data FROM tasks WHERE id = ? AND is_active = 1', (task_id,))
    task = c.fetchone()
    
    if not task:
        await event.answer("❌ Task not found!", alert=True)
        return
    
    task_name, task_type, reward, task_data = task
    
    await event.edit(f"🔍 *Verifying: {task_name}*\n\nPlease wait, bot is checking...")
    
    if task_type == 'join':
        try:
            data = json.loads(task_data) if task_data else {}
            chat_link = data.get('chat_id', '')
            
            success, msg = await auto_verify_join(user_id, task_id, chat_link, reward)
            
            if success:
                await event.answer(f"✅ +{reward}₹ added!", alert=True)
                success_text = f"""
✅ *TASK VERIFIED SUCCESSFULLY!*

📋 Task: {task_name}
💰 Reward: +{reward}₹

💵 New Balance: {get_user(user_id)[2]}₹

🤖 Auto-verification completed!
"""
                await event.edit(success_text, buttons=[[Button.inline("📋 VIEW MORE TASKS", b"tasks")]])
            else:
                # Extract group link for join button
                data = json.loads(task_data) if task_data else {}
                chat_link = data.get('chat_id', '')
                await event.edit(msg, buttons=[
                    [Button.url("🔗 JOIN GROUP NOW", chat_link)],
                    [Button.inline("🔄 Try Again", f"verify_task_{task_id}")],
                    [Button.inline("🔙 BACK", b"tasks")]
                ])
        except Exception as e:
            await event.edit(f"❌ Error: {str(e)}", buttons=[[Button.inline("🔙 BACK", b"tasks")]])
    
    elif task_type == 'referral':
        # Check referral task
        data = json.loads(task_data) if task_data else {}
        required = data.get('required_referrals', 1)
        
        c.execute('SELECT COUNT(*) FROM users WHERE referred_by = ?', (user_id,))
        referral_count = c.fetchone()[0]
        
        if referral_count >= required:
            completed.append(task_id)
            c.execute('UPDATE users SET tasks_completed = ? WHERE user_id = ?', 
                      (','.join(map(str, completed)), user_id))
            c.execute('UPDATE users SET total_tasks_completed = total_tasks_completed + 1 WHERE user_id = ?', (user_id,))
            add_balance(user_id, reward)
            conn.commit()
            
            await event.answer(f"✅ +{reward}₹ added!", alert=True)
            await event.edit(f"✅ *Task Verified!*\n\nYou have {referral_count} referrals!\n+{reward}₹ added!", 
                           buttons=[[Button.inline("📋 VIEW MORE TASKS", b"tasks")]])
        else:
            await event.edit(f"❌ *Task Verification Failed*\n\nYou need {required} referral(s).\nYou have {referral_count} referral(s).\n\nShare your referral link to get more referrals!", 
                           buttons=[
                               [Button.inline("🔗 GET REFERRAL LINK", b"refer")],
                               [Button.inline("🔄 Try Again", f"verify_task_{task_id}")],
                               [Button.inline("🔙 BACK", b"tasks")]
                           ])

# ============== REFERRAL ==============
async def show_referral(event, user_id):
    user = get_user(user_id)
    
    if user[11] == 0:
        await verify_captcha(event, user_id)
        return
    
    bot_username = (await bot.get_me()).username
    referral_code = user[8]
    referral_reward = int(get_setting('referral_reward'))
    min_referrals = int(get_setting('min_referrals'))
    max_ref_per_day = int(get_setting('max_ref_per_day'))
    
    reset_daily_referrals()
    user = get_user(user_id)
    
    remaining_today = max(0, max_ref_per_day - (user[17] if user[17] else 0))
    
    c.execute('SELECT COUNT(*) FROM users WHERE referred_by = ?', (user_id,))
    total_refs = c.fetchone()[0]
    
    referral_text = f"""
🔗 *YOUR SECURE REFERRAL LINK* 🔗

👥 Your Referrals: {total_refs}
💰 Per Referral: +{referral_reward}₹
🎯 Need: {min_referrals} referrals to withdraw

📊 *Today's Limit:* {user[17] if user[17] else 0}/{max_ref_per_day} used
📌 *Remaining Today:* {remaining_today}

📌 *Your Referral Code:* `{referral_code}`

🔗 *Share this link:*
`https://t.me/{bot_username}?start={referral_code}`

⚠️ *Note:* 
• When friend joins using your link, you get {referral_reward}₹
• Friend also gets {int(referral_reward/2)}₹ bonus
• Fake referrals = Permanent Ban!
"""
    
    buttons = [[Button.inline("🔙 BACK", b"main_menu")]]
    
    try:
        await event.edit(referral_text, buttons=buttons)
    except:
        await event.reply(referral_text, buttons=buttons)

# ============== BALANCE ==============
async def show_balance(event, user_id):
    user = get_user(user_id)
    tasks_completed = len(get_completed_tasks(user_id))
    max_withdrawal = get_max_withdrawal_amount(tasks_completed)
    referral_reward = int(get_setting('referral_reward'))
    
    c.execute('SELECT COUNT(*) FROM users WHERE referred_by = ?', (user_id,))
    total_refs = c.fetchone()[0]
    
    balance_text = f"""
💰 *YOUR WALLET* 💰

💵 Current Balance: `{user[2]}₹`
📊 Total Earned: `{user[5]}₹`
👥 Referral Bonus: `{total_refs * referral_reward}₹`
💸 Total Withdrawn: `{user[14] if len(user) > 14 else 0}₹`

📈 *Withdrawal Limit:* {max_withdrawal}₹
📋 Tasks Done: {tasks_completed}
👥 Referrals: {total_refs}/{int(get_setting('min_referrals'))}

💡 *Note:* 100₹+ withdrawal par 18% GST + 2% TDS lagta hai
"""
    
    buttons = [
        [Button.inline("💸 WITHDRAW", b"withdraw")],
        [Button.inline("🔙 BACK", b"main_menu")]
    ]
    
    try:
        await event.edit(balance_text, buttons=buttons)
    except:
        await event.reply(balance_text, buttons=buttons)

# ============== STATS ==============
async def show_stats(event, user_id):
    user = get_user(user_id)
    tasks_completed = len(get_completed_tasks(user_id))
    
    c.execute('SELECT COUNT(*) FROM users WHERE total_earned > ?', (user[5],))
    rank = c.fetchone()[0] + 1
    
    c.execute('SELECT COUNT(*) FROM users WHERE referred_by = ?', (user_id,))
    total_refs = c.fetchone()[0]
    
    stats_text = f"""
📈 *YOUR STATS* 📈

🏆 Rank: #{rank}
👥 Referrals: {total_refs}
📋 Tasks Done: {tasks_completed}
💰 Total Earned: {user[5]}₹
💸 Withdrawn: {user[14] if len(user) > 14 else 0}₹
🔝 Max Limit: {get_max_withdrawal_amount(tasks_completed)}₹

🔒 *Security Status:*
├ Verified: {'✅ Yes' if user[11] == 1 else '❌ No'}
├ Banned: {'❌ Yes' if user[12] == 1 else '✅ No'}
└ Auto-Verify: ✅ Active (Bot must be admin)
"""
    
    buttons = [[Button.inline("🔙 BACK", b"main_menu")]]
    
    try:
        await event.edit(stats_text, buttons=buttons)
    except:
        await event.reply(stats_text, buttons=buttons)

# ============== WITHDRAWAL ==============
async def show_withdraw(event, user_id):
    user = get_user(user_id)
    tasks_completed = len(get_completed_tasks(user_id))
    max_withdrawal = get_max_withdrawal_amount(tasks_completed)
    min_referrals = int(get_setting('min_referrals'))
    withdrawal_min = int(get_setting('withdrawal_min'))
    required_tasks = int(get_setting('required_tasks'))
    
    c.execute('SELECT COUNT(*) FROM users WHERE referred_by = ?', (user_id,))
    total_refs = c.fetchone()[0]
    
    if tasks_completed < required_tasks:
        await event.answer(f"❌ Need {required_tasks} tasks completed! You have {tasks_completed}", alert=True)
        return
    
    if total_refs < min_referrals:
        await event.answer(f"❌ Need {min_referrals} referrals! You have {total_refs}", alert=True)
        return
    
    if datetime.now().day != WITHDRAWAL_REQUEST_DAY:
        await event.answer(f"📅 Withdrawals only on 1st of month!", alert=True)
        return
    
    if user[2] < withdrawal_min:
        await event.answer(f"❌ Minimum {withdrawal_min}₹ required!", alert=True)
        return
    
    withdraw_amount = min(user[2], max_withdrawal)
    gst, tds, net_amount = calculate_tax(withdraw_amount)
    tax_total = gst + tds
    
    if withdraw_amount > int(get_setting('no_tax_limit')):
        tax_text = f"""
📊 *Tax Details:*
├ 18% GST: -{gst}₹
├ 2% TDS: -{tds}₹
└ Total Tax: {tax_total}₹

💰 *Net Amount After Tax:* {net_amount}₹

⚠️ *You need to pay {tax_total}₹ tax first!*
"""
    else:
        tax_text = f"✅ No tax for amount ≤ {get_setting('no_tax_limit')}₹"
        net_amount = withdraw_amount
        tax_total = 0
    
    user_upi = user[9] if len(user) > 9 else None
    
    withdraw_text = f"""
💸 *WITHDRAWAL REQUEST*

💰 Request Amount: {withdraw_amount}₹
📈 Your Limit: {max_withdrawal}₹
{tax_text}

📝 *Instructions:*
1️⃣ Select your UPI
2️⃣ Pay tax amount (if applicable)
3️⃣ Upload payment screenshot
4️⃣ Amount will be credited {PAYMENT_START_DAY}th-{PAYMENT_END_DAY}th
"""
    
    buttons = []
    
    if user_upi:
        buttons.append([Button.inline(f"✅ Use Saved UPI: {user_upi[:15]}", f"use_upi_{user_upi}")])
    buttons.append([Button.inline("📝 Enter New UPI", b"enter_new_upi")])
    
    if tax_total > 0:
        buttons.append([Button.inline("💰 Pay Tax & Continue", f"pay_tax_{withdraw_amount}_{net_amount}_{tax_total}")])
    else:
        buttons.append([Button.inline("💸 Confirm Withdrawal", f"confirm_withdraw_{withdraw_amount}_{net_amount}")])
    
    buttons.append([Button.inline("🔙 CANCEL", b"main_menu")])
    
    bot.pending_withdraw = {
        'amount': withdraw_amount,
        'net_amount': net_amount,
        'tax_total': tax_total,
        'gst': gst,
        'tds': tds
    }
    
    try:
        await event.edit(withdraw_text, buttons=buttons)
    except:
        await event.reply(withdraw_text, buttons=buttons)

async def use_saved_upi(event, user_id, upi_id):
    bot.selected_upi = upi_id
    await event.answer(f"✅ UPI selected: {upi_id}", alert=True)
    await show_withdraw_payment(event, user_id)

async def enter_new_upi(event, user_id):
    await event.edit("📝 *Enter your UPI ID*\n\nSend your UPI ID (e.g., name@okhdfcbank)\n\nType /cancel to cancel.")
    
    @bot.on(events.NewMessage(chats=[user_id]))
    async def get_upi(upi_event):
        upi_id = upi_event.raw_text.strip()
        if upi_id == '/cancel':
            await main_menu(upi_event, user_id)
            bot.remove_event_handler(get_upi)
            return
            
        if '@' in upi_id and '.' in upi_id:
            c.execute('UPDATE users SET upi_id = ? WHERE user_id = ?', (upi_id, user_id))
            conn.commit()
            await upi_event.reply(f"✅ UPI saved: {upi_id}")
            bot.selected_upi = upi_id
            await show_withdraw_payment(upi_event, user_id)
            bot.remove_event_handler(get_upi)
        else:
            await upi_event.reply("❌ Invalid UPI! Send like: name@okhdfcbank")

async def show_withdraw_payment(event, user_id):
    pending = getattr(bot, 'pending_withdraw', {})
    tax_total = pending.get('tax_total', 0)
    master_upi = get_setting('master_upi')
    master_qr = get_setting('master_qr')
    
    if tax_total > 0:
        payment_text = f"""
💰 *TAX PAYMENT REQUIRED*

💵 Amount to Pay: {tax_total}₹
📱 Pay to UPI: `{master_upi}`

📌 *Payment Options:*

1️⃣ *Scan QR Code*
{master_qr}

2️⃣ *Manual UPI Transfer*
Send {tax_total}₹ to: `{master_upi}`

⚠️ *After payment, send screenshot for verification!*

⏰ You have 2 hours to complete payment.
"""
        
        buttons = [
            [Button.inline("📸 Send Payment Screenshot", b"send_tax_screenshot")],
            [Button.inline("🔙 BACK", b"withdraw")]
        ]
        
        deadline = datetime.now() + timedelta(hours=2)
        bot.payment_deadline = deadline
    else:
        await process_withdrawal(event, user_id, bot.selected_upi, pending)
        return
    
    try:
        await event.edit(payment_text, buttons=buttons)
    except:
        await event.reply(payment_text, buttons=buttons)

async def send_tax_screenshot(event, user_id):
    await event.edit("📸 *Send Tax Payment Screenshot*\n\nPlease send screenshot of your payment to:\n`" + get_setting('master_upi') + "`\n\n⚠️ Screenshot must show:\n• Transaction ID\n• Amount\n• UPI ID\n\nYou have 2 hours to submit.\n\nType /cancel to cancel.")
    
    deadline = datetime.now() + timedelta(hours=2)
    
    @bot.on(events.NewMessage(chats=[user_id]))
    async def get_screenshot(msg_event):
        if msg_event.raw_text == '/cancel':
            await main_menu(msg_event, user_id)
            bot.remove_event_handler(get_screenshot)
            return
        
        if msg_event.photo:
            if datetime.now() > deadline:
                await msg_event.reply("❌ Time expired! Please restart withdrawal.")
                bot.remove_event_handler(get_screenshot)
                await main_menu(msg_event, user_id)
                return
            
            os.makedirs("tax_payments", exist_ok=True)
            photo = msg_event.photo
            file = await bot.download_media(photo, file=f"tax_payments/{user_id}_{datetime.now().timestamp()}.jpg")
            
            pending = getattr(bot, 'pending_withdraw', {})
            upi_id = getattr(bot, 'selected_upi', None)
            
            if not upi_id:
                await msg_event.reply("❌ UPI not selected! Please restart withdrawal.")
                bot.remove_event_handler(get_screenshot)
                await main_menu(msg_event, user_id)
                return
            
            current_month = get_current_month()
            
            c.execute('''
                INSERT INTO withdrawals (
                    user_id, amount, tax_amount, net_amount, upi_id, status, 
                    tax_paid, tax_payment_screenshot, request_date, request_month, 
                    payment_date, verification_deadline
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                user_id, pending.get('amount', 0), pending.get('tax_total', 0), 
                pending.get('net_amount', 0), upi_id, 'pending_verification', 1, file,
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'), current_month,
                f"{datetime.now().year}-{datetime.now().month}-{PAYMENT_START_DAY}",
                deadline.strftime('%Y-%m-%d %H:%M:%S')
            ))
            
            withdrawal_db_id = c.lastrowid
            add_balance(user_id, -pending.get('amount', 0))
            conn.commit()
            
            await msg_event.reply(f"""
✅ *Tax Payment Screenshot Received!*

📋 Withdrawal ID: #{withdrawal_db_id}
💰 Amount: {pending.get('amount', 0)}₹
💵 Net Amount: {pending.get('net_amount', 0)}₹
⏰ Verification Deadline: {deadline.strftime('%H:%M:%S')}

Admin will verify within 2 hours.
""")
            
            for master in MASTER_IDS:
                await bot.send_message(master, f"""
📸 *New Tax Payment Received*

👤 User: `{user_id}`
💰 Amount: {pending.get('amount', 0)}₹
💵 Tax Paid: {pending.get('tax_total', 0)}₹
📱 Net Payable: {pending.get('net_amount', 0)}₹
🆔 Withdrawal ID: #{withdrawal_db_id}
""")
            
            bot.remove_event_handler(get_screenshot)
            await main_menu(msg_event, user_id)
        else:
            await msg_event.reply("❌ Please send a photo/screenshot. Type /cancel to cancel.")

async def process_withdrawal(event, user_id, upi_id, pending):
    withdraw_amount = pending.get('amount', 0)
    net_amount = pending.get('net_amount', 0)
    current_month = get_current_month()
    
    c.execute('''
        INSERT INTO withdrawals (user_id, amount, tax_amount, net_amount, upi_id, status, request_date, request_month, payment_date) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (user_id, withdraw_amount, 0, net_amount, upi_id, 'pending',
          datetime.now().strftime('%Y-%m-%d %H:%M:%S'), current_month,
          f"{datetime.now().year}-{datetime.now().month}-{PAYMENT_START_DAY}"))
    
    add_balance(user_id, -withdraw_amount)
    conn.commit()
    
    await event.edit(f"✅ Withdrawal request submitted!\n💰 {net_amount}₹\n📱 {upi_id}\n💵 Payment: {PAYMENT_START_DAY}th-{PAYMENT_END_DAY}th")
    
    for master in MASTER_IDS:
        await bot.send_message(master, f"💰 New withdrawal\nUser: {user_id}\nAmount: {withdraw_amount}₹\nNet: {net_amount}₹\nUPI: {upi_id}")

# ============== HELP ==============
async def show_help(event, user_id):
    min_referrals = int(get_setting('min_referrals'))
    referral_reward = int(get_setting('referral_reward'))
    no_tax_limit = int(get_setting('no_tax_limit'))
    required_tasks = int(get_setting('required_tasks'))
    max_ref_per_day = int(get_setting('max_ref_per_day'))
    
    help_text = f"""
❓ *HELP GUIDE* ❓

📌 *How to Earn:*

1️⃣ *Complete Join Tasks*
   ├ Click "JOIN" button
   ├ Join the group/channel
   ├ Click "Check & Verify"
   └ Bot auto-adds money (Bot must be admin)

2️⃣ *Get Referrals*
   ├ Share your referral link
   └ Get {referral_reward}₹ per friend

3️⃣ *Withdraw Money*
   ├ Need {min_referrals} referrals
   ├ Need {required_tasks} tasks completed
   ├ Request on 1st of month
   ├ Pay tax (if applicable)
   ├ Upload screenshot
   └ Get paid 7th-14th

💰 *Tax Rules:*
├ Up to {no_tax_limit}₹: No tax
├ Above {no_tax_limit}₹: 18% GST + 2% TDS

🔒 *Security Features:*
├ ✓ Captcha Verification
├ ✓ Auto Task Verification
├ ✓ IP Tracking
├ ✓ Daily Referral Limit: {max_ref_per_day}
└ ✓ Required Tasks: {required_tasks}

📅 *Schedule:* Request on 1st, Payment 7th-14th
"""
    
    buttons = [[Button.inline("🔙 BACK", b"main_menu")]]
    
    try:
        await event.edit(help_text, buttons=buttons)
    except:
        await event.reply(help_text, buttons=buttons)

# ============== MASTER PANEL ==============
async def master_panel(event, user_id):
    if not is_master(user_id):
        await event.answer("❌ Master access only!", alert=True)
        return
    
    c.execute('SELECT COUNT(*) FROM users')
    total_users = c.fetchone()[0]
    
    c.execute('SELECT COUNT(*) FROM users WHERE is_verified = 0')
    pending_verification = c.fetchone()[0]
    
    c.execute('SELECT SUM(total_earned) FROM users')
    total_payout = c.fetchone()[0] or 0
    
    c.execute('SELECT COUNT(*) FROM withdrawals WHERE status IN ("pending", "pending_verification")')
    pending_withdrawals = c.fetchone()[0]
    
    c.execute('SELECT COUNT(*) FROM tasks WHERE is_active = 1')
    active_tasks = c.fetchone()[0]
    
    master_text = f"""
👑 *MASTER PANEL* 👑

📊 *STATS*
├ 👥 Users: {total_users}
├ ⏳ Pending Verification: {pending_verification}
├ 💰 Total Payout: {total_payout}₹
├ ⏳ Pending WD: {pending_withdrawals}
└ 📋 Active Tasks: {active_tasks}

🔧 *MANAGEMENT*
├ Task Management
├ Withdrawal Management
├ Broadcast Message
└ Settings
"""
    
    buttons = [
        [Button.inline("📋 TASK MANAGEMENT", b"task_management")],
        [Button.inline("💰 WITHDRAWAL MGMT", b"withdrawal_management")],
        [Button.inline("📢 BROADCAST", b"broadcast_menu")],
        [Button.inline("⚙️ SETTINGS", b"settings_menu")],
        [Button.inline("🔙 BACK", b"main_menu")]
    ]
    
    try:
        await event.edit(master_text, buttons=buttons)
    except:
        await event.reply(master_text, buttons=buttons)

# ============== TASK MANAGEMENT ==============
async def task_management(event, user_id):
    if not is_master(user_id):
        return
    
    c.execute('SELECT id, task_name, task_type, reward, is_active FROM tasks')
    tasks = c.fetchall()
    
    task_text = "📋 *TASK MANAGEMENT*\n\n"
    for task_id, name, ttype, reward, active in tasks:
        status = "✅" if active else "❌"
        task_text += f"{status} ID:{task_id} | {name}\n   Type: {ttype} | Reward: {reward}₹\n\n"
    
    buttons = [
        [Button.inline("➕ ADD TASK", b"add_task")],
        [Button.inline("✏️ EDIT TASK", b"edit_task")],
        [Button.inline("❌ DELETE TASK", b"delete_task")],
        [Button.inline("🔙 BACK", b"master_panel")]
    ]
    
    await event.edit(task_text, buttons=buttons)

async def add_task_ui(event, user_id):
    if not is_master(user_id):
        return
    
    await event.edit("📝 *ADD TASK*\n\nChoose task type:", buttons=[
        [Button.inline("🔗 Join Group/Channel", b"add_join_task")],
        [Button.inline("👤 Referral Task", b"add_referral_task")],
        [Button.inline("🔙 BACK", b"task_management")]
    ])

async def add_join_task(event, user_id):
    await event.edit("🔗 *Add Join Group/Channel Task*\n\nSend in format:\n`Task Name | Reward | Group Link`\n\nExample:\n`Join Telegram Channel | 10 | https://t.me/mychannel`\n\n⚠️ Bot must be admin in the group to verify!")
    
    @bot.on(events.NewMessage(chats=[user_id]))
    async def get_task(msg_event):
        text = msg_event.raw_text.strip()
        if '|' in text:
            parts = text.split('|')
            if len(parts) >= 3:
                name = parts[0].strip()
                reward = int(parts[1].strip())
                link = parts[2].strip()
                
                if 't.me/' not in link:
                    await msg_event.reply("❌ Invalid Telegram link! Link should contain 't.me/'")
                    return
                
                data = json.dumps({'chat_id': link})
                c.execute('INSERT INTO tasks (task_name, task_type, reward, task_data, created_by, created_date) VALUES (?, ?, ?, ?, ?, ?)',
                          (name, 'join', reward, data, user_id, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
                conn.commit()
                
                await msg_event.reply(f"✅ Join task added!\n📋 {name}\n💰 {reward}₹\n🔗 {link}\n\n⚠️ Make sure bot is admin in this group!")
                bot.remove_event_handler(get_task)
                await task_management(msg_event, user_id)
            else:
                await msg_event.reply("❌ Invalid format! Use: `Task Name | Reward | Link`")
        else:
            await msg_event.reply("❌ Invalid format! Use: `Task Name | Reward | Link`")

async def add_referral_task(event, user_id):
    await event.edit("👤 *Add Referral Task*\n\nSend in format:\n`Task Name | Reward | Required Referrals`\n\nExample:\n`Refer 1 Friend | 20 | 1`")
    
    @bot.on(events.NewMessage(chats=[user_id]))
    async def get_task(msg_event):
        text = msg_event.raw_text.strip()
        if '|' in text:
            parts = text.split('|')
            if len(parts) >= 3:
                name = parts[0].strip()
                reward = int(parts[1].strip())
                required = int(parts[2].strip())
                
                data = json.dumps({'required_referrals': required})
                c.execute('INSERT INTO tasks (task_name, task_type, reward, task_data, created_by, created_date) VALUES (?, ?, ?, ?, ?, ?)',
                          (name, 'referral', reward, data, user_id, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
                conn.commit()
                
                await msg_event.reply(f"✅ Referral task added!\n📋 {name}\n💰 {reward}₹\n👥 Required referrals: {required}")
                bot.remove_event_handler(get_task)
                await task_management(msg_event, user_id)
            else:
                await msg_event.reply("❌ Invalid format! Use: `Name | Reward | Required`")
        else:
            await msg_event.reply("❌ Invalid format! Use: `Name | Reward | Required`")

async def edit_task(event, user_id):
    if not is_master(user_id):
        return
    
    c.execute('SELECT id, task_name, reward FROM tasks')
    tasks = c.fetchall()
    
    buttons = []
    for task_id, name, reward in tasks:
        buttons.append([Button.inline(f"{name} ({reward}₹)", f"edit_task_{task_id}")])
    buttons.append([Button.inline("🔙 BACK", b"task_management")])
    
    await event.edit("✏️ *Select task to edit:*", buttons=buttons)

async def edit_task_details(event, user_id, task_id):
    c.execute('SELECT task_name, reward FROM tasks WHERE id = ?', (task_id,))
    task = c.fetchone()
    
    await event.edit(f"✏️ *Edit Task: {task[0]}*\n\n"
                     f"💰 Current Reward: {task[1]}₹\n\n"
                     f"Send new reward amount (number only):")
    
    @bot.on(events.NewMessage(chats=[user_id]))
    async def update_reward(msg_event):
        try:
            new_reward = int(msg_event.raw_text.strip())
            c.execute('UPDATE tasks SET reward = ? WHERE id = ?', (new_reward, task_id))
            conn.commit()
            await msg_event.reply(f"✅ Reward updated to {new_reward}₹")
            bot.remove_event_handler(update_reward)
            await task_management(msg_event, user_id)
        except:
            await msg_event.reply("❌ Send a number only!")

async def delete_task(event, user_id):
    if not is_master(user_id):
        return
    
    c.execute('SELECT id, task_name FROM tasks')
    tasks = c.fetchall()
    
    buttons = []
    for task_id, name in tasks:
        buttons.append([Button.inline(f"🗑️ {name}", f"delete_task_{task_id}")])
    buttons.append([Button.inline("🔙 BACK", b"task_management")])
    
    await event.edit("❌ *Select task to delete:*", buttons=buttons)

async def confirm_delete_task(event, user_id, task_id):
    c.execute('DELETE FROM tasks WHERE id = ?', (task_id,))
    conn.commit()
    await event.answer("✅ Task deleted!", alert=True)
    await task_management(event, user_id)

# ============== WITHDRAWAL MANAGEMENT ==============
async def withdrawal_management(event, user_id):
    if not is_master(user_id):
        return
    
    c.execute('''
        SELECT w.id, w.user_id, w.amount, w.net_amount, w.upi_id, w.status, 
               w.request_date, w.verification_deadline, u.username
        FROM withdrawals w
        JOIN users u ON w.user_id = u.user_id
        WHERE w.status IN ('pending', 'pending_verification')
        ORDER BY w.request_date ASC
    ''')
    
    withdrawals = c.fetchall()
    
    if not withdrawals:
        await event.edit("✅ No pending withdrawals!", buttons=[[Button.inline("🔙 BACK", b"master_panel")]])
        return
    
    text = "💰 *PENDING WITHDRAWALS*\n\n"
    buttons = []
    
    for w in withdrawals:
        wid, uid, amount, net, upi, status, date, deadline, username = w
        net_show = net if net else amount
        text += f"""
┌─ ID: #{wid}
├ 👤 User: `{uid}` (@{username or 'N/A'})
├ 💰 Amount: {amount}₹ → Net: {net_show}₹
├ 📱 UPI: `{upi}`
├ 📅 Date: {date[:10] if date else 'N/A'}
├ 📌 Status: {status}
└─────────────────

"""
        buttons.append([Button.inline(f"🔍 Verify #{wid}", f"verify_wd_{wid}")])
    
    buttons.append([Button.inline("🔙 BACK", b"master_panel")])
    
    await event.edit(text, buttons=buttons)

async def verify_withdrawal(event, user_id, withdrawal_id):
    if not is_master(user_id):
        return
    
    c.execute('SELECT user_id, amount, net_amount, upi_id, status, tax_payment_screenshot, verification_deadline FROM withdrawals WHERE id = ?', (withdrawal_id,))
    w = c.fetchone()
    
    if not w:
        await event.answer("❌ Withdrawal not found!", alert=True)
        return
    
    uid, amount, net, upi, status, screenshot, deadline = w
    net_show = net if net else amount
    
    verify_text = f"""
🔍 *VERIFY WITHDRAWAL #{withdrawal_id}*

👤 User: `{uid}`
💰 Amount: {amount}₹
💵 Net Payable: {net_show}₹
📱 UPI: `{upi}`
📸 Tax Screenshot: {'✅ Received' if screenshot else '❌ Not required'}

⏰ Deadline: {deadline if deadline else 'N/A'}

📌 *Verification Options:*
├ ✅ Approve - Valid screenshot
├ ❌ Reject - Fake/Invalid screenshot
└ ⏰ Extend Deadline - Give more time
"""
    
    buttons = [
        [Button.inline("✅ APPROVE & PAY", f"approve_wd_{withdrawal_id}")],
        [Button.inline("❌ REJECT (Fake Screenshot)", f"reject_wd_{withdrawal_id}")],
        [Button.inline("⏰ EXTEND DEADLINE", f"extend_wd_{withdrawal_id}")],
        [Button.inline("🔙 BACK", b"withdrawal_management")]
    ]
    
    await event.edit(verify_text, buttons=buttons)

async def approve_withdrawal(event, user_id, withdrawal_id):
    if not is_master(user_id):
        return
    
    c.execute('SELECT user_id, net_amount, upi_id FROM withdrawals WHERE id = ?', (withdrawal_id,))
    w = c.fetchone()
    
    if w:
        uid, net_amount, upi = w
        
        c.execute('''
            UPDATE withdrawals 
            SET status = 'completed', 
                approved_by = ?, 
                payment_date = ?
            WHERE id = ?
        ''', (user_id, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), withdrawal_id))
        conn.commit()
        
        await bot.send_message(uid, f"""
✅ *WITHDRAWAL APPROVED!*

💰 Amount: {net_amount}₹
📱 UPI: `{upi}`
📅 Date: {datetime.now().strftime('%d %B %Y')}

Amount has been sent to your UPI account.

Thank you for using Task Master! 🌟
""")
        
        await event.answer("✅ Withdrawal approved!", alert=True)
    
    await withdrawal_management(event, user_id)

async def reject_withdrawal(event, user_id, withdrawal_id):
    if not is_master(user_id):
        return
    
    c.execute('SELECT user_id, amount FROM withdrawals WHERE id = ?', (withdrawal_id,))
    w = c.fetchone()
    
    if w:
        uid, amount = w
        
        c.execute('UPDATE withdrawals SET status = "cancelled", cancel_reason = "Fake/Invalid screenshot", approved_by = ? WHERE id = ?',
                  (user_id, withdrawal_id))
        
        c.execute('UPDATE users SET is_banned = 1 WHERE user_id = ?', (uid,))
        
        add_balance(uid, amount)
        conn.commit()
        
        log_suspicious(uid, get_client_ip(event), "fake_screenshot", f"Withdrawal #{withdrawal_id} - Fake screenshot detected")
        
        await bot.send_message(uid, f"""
❌ *WITHDRAWAL REJECTED - PERMANENT BAN!*

💰 Amount: {amount}₹
📋 Reason: Fake/Invalid payment screenshot

⚠️ *YOUR ACCOUNT HAS BEEN PERMANENTLY BANNED*
This decision is final.
""")
        
        await event.answer("❌ Withdrawal rejected! User banned!", alert=True)
    
    await withdrawal_management(event, user_id)

async def extend_deadline(event, user_id, withdrawal_id):
    if not is_master(user_id):
        return
    
    new_deadline = datetime.now() + timedelta(hours=2)
    
    c.execute('UPDATE withdrawals SET verification_deadline = ? WHERE id = ?',
              (new_deadline.strftime('%Y-%m-%d %H:%M:%S'), withdrawal_id))
    conn.commit()
    
    c.execute('SELECT user_id FROM withdrawals WHERE id = ?', (withdrawal_id,))
    uid = c.fetchone()[0]
    
    await bot.send_message(uid, f"⏰ Your withdrawal verification deadline has been extended by 2 hours. New deadline: {new_deadline.strftime('%H:%M:%S')}")
    
    await event.answer("✅ Deadline extended!", alert=True)
    await withdrawal_management(event, user_id)

# ============== BROADCAST SYSTEM ==============
async def broadcast_menu(event, user_id):
    if not is_master(user_id):
        return
    
    c.execute('SELECT COUNT(*) FROM users WHERE is_verified = 1')
    total_users = c.fetchone()[0]
    
    broadcast_text = f"""
📢 *BROADCAST CENTER* 📢

👥 Verified Users: {total_users}

📝 *Instructions:*
1️⃣ Type your message below
2️⃣ Click "CONFIRM & SEND" button
3️⃣ Message will be sent to all verified users

⚠️ *Warning:* Message will be sent to ALL verified users!
"""
    
    buttons = [
        [Button.inline("📜 VIEW HISTORY", b"broadcast_history")],
        [Button.inline("🔙 BACK", b"master_panel")]
    ]
    
    await event.edit(broadcast_text, buttons=buttons)
    
    @bot.on(events.NewMessage(chats=[user_id]))
    async def get_broadcast_message(msg_event):
        broadcast_message = msg_event.raw_text.strip()
        
        if broadcast_message == '/cancel':
            await master_panel(msg_event, user_id)
            bot.remove_event_handler(get_broadcast_message)
            return
        
        bot.broadcast_msg = broadcast_message
        
        preview_text = f"""
📢 *BROADCAST PREVIEW*

📝 *Message:*
━━━━━━━━━━━━━━━━
{broadcast_message}
━━━━━━━━━━━━━━━━

👥 Will be sent to: {total_users} verified users

⚠️ Click "CONFIRM & SEND" to broadcast.
"""
        
        buttons = [
            [Button.inline("✅ CONFIRM & SEND", b"confirm_broadcast")],
            [Button.inline("❌ CANCEL", b"broadcast_menu")]
        ]
        
        await msg_event.reply(preview_text, buttons=buttons)
        bot.remove_event_handler(get_broadcast_message)

async def confirm_broadcast_send(event, user_id):
    if not is_master(user_id):
        return
    
    broadcast_message = getattr(bot, 'broadcast_msg', None)
    
    if not broadcast_message:
        await event.answer("❌ No message to send!", alert=True)
        await broadcast_menu(event, user_id)
        return
    
    await event.edit("📢 *Starting broadcast...*\n\nPlease wait...")
    
    c.execute('SELECT user_id FROM users WHERE is_verified = 1')
    users = c.fetchall()
    
    total_users = len(users)
    success_count = 0
    fail_count = 0
    current = 0
    
    progress_msg = await event.reply("🔄 Sending messages... 0%")
    
    for user in users:
        try:
            await bot.send_message(user[0], f"📢 *ANNOUNCEMENT*\n\n{broadcast_message}")
            success_count += 1
            await asyncio.sleep(0.2)
        except:
            fail_count += 1
        
        current += 1
        if current % 10 == 0 or current == total_users:
            percent = int((current / total_users) * 100)
            try:
                await progress_msg.edit(f"🔄 Sending... {percent}% ({current}/{total_users})")
            except:
                pass
    
    c.execute('''
        INSERT INTO broadcast_history (message, sent_by, sent_date, total_received, total_failed)
        VALUES (?, ?, ?, ?, ?)
    ''', (broadcast_message, user_id, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), success_count, fail_count))
    conn.commit()
    
    result_text = f"""
✅ *BROADCAST COMPLETED!*

📨 Sent to: {success_count} verified users
❌ Failed: {fail_count} users
📅 Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""
    
    await progress_msg.edit(result_text)
    bot.broadcast_msg = None
    
    buttons = [[Button.inline("🔙 BACK", b"master_panel")]]
    await event.edit(result_text, buttons=buttons)

async def broadcast_history(event, user_id):
    if not is_master(user_id):
        return
    
    c.execute('SELECT id, message, sent_by, sent_date, total_received, total_failed FROM broadcast_history ORDER BY sent_date DESC LIMIT 10')
    history = c.fetchall()
    
    if not history:
        await event.edit("📭 No broadcast history found!", buttons=[[Button.inline("🔙 BACK", b"broadcast_menu")]])
        return
    
    text = "📜 *BROADCAST HISTORY* (Last 10)\n\n"
    for h in history:
        hid, msg, by, date, received, failed = h
        text += f"┌─ ID: #{hid}\n├ 📅 Date: {date[:16]}\n├ 📨 Sent: {received} | ❌ Failed: {failed}\n├ 📝 Message: {msg[:40]}...\n└─────────────\n\n"
    
    buttons = [
        [Button.inline("🔄 REFRESH", b"broadcast_history")],
        [Button.inline("🔙 BACK", b"broadcast_menu")]
    ]
    
    await event.edit(text, buttons=buttons)

# ============== SETTINGS MENU ==============
async def settings_menu(event, user_id):
    if not is_master(user_id):
        return
    
    settings_text = f"""
⚙️ *BOT SETTINGS*

💰 *Referral Settings*
├ Referral Reward: {get_setting('referral_reward')}₹
├ Referred Bonus: {get_setting('referred_bonus')}₹
└ Min Referrals: {get_setting('min_referrals')}

💸 *Withdrawal Settings*
├ Min Amount: {get_setting('withdrawal_min')}₹
├ Required Tasks: {get_setting('required_tasks')}
├ GST: {get_setting('gst_percent')}%
├ TDS: {get_setting('tds_percent')}%
└ Tax Free Limit: {get_setting('no_tax_limit')}₹

🔒 *Security Settings*
├ Max Referrals/Day: {get_setting('max_ref_per_day')}
└ Same IP Limit: {SAME_IP_LIMIT}

🏦 *Payment Settings*
├ Master UPI: {get_setting('master_upi')}
└ QR Code: {get_setting('master_qr')[:50]}...
"""
    
    buttons = [
        [Button.inline("💰 Edit Referral Reward", b"edit_referral_reward")],
        [Button.inline("📊 Edit Min Referrals", b"edit_min_referrals")],
        [Button.inline("💸 Edit Withdrawal Min", b"edit_withdrawal_min")],
        [Button.inline("📋 Edit Required Tasks", b"edit_required_tasks")],
        [Button.inline("🔒 Edit Max Referrals/Day", b"edit_max_refs")],
        [Button.inline("🏦 Edit Master UPI/QR", b"edit_payment_settings")],
        [Button.inline("🔙 BACK", b"master_panel")]
    ]
    
    await event.edit(settings_text, buttons=buttons)

async def edit_referral_reward(event, user_id):
    await event.edit(f"💰 *Edit Referral Reward*\n\nSend new referral reward amount (number only):\nCurrent: {get_setting('referral_reward')}₹")
    
    @bot.on(events.NewMessage(chats=[user_id]))
    async def update(msg_event):
        try:
            new_value = int(msg_event.raw_text.strip())
            update_setting('referral_reward', new_value, user_id)
            update_setting('referred_bonus', new_value // 2, user_id)
            await msg_event.reply(f"✅ Referral reward updated to {new_value}₹!")
            bot.remove_event_handler(update)
            await settings_menu(msg_event, user_id)
        except:
            await msg_event.reply("❌ Send a number only!")

async def edit_min_referrals(event, user_id):
    await event.edit(f"📊 *Edit Minimum Referrals*\n\nSend new minimum referrals required for withdrawal:\nCurrent: {get_setting('min_referrals')}")
    
    @bot.on(events.NewMessage(chats=[user_id]))
    async def update(msg_event):
        try:
            new_value = int(msg_event.raw_text.strip())
            update_setting('min_referrals', new_value, user_id)
            await msg_event.reply(f"✅ Minimum referrals updated to {new_value}!")
            bot.remove_event_handler(update)
            await settings_menu(msg_event, user_id)
        except:
            await msg_event.reply("❌ Send a number only!")

async def edit_withdrawal_min(event, user_id):
    await event.edit(f"💸 *Edit Minimum Withdrawal*\n\nSend new minimum withdrawal amount (number only):\nCurrent: {get_setting('withdrawal_min')}₹")
    
    @bot.on(events.NewMessage(chats=[user_id]))
    async def update(msg_event):
        try:
            new_value = int(msg_event.raw_text.strip())
            update_setting('withdrawal_min', new_value, user_id)
            await msg_event.reply(f"✅ Minimum withdrawal updated to {new_value}₹!")
            bot.remove_event_handler(update)
            await settings_menu(msg_event, user_id)
        except:
            await msg_event.reply("❌ Send a number only!")

async def edit_required_tasks(event, user_id):
    await event.edit(f"📋 *Edit Required Tasks for Withdrawal*\n\nSend new number of tasks required:\nCurrent: {get_setting('required_tasks')}")
    
    @bot.on(events.NewMessage(chats=[user_id]))
    async def update(msg_event):
        try:
            new_value = int(msg_event.raw_text.strip())
            update_setting('required_tasks', new_value, user_id)
            await msg_event.reply(f"✅ Required tasks updated to {new_value}!")
            bot.remove_event_handler(update)
            await settings_menu(msg_event, user_id)
        except:
            await msg_event.reply("❌ Send a number only!")

async def edit_max_refs(event, user_id):
    await event.edit(f"🔒 *Edit Max Referrals Per Day*\n\nSend new maximum referrals per day:\nCurrent: {get_setting('max_ref_per_day')}")
    
    @bot.on(events.NewMessage(chats=[user_id]))
    async def update(msg_event):
        try:
            new_value = int(msg_event.raw_text.strip())
            update_setting('max_ref_per_day', new_value, user_id)
            await msg_event.reply(f"✅ Max referrals per day updated to {new_value}!")
            bot.remove_event_handler(update)
            await settings_menu(msg_event, user_id)
        except:
            await msg_event.reply("❌ Send a number only!")

async def edit_payment_settings(event, user_id):
    await event.edit("🏦 *Edit Payment Settings*\n\nSend new Master UPI ID:\nCurrent: " + get_setting('master_upi'))
    
    @bot.on(events.NewMessage(chats=[user_id]))
    async def update_upi(msg_event):
        new_upi = msg_event.raw_text.strip()
        if '@' in new_upi and '.' in new_upi:
            update_setting('master_upi', new_upi, user_id)
            await msg_event.reply(f"✅ Master UPI updated to {new_upi}!\n\nNow send new QR Code URL (or send 'skip' to keep current):")
            
            @bot.on(events.NewMessage(chats=[user_id]))
            async def update_qr(qr_event):
                new_qr = qr_event.raw_text.strip()
                if new_qr.lower() != 'skip':
                    update_setting('master_qr', new_qr, user_id)
                    await qr_event.reply(f"✅ QR Code updated!")
                else:
                    await qr_event.reply(f"✅ QR Code unchanged.")
                bot.remove_event_handler(update_qr)
                bot.remove_event_handler(update_upi)
                await settings_menu(qr_event, user_id)
        else:
            await msg_event.reply("❌ Invalid UPI! Send like: name@okhdfcbank")

# ============== CAPTCHA HANDLER ==============
@bot.on(events.NewMessage())
async def handle_captcha(event):
    user_id = event.sender_id
    user = get_user(user_id)
    
    if len(user) > 19 and user[11] == 0 and user[19]:
        try:
            answer = int(event.raw_text.strip())
            if str(answer) == user[19]:
                if user[20] and datetime.now() < datetime.strptime(user[20], '%Y-%m-%d %H:%M:%S'):
                    c.execute('UPDATE users SET is_verified = 1, captcha_code = NULL, captcha_expiry = NULL WHERE user_id = ?', (user_id,))
                    conn.commit()
                    await event.reply("✅ *Verification successful!* 🎉\n\nWelcome to Task Master!\nUse /start to begin.")
                else:
                    await event.reply("❌ Captcha expired! Please send /start again.")
            else:
                await event.reply("❌ Wrong answer! Send /start to try again.")
        except ValueError:
            pass

# ============== CALLBACK HANDLER ==============
@bot.on(events.CallbackQuery)
async def callback_handler(event):
    user_id = event.sender_id
    data = event.data.decode()
    
    print(f"🔔 Callback received: {data}")
    
    try:
        if data == "main_menu":
            await main_menu(event, user_id)
        elif data == "tasks":
            await show_tasks(event, user_id)
        elif data == "refer":
            await show_referral(event, user_id)
        elif data == "balance":
            await show_balance(event, user_id)
        elif data == "stats":
            await show_stats(event, user_id)
        elif data == "withdraw":
            await show_withdraw(event, user_id)
        elif data == "help":
            await show_help(event, user_id)
        elif data == "master_panel":
            await master_panel(event, user_id)
        elif data == "task_management":
            await task_management(event, user_id)
        elif data == "withdrawal_management":
            await withdrawal_management(event, user_id)
        elif data == "broadcast_menu":
            await broadcast_menu(event, user_id)
        elif data == "broadcast_history":
            await broadcast_history(event, user_id)
        elif data == "confirm_broadcast":
            await confirm_broadcast_send(event, user_id)
        elif data == "settings_menu":
            await settings_menu(event, user_id)
        elif data == "add_task":
            await add_task_ui(event, user_id)
        elif data == "add_join_task":
            await add_join_task(event, user_id)
        elif data == "add_referral_task":
            await add_referral_task(event, user_id)
        elif data == "edit_task":
            await edit_task(event, user_id)
        elif data == "delete_task":
            await delete_task(event, user_id)
        elif data == "edit_referral_reward":
            await edit_referral_reward(event, user_id)
        elif data == "edit_min_referrals":
            await edit_min_referrals(event, user_id)
        elif data == "edit_withdrawal_min":
            await edit_withdrawal_min(event, user_id)
        elif data == "edit_required_tasks":
            await edit_required_tasks(event, user_id)
        elif data == "edit_max_refs":
            await edit_max_refs(event, user_id)
        elif data == "edit_payment_settings":
            await edit_payment_settings(event, user_id)
        elif data == "enter_new_upi":
            await enter_new_upi(event, user_id)
        elif data == "send_tax_screenshot":
            await send_tax_screenshot(event, user_id)
        elif data.startswith("use_upi_"):
            upi_id = data.replace("use_upi_", "")
            await use_saved_upi(event, user_id, upi_id)
        elif data.startswith("pay_tax_"):
            parts = data.split("_")
            amount = int(parts[2])
            net = int(parts[3])
            tax = int(parts[4])
            bot.pending_withdraw = {'amount': amount, 'net_amount': net, 'tax_total': tax}
            await show_withdraw_payment(event, user_id)
        elif data.startswith("confirm_withdraw_"):
            parts = data.split("_")
            amount = int(parts[2])
            net = int(parts[3])
            bot.pending_withdraw = {'amount': amount, 'net_amount': net, 'tax_total': 0}
            await process_withdrawal(event, user_id, getattr(bot, 'selected_upi', None), bot.pending_withdraw)
        elif data.startswith("verify_task_"):
            task_id = int(data.split("_")[2])
            await verify_task(event, user_id, task_id)
        elif data.startswith("verify_wd_"):
            withdrawal_id = int(data.split("_")[2])
            await verify_withdrawal(event, user_id, withdrawal_id)
        elif data.startswith("approve_wd_"):
            withdrawal_id = int(data.split("_")[2])
            await approve_withdrawal(event, user_id, withdrawal_id)
        elif data.startswith("reject_wd_"):
            withdrawal_id = int(data.split("_")[2])
            await reject_withdrawal(event, user_id, withdrawal_id)
        elif data.startswith("extend_wd_"):
            withdrawal_id = int(data.split("_")[2])
            await extend_deadline(event, user_id, withdrawal_id)
        elif data.startswith("edit_task_"):
            task_id = int(data.split("_")[2])
            await edit_task_details(event, user_id, task_id)
        elif data.startswith("delete_task_"):
            task_id = int(data.split("_")[2])
            await confirm_delete_task(event, user_id, task_id)
        else:
            print(f"⚠️ Unknown callback: {data}")
        
        await event.answer()
    except Exception as e:
        print(f"❌ Error in callback: {e}")
        import traceback
        traceback.print_exc()
        await event.answer("⚠️ Error occurred!", alert=True)

# ============== START COMMAND ==============
@bot.on(events.NewMessage(pattern='/start'))
async def start(event):
    user_id = event.sender_id
    username = event.sender.username or str(user_id)
    ip = get_client_ip(event)
    
    user = get_user(user_id)
    
    c.execute('UPDATE users SET username = ?, ip_address = ? WHERE user_id = ?', (username, ip, user_id))
    
    c.execute('INSERT OR IGNORE INTO ip_tracking (ip_address, user_id, first_seen) VALUES (?, ?, ?)',
              (ip, user_id, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    c.execute('UPDATE ip_tracking SET last_seen = ? WHERE ip_address = ? AND user_id = ?',
              (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), ip, user_id))
    conn.commit()
    
    # Check referral
    msg_parts = event.raw_text.split()
    if len(msg_parts) > 1 and user[3] is None:
        code = msg_parts[1]
        c.execute('SELECT user_id FROM users WHERE referral_code = ?', (code,))
        referrer = c.fetchone()
        
        if referrer and referrer[0] != user_id:
            referrer_id = referrer[0]
            referrer_ip = get_client_ip(event)
            
            if ip == referrer_ip:
                await event.reply("❌ *REFERRAL BLOCKED!*\n\nYou cannot refer yourself or use same IP address!")
                log_suspicious(user_id, ip, "self_referral", f"Attempted self referral from IP {ip}")
            else:
                c.execute('SELECT daily_referrals FROM users WHERE user_id = ?', (referrer_id,))
                daily_refs = c.fetchone()[0] or 0
                max_refs = int(get_setting('max_ref_per_day'))
                
                if daily_refs >= max_refs:
                    await event.reply(f"❌ Referrer has reached daily limit ({max_refs})! Try again tomorrow.")
                else:
                    referral_reward = int(get_setting('referral_reward'))
                    referred_bonus = int(get_setting('referred_bonus'))
                    
                    c.execute('UPDATE users SET referred_by = ? WHERE user_id = ?', (referrer_id, user_id))
                    c.execute('UPDATE users SET total_referrals = total_referrals + 1, daily_referrals = daily_referrals + 1 WHERE user_id = ?', (referrer_id,))
                    
                    c.execute('INSERT INTO referral_earnings (referrer_id, referred_id, amount, date, referrer_ip, referred_ip) VALUES (?, ?, ?, ?, ?, ?)',
                              (referrer_id, user_id, referral_reward, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), referrer_ip, ip))
                    
                    add_balance(referrer_id, referral_reward)
                    add_balance(user_id, referred_bonus)
                    conn.commit()
                    
                    await event.reply(f"🎉 +{referred_bonus}₹ from referral!")
    
    if user[11] == 0:
        await verify_captcha(event, user_id)
    else:
        await main_menu(event, user_id)

print("=" * 60)
print("🤖 AUTO-VERIFY TASK MASTER PRO BOT STARTED!")
print("=" * 60)
print(f"👑 Master ID: {MASTER_IDS}")
print("📋 Features:")
print("   ├ ✅ JOIN Button - Direct group join")
print("   ├ ✅ Auto-Verify - Bot checks membership (Must be admin)")
print("   ├ ✅ Captcha Verification")
print("   ├ ✅ IP Tracking")
print("   ├ ✅ Referral System")
print("   └ ✅ Withdrawal with Tax")
print("=" * 60)

bot.run_until_disconnected()