#     ▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬
#     💜 𝗖𝗛𝗔𝗡𝗡𝗘𝗟𝗦 𝗙𝗢𝗥 𝗨𝗣𝗗𝗔𝗧𝗘𝗦 💜
#     Tᘜ Oᗯᑎᗴᖇ : https://t.me/TonyStark_Botz
#     Tᘜ ᑕOᗰᗰᑌᑎITY : https://t.me/Kanus_Network
#     ᘜITᕼᑌᗻ Iᗪ : https://github.com/TonyStark-Botz
#     BY : Kᴀɴᴜs Nᴇᴛᴡᴏʀᴋ™
#     ▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬

import re, asyncio
from database import Db, db
from config import temp
from .test import CLIENT, get_client
from script import Script
import base64
from pyrogram.file_id import FileId
from pyrogram import Client, filters, enums 
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup
import struct
from pyrogram.errors import (
    FloodWait, RPCError, ChannelInvalid, ChatAdminRequired,
    MessageDeleteForbidden, SessionPasswordNeeded
)

CLIENT = CLIENT()
COMPLETED_BTN = InlineKeyboardMarkup([
    [InlineKeyboardButton('💟 sᴜᴘᴘᴏʀᴛ ɢʀᴏᴜᴘ 💟', url='https://t.me/VJ_Bot_Disscussion')],
    [InlineKeyboardButton('💠 ᴜᴘᴅᴀᴛᴇ ᴄʜᴀɴɴᴇʟ 💠', url='https://t.me/vj_botz')]
])
CANCEL_BTN = InlineKeyboardMarkup([[InlineKeyboardButton('• ᴄᴀɴᴄᴇʟ', 'terminate_frwd')]])

def encode_file_id(s: bytes) -> str:
    r = b""
    n = 0
    for i in s + bytes([22]) + bytes([4]):
        if i == 0:
            n += 1
        else:
            if n:
                r += b"\x00" + bytes([n])
                n = 0
            r += bytes([i])
    return base64.urlsafe_b64encode(r).decode().rstrip("=")

def unpack_new_file_id(new_file_id):
    """Return file_id"""
    decoded = FileId.decode(new_file_id)
    file_id = encode_file_id(
        struct.pack(
            "<iiqq",
            int(decoded.file_type),
            decoded.dc_id,
            decoded.media_id,
            decoded.access_hash
        )
    )
    return file_id

async def safe_delete_messages(bot, chat_id, message_ids, max_retries=3):
    """Safely delete messages with retry logic"""
    for attempt in range(max_retries):
        try:
            await bot.delete_messages(chat_id, message_ids)
            return True
        except MessageDeleteForbidden:
            print("No permission to delete messages")
            return False
        except FloodWait as e:
            await asyncio.sleep(e.x)
        except Exception as e:
            print(f"Error deleting messages (attempt {attempt + 1}): {e}")
            if attempt == max_retries - 1:
                return False
            await asyncio.sleep(2 ** attempt)  # Exponential backoff
    return False

async def safe_search_messages(bot, **kwargs):
    """Safe message search with retry logic"""
    for attempt in range(5):
        try:
            return bot.search_messages(**kwargs)
        except FloodWait as e:
            await asyncio.sleep(e.x)
        except Exception as e:
            print(f"Search error (attempt {attempt + 1}): {e}")
            if attempt == 4:
                raise e
            await asyncio.sleep(2 ** attempt)

@Client.on_message(filters.command("unequify") & filters.private)
async def unequify(client, message):
    user_id = message.from_user.id
    
    # Check if previous task is running
    if temp.lock.get(user_id):
        return await message.reply("**❌ Please wait until previous task completes**")
    
    # Check for userbot
    _bot = await db.get_userbot(user_id)
    if not _bot:
        return await message.reply("<b>❌ Need userbot to do this process. Please add a userbot using /settings</b>")
    
    temp.CANCEL[user_id] = False
    temp.lock[user_id] = True
    
    try:
        # Get target chat info
        target = await client.ask(
            user_id, 
            text="**📩 Forward the last message from target chat or send last message link.**\n/cancel - `cancel this process`",
            timeout=300
        )
        
        if target.text and target.text.startswith("/cancel"):
            temp.lock[user_id] = False
            return await message.reply("**🚫 Process cancelled!**")
        
        chat_id = None
        last_msg_id = None
        
        if target.text:
            # Handle link parsing
            regex = re.compile("(https://)?(t\.me/|telegram\.me/|telegram\.dog/)(c/)?(\d+|[a-zA-Z_0-9]+)/(\d+)$")
            match = regex.match(target.text.replace("?single", ""))
            if not match:
                temp.lock[user_id] = False
                return await message.reply('**❌ Invalid link**')
            
            chat_id = match.group(4)
            last_msg_id = int(match.group(5))
            if chat_id.isnumeric():
                chat_id = int(("-100" + chat_id))
                
        elif target.forward_from_chat and target.forward_from_chat.type in [enums.ChatType.CHANNEL, enums.ChatType.SUPERGROUP]:
            last_msg_id = target.forward_from_message_id
            chat_id = target.forward_from_chat.username or target.forward_from_chat.id
        else:
            temp.lock[user_id] = False
            return await message.reply_text("**❌ Invalid input!**")
        
        # Confirmation
        confirm = await client.ask(
            user_id, 
            text="**⚠️ Send /yes to start the process or /no to cancel**",
            timeout=60
        )
        
        if confirm.text.lower() != '/yes':
            temp.lock[user_id] = False
            return await confirm.reply("**🚫 Process cancelled!**")
        
        sts = await confirm.reply("**🔄 Processing...**")
        
        # Initialize userbot client
        il = False
        data = _bot['session']
        bot = None
        
        try:
            bot = await get_client(data, is_bot=il)
            await bot.start()
        except SessionPasswordNeeded:
            temp.lock[user_id] = False
            return await sts.edit("**❌ 2FA password required for this userbot**")
        except Exception as e:
            temp.lock[user_id] = False
            return await sts.edit(f"**❌ Failed to start userbot:** `{e}`")
        
        # Check admin permissions
        try:
            test_msg = await bot.send_message(chat_id, text="🔄 Testing permissions...")
            await test_msg.delete()
        except ChatAdminRequired:
            temp.lock[user_id] = False
            await sts.edit(f"**❌ Please make your [userbot](t.me/{_bot['username']}) admin in target chat with delete permissions**")
            await bot.stop()
            return
        except Exception as e:
            temp.lock[user_id] = False
            await sts.edit(f"**❌ Cannot access chat:** `{e}`")
            await bot.stop()
            return
        
        # Main processing logic
        MESSAGES = []
        DUPLICATE = []
        total = deleted = 0
        
        try:
            await sts.edit(Script.DUPLICATE_TEXT.format(total, deleted, "🔄 Progressing..."), reply_markup=CANCEL_BTN)
            
            async for message_obj in safe_search_messages(bot, chat_id=chat_id, filter=enums.MessagesFilter.DOCUMENT):
                if temp.CANCEL.get(user_id):
                    await sts.edit(Script.DUPLICATE_TEXT.format(total, deleted, "🚫 Cancelled"), reply_markup=COMPLETED_BTN)
                    break
                
                if not message_obj.document:
                    continue
                    
                file = message_obj.document
                file_id = unpack_new_file_id(file.file_id) 
                
                if file_id in MESSAGES:
                    DUPLICATE.append(message_obj.id)
                else:
                    MESSAGES.append(file_id)
                
                total += 1
                
                # Update progress every 100 messages
                if total % 100 == 0:
                    await sts.edit(Script.DUPLICATE_TEXT.format(total, deleted, "🔄 Progressing..."), reply_markup=CANCEL_BTN)
                
                # Delete duplicates in batches of 50
                if len(DUPLICATE) >= 50:
                    success = await safe_delete_messages(bot, chat_id, DUPLICATE)
                    if success:
                        deleted += len(DUPLICATE)
                    await sts.edit(Script.DUPLICATE_TEXT.format(total, deleted, "🔄 Progressing..."), reply_markup=CANCEL_BTN)
                    DUPLICATE = []
                    
        except Exception as e:
            await sts.edit(f"**❌ ERROR**\n`{e}`")
            
        finally:
            # Final cleanup
            if DUPLICATE:
                success = await safe_delete_messages(bot, chat_id, DUPLICATE)
                if success:
                    deleted += len(DUPLICATE)
            
            temp.lock[user_id] = False
            status = "✅ Completed" if not temp.CANCEL.get(user_id) else "🚫 Cancelled"
            await sts.edit(Script.DUPLICATE_TEXT.format(total, deleted, status), reply_markup=COMPLETED_BTN)
            
            if bot:
                await bot.stop()
                
    except asyncio.TimeoutError:
        temp.lock[user_id] = False
        await message.reply("**⏰ Timeout! Process cancelled.**")
    except Exception as e:
        temp.lock[user_id] = False
        await message.reply(f"**❌ Unexpected error:** `{e}`")

#     ▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬
#     💜 𝗖𝗛𝗔𝗡𝗡𝗘𝗟𝗦 𝗙𝗢𝗥 𝗨𝗣𝗗𝗔𝗧𝗘𝗦 💜
#     Tᘜ Oᗯᑎᗴᖇ : https://t.me/TonyStark_Botz
#     Tᘜ ᑕOᗰᗰᑌᑎITY : https://t.me/Kanus_Network
#     ᘜITᕼᑌᗻ Iᗪ : https://github.com/TonyStark-Botz
#     BY : Kᴀɴᴜs Nᴇᴛᴡᴏʀᴋ™
#     ▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬
