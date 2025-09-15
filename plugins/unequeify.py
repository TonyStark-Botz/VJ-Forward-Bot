import re
import asyncio
import traceback
from database import db
from config import temp
from .test import get_client
from script import Script
from pyrogram import Client, filters, enums 
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from pyrogram.errors import FloodWait, MessageNotModified
import time
import shelve
import os

COMPLETED_BTN = InlineKeyboardMarkup([
    [InlineKeyboardButton('💟 Support Group', url='https://t.me/VJ_Bot_Disscussion')],
    [InlineKeyboardButton('💠 Update Channel', url='https://t.me/vj_botz')]
])
CANCEL_BTN = InlineKeyboardMarkup([[InlineKeyboardButton('• Cancel', 'terminate_frwd')]])

async def safe_edit_message(message, text, reply_markup=None):
    try:
        await message.edit(text, reply_markup=reply_markup)
    except MessageNotModified:
        pass
    except Exception as e:
        print(f"Error editing message: {e}")

class DiskSet:
    def __init__(self, filename):
        self.filename = filename
        self._ensure_dir()
        
    def _ensure_dir(self):
        os.makedirs(os.path.dirname(self.filename) if os.path.dirname(self.filename) else '.', exist_ok=True)
    
    def add(self, value):
        try:
            with shelve.open(self.filename) as db:
                db[value] = True
        except Exception as e:
            print(f"DiskSet add error: {e}")
            raise
    
    def __contains__(self, value):
        try:
            with shelve.open(self.filename) as db:
                return value in db
        except Exception as e:
            print(f"DiskSet contains error: {e}")
            return False
    
    def clear(self):
        try:
            for ext in [".dat", ".bak", ".dir"]:
                if os.path.exists(self.filename + ext):
                    os.remove(self.filename + ext)
        except Exception as e:
            print(f"DiskSet clear error: {e}")

def format_error_with_traceback(error, max_length=4000):
    """Format error with traceback but limit the length"""
    try:
        tb_str = traceback.format_exc()
        error_msg = f"**ERROR:**\n`{error}`\n\n**Traceback:**\n`{tb_str}`"
        
        # Trim if too long for Telegram message
        if len(error_msg) > max_length:
            error_msg = error_msg[:max_length] + "...\n\n**Traceback truncated due to length**"
        
        return error_msg
    except:
        return f"**ERROR:**\n`{error}`\n\n**Failed to get traceback**"

@Client.on_message(filters.command("unequify") & filters.private)
async def unequify(client, message):
    user_id = message.from_user.id
    temp.CANCEL[user_id] = False
    
    if temp.lock.get(user_id):
        return await message.reply("Please wait until previous task completes")
    
    try:
        _bot = await db.get_userbot(user_id)
        if not _bot:
            return await message.reply("Need userbot to do this process. Please add a userbot using /settings")
        
        target = await client.ask(user_id, text="Forward the last message from target chat or send last message link.\n/cancel - cancel this process")
        
        if target.text and target.text.startswith("/cancel"):
            return await message.reply("Process cancelled!")
        
        chat_id = None
        if target.text:
            regex = re.compile(r"(https://)?(t\.me/|telegram\.me/|telegram\.dog/)(c/)?(\d+|[a-zA-Z_0-9]+)/(\d+)$")
            match = regex.match(target.text.replace("?single", ""))
            if not match:
                return await message.reply('Invalid link')
            chat_id = match.group(4)
            if chat_id.isnumeric():
                chat_id = int("-100" + chat_id)
        elif hasattr(target, 'forward_from_chat') and target.forward_from_chat:
            chat_id = target.forward_from_chat.username or target.forward_from_chat.id
        else:
            return await message.reply_text("Invalid input!")
        
        confirm = await client.ask(user_id, text="Send /yes to start the process and /no to cancel")
        if confirm.text.lower() in ['/no', '/cancel']:
            return await confirm.reply("Process cancelled!")
        
        sts = await confirm.reply("Initializing...")
        
        try:
            bot = await get_client(_bot['session'], is_bot=False)
            await bot.start()
        except Exception as e:
            error_msg = format_error_with_traceback(e)
            return await sts.edit(error_msg)
        
        try:
            me = await bot.get_me()
            chat_member = await bot.get_chat_member(chat_id, me.id)
            if not (chat_member.privileges and chat_member.privileges.can_delete_messages):
                await sts.edit("Please make your userbot admin in target chat with delete permissions")
                return await bot.stop()
        except Exception as e:
            error_msg = format_error_with_traceback(e)
            await sts.edit(error_msg)
            return await bot.stop()
        
        disk_set = DiskSet(f"temp/{user_id}_unique_files")
        duplicates_count = 0
        total_count = 0
        last_update_time = time.time()
        temp.lock[user_id] = True
        temp.CANCEL[user_id] = False
        
        try:
            await safe_edit_message(sts, Script.DUPLICATE_TEXT.format(total_count, duplicates_count, "Starting..."), reply_markup=CANCEL_BTN)
            
            async for message_obj in bot.get_chat_history(chat_id):
                if temp.CANCEL.get(user_id):
                    await safe_edit_message(sts, Script.DUPLICATE_TEXT.format(total_count, duplicates_count, "Cancelled"), reply_markup=COMPLETED_BTN)
                    disk_set.clear()
                    return await bot.stop()
                
                if message_obj and message_obj.document:
                    file_unique_id = message_obj.document.file_unique_id
                    
                    if file_unique_id in disk_set:
                        try:
                            await bot.delete_messages(chat_id, message_obj.id)
                            duplicates_count += 1
                        except FloodWait as e:
                            await asyncio.sleep(e.x)
                        except Exception as e:
                            print(f"Error deleting message {message_obj.id}: {traceback.format_exc()}")
                    else:
                        disk_set.add(file_unique_id)
                    
                    total_count += 1
                    
                    current_time = time.time()
                    if total_count % 1000 == 0 or current_time - last_update_time > 30:
                        await safe_edit_message(sts, Script.DUPLICATE_TEXT.format(total_count, duplicates_count, "Processing..."), reply_markup=CANCEL_BTN)
                        last_update_time = current_time
                    
                    if total_count % 100 == 0:
                        await asyncio.sleep(0.1)
                        
        except FloodWait as e:
            await asyncio.sleep(e.x)
            # Continue processing after flood wait
            await safe_edit_message(sts, f"FloodWait: Sleeping for {e.x} seconds...", reply_markup=CANCEL_BTN)
        except Exception as e:
            temp.lock[user_id] = False 
            error_msg = format_error_with_traceback(e)
            await sts.edit(error_msg)
            disk_set.clear()
            return await bot.stop()
        
        temp.lock[user_id] = False
        await safe_edit_message(sts, Script.DUPLICATE_TEXT.format(total_count, duplicates_count, "Completed"), reply_markup=COMPLETED_BTN)
        
    except Exception as e:
        error_msg = format_error_with_traceback(e)
        await message.reply(f"Unexpected error: {error_msg}")
        return
    
    finally:
        # Cleanup in case of any failure
        try:
            disk_set.clear()
        except:
            pass
        try:
            await bot.stop()
        except:
            pass
        temp.lock[user_id] = False
        temp.CANCEL[user_id] = False
