#     ▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬
#     💜 𝗖𝗛𝗔𝗡𝗡𝗘𝗟𝗦 𝗙𝗢𝗥 𝗨𝗣𝗗𝗔𝗧𝗘𝗦 💜
#     Tᘜ Oᗯᑎᗴᖇ : https://t.me/TonyStark_Botz
#     Tᘜ ᑕOᗰᗰᑌᑎITY : https://t.me/Kanus_Network
#     ᘜITᕼᑌᗻ Iᗪ : https://github.com/TonyStark-Botz
#     BY : Kᴀɴᴜs Nᴇᴛᴡᴏʀᴋ™
#     ▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬

import motor.motor_asyncio
from datetime import datetime
from config import Config

class Db:

    def __init__(self, uri, database_name):
        self._client = motor.motor_asyncio.AsyncIOMotorClient(uri)
        self.db = self._client[database_name]
        
        # Collections
        self.bot = self.db.bots
        self.userbot = self.db.userbot 
        self.col = self.db.users
        self.nfy = self.db.notify
        self.chl = self.db.channels 
        
        # New collection for file_id storage
        self.file_ids = self.db.file_ids

    def new_user(self, id, name):
        return dict(
            id = id,
            name = name,
            ban_status=dict(
                is_banned=False,
                ban_reason="",
            ),
        )

    async def add_user(self, id, name):
        user = self.new_user(id, name)
        await self.col.insert_one(user)

    async def is_user_exist(self, id):
        user = await self.col.find_one({'id':int(id)})
        return bool(user)

    async def total_users_count(self):
        count = await self.col.count_documents({})
        return count

    async def total_users_bots_count(self):
        bcount = await self.bot.count_documents({})
        count = await self.col.count_documents({})
        return count, bcount

    async def remove_ban(self, id):
        ban_status = dict(
            is_banned=False,
            ban_reason=''
        )
        await self.col.update_one({'id': id}, {'$set': {'ban_status': ban_status}})

    async def ban_user(self, user_id, ban_reason="No Reason"):
        ban_status = dict(
            is_banned=True,
            ban_reason=ban_reason
        )
        await self.col.update_one({'id': user_id}, {'$set': {'ban_status': ban_status}})

    async def get_ban_status(self, id):
        default = dict(
            is_banned=False,
            ban_reason=''
        )
        user = await self.col.find_one({'id':int(id)})
        if not user:
            return default
        return user.get('ban_status', default)

    async def get_all_users(self):
        return self.col.find({})

    async def delete_user(self, user_id):
        await self.col.delete_many({'id': int(user_id)})

    async def get_banned(self):
        users = self.col.find({'ban_status.is_banned': True})
        b_users = [user['id'] async for user in users]
        return b_users

    async def update_configs(self, id, configs):
        await self.col.update_one({'id': int(id)}, {'$set': {'configs': configs}})

    async def get_configs(self, id):
        default = {
            'caption': None,
            'duplicate': True,
            'forward_tag': False,
            'min_size': 0,
            'max_size': 0,
            'extension': None,
            'keywords': None,
            'protect': None,
            'button': None,
            'db_uri': None,
            'filters': {
               'poll': True,
               'text': True,
               'audio': True,
               'voice': True,
               'video': True,
               'photo': True,
               'document': True,
               'animation': True,
               'sticker': True
            }
        }
        user = await self.col.find_one({'id':int(id)})
        if user:
            return user.get('configs', default)
        return default 

    async def add_bot(self, datas):
       if not await self.is_bot_exist(datas['user_id']):
          await self.bot.insert_one(datas)

    async def remove_bot(self, user_id):
       await self.bot.delete_many({'user_id': int(user_id)})

    async def get_bot(self, user_id: int):
       bot = await self.bot.find_one({'user_id': user_id})
       return bot if bot else None

    async def is_bot_exist(self, user_id):
       bot = await self.bot.find_one({'user_id': user_id})
       return bool(bot)
   
    async def add_userbot(self, datas):
       if not await self.is_userbot_exist(datas['user_id']):
          await self.userbot.insert_one(datas)

    async def remove_userbot(self, user_id):
       await self.userbot.delete_many({'user_id': int(user_id)})

    async def get_userbot(self, user_id: int):
       bot = await self.userbot.find_one({'user_id': user_id})
       return bot if bot else None

    async def is_userbot_exist(self, user_id):
       bot = await self.userbot.find_one({'user_id': user_id})
       return bool(bot)
    
    async def in_channel(self, user_id: int, chat_id: int) -> bool:
       channel = await self.chl.find_one({"user_id": int(user_id), "chat_id": int(chat_id)})
       return bool(channel)

    async def add_channel(self, user_id: int, chat_id: int, title, username):
       channel = await self.in_channel(user_id, chat_id)
       if channel:
         return False
       return await self.chl.insert_one({"user_id": user_id, "chat_id": chat_id, "title": title, "username": username})

    async def remove_channel(self, user_id: int, chat_id: int):
       channel = await self.in_channel(user_id, chat_id )
       if not channel:
         return False
       return await self.chl.delete_many({"user_id": int(user_id), "chat_id": int(chat_id)})

    async def get_channel_details(self, user_id: int, chat_id: int):
       return await self.chl.find_one({"user_id": int(user_id), "chat_id": int(chat_id)})

    async def get_user_channels(self, user_id: int):
       channels = self.chl.find({"user_id": int(user_id)})
       return [channel async for channel in channels]

    async def get_filters(self, user_id):
       filters = []
       filter = (await self.get_configs(user_id))['filters']
       for k, v in filter.items():
          if v == False:
            filters.append(str(k))
       return filters

    async def add_frwd(self, user_id):
       return await self.nfy.insert_one({'user_id': int(user_id)})

    async def rmve_frwd(self, user_id=0, all=False):
       data = {} if all else {'user_id': int(user_id)}
       return await self.nfy.delete_many(data)

    async def get_all_frwd(self):
       return self.nfy.find({})
  
    async def forwad_count(self):
        c = await self.nfy.count_documents({})
        return c
        
    async def is_forwad_exit(self, user):
        u = await self.nfy.find_one({'user_id': user})
        return bool(u)
        
    async def get_forward_details(self, user_id):
        defult = {
            'chat_id': None,
            'forward_id': None,
            'toid': None,
            'last_id': None,
            'limit': None,
            'msg_id': None,
            'start_time': None,
            'fetched': 0,
            'offset': 0,
            'deleted': 0,
            'total': 0,
            'duplicate': 0,
            'skip': 0,
            'filtered' :0
        }
        user = await self.nfy.find_one({'user_id': int(user_id)})
        if user:
            return user.get('details', defult)
        return defult
   
    async def update_forward(self, user_id, details):
        await self.nfy.update_one({'user_id': user_id}, {'$set': {'details': details}})

    # ============================
    # FILE ID STORAGE METHODS
    # ============================
    
    async def add_file_id(self, user_id, chat_id, file_id, message_id, media_type=None):
        """Store file_id for duplicate checking"""
        await self.file_ids.update_one(
            {
                'user_id': user_id, 
                'chat_id': chat_id, 
                'file_id': file_id
            },
            {
                '$set': {
                    'message_id': message_id, 
                    'media_type': media_type,
                    'timestamp': datetime.now()
                }
            },
            upsert=True
        )

    async def is_file_id_exist(self, user_id, chat_id, file_id):
        """Check if file_id exists for given user and chat"""
        doc = await self.file_ids.find_one({
            'user_id': user_id, 
            'chat_id': chat_id, 
            'file_id': file_id
        })
        return bool(doc)

    async def get_file_id_count(self, user_id, chat_id=None):
        """Get total file_ids stored for user (optionally for specific chat)"""
        query = {'user_id': user_id}
        if chat_id:
            query['chat_id'] = chat_id
        return await self.file_ids.count_documents(query)

    async def clear_user_file_ids(self, user_id, chat_id=None):
        """Clear file_ids for user (optionally for specific chat)"""
        query = {'user_id': user_id}
        if chat_id:
            query['chat_id'] = chat_id
        
        result = await self.file_ids.delete_many(query)
        return result.deleted_count

    async def get_all_file_ids(self, user_id, chat_id=None):
        """Get all file_ids for user (optionally for specific chat)"""
        query = {'user_id': user_id}
        if chat_id:
            query['chat_id'] = chat_id
            
        cursor = self.file_ids.find(query, {'file_id': 1, '_id': 0})
        return [doc['file_id'] async for doc in cursor]

    async def get_duplicate_message_ids(self, user_id, chat_id):
        """Get message IDs of duplicate files"""
        pipeline = [
            {
                '$match': {
                    'user_id': user_id,
                    'chat_id': chat_id
                }
            },
            {
                '$group': {
                    '_id': '$file_id',
                    'count': {'$sum': 1},
                    'message_ids': {'$push': '$message_id'},
                    'first_seen': {'$min': '$timestamp'}
                }
            },
            {
                '$match': {
                    'count': {'$gt': 1}
                }
            },
            {
                '$project': {
                    'file_id': '$_id',
                    'duplicate_count': '$count',
                    'message_ids': {
                        '$slice': ['$message_ids', 1, {'$size': '$message_ids'}]
                    },  # Skip first occurrence, keep duplicates
                    'first_seen': 1
                }
            }
        ]
        
        duplicates = []
        async for doc in self.file_ids.aggregate(pipeline):
            duplicates.extend(doc['message_ids'])
            
        return duplicates

    async def get_file_statistics(self, user_id, chat_id):
        """Get statistics about stored files"""
        total_files = await self.file_ids.count_documents({
            'user_id': user_id, 
            'chat_id': chat_id
        })
        
        pipeline = [
            {
                '$match': {
                    'user_id': user_id,
                    'chat_id': chat_id
                }
            },
            {
                '$group': {
                    '_id': '$file_id',
                    'count': {'$sum': 1}
                }
            },
            {
                '$group': {
                    '_id': None,
                    'unique_files': {'$sum': 1},
                    'duplicate_files': {
                        '$sum': {
                            '$cond': [{'$gt': ['$count', 1]}, 1, 0]
                        }
                    },
                    'total_duplicates': {
                        '$sum': {
                            '$cond': [{'$gt': ['$count', 1]}, {'$subtract': ['$count', 1]}, 0]
                        }
                    }
                }
            }
        ]
        
        stats = await self.file_ids.aggregate(pipeline).to_list(length=1)
        
        if stats:
            return {
                'total_files': total_files,
                'unique_files': stats[0]['unique_files'],
                'duplicate_files': stats[0]['duplicate_files'],
                'total_duplicates': stats[0]['total_duplicates']
            }
        else:
            return {
                'total_files': 0,
                'unique_files': 0,
                'duplicate_files': 0,
                'total_duplicates': 0
            }

    async def create_file_indexes(self):
        """Create indexes for better performance"""
        try:
            # Compound index for fast duplicate checking
            await self.file_ids.create_index([
                ('user_id', 1),
                ('chat_id', 1), 
                ('file_id', 1)
            ], unique=True)
            
            # Index for cleanup operations
            await self.file_ids.create_index([('timestamp', 1)])
            
            print("✅ File ID indexes created successfully")
        except Exception as e:
            print(f"⚠️ Index creation warning: {e}")

# Initialize database and create indexes
db = Db(Config.DATABASE_URI, Config.DATABASE_NAME)

# Create indexes on startup (optional - you can call this manually)
async def initialize_database():
    try:
        await db.create_file_indexes()
    except Exception as e:
        print(f"Database initialization note: {e}")

#     ▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬
#     💜 𝗖𝗛𝗔𝗡𝗡𝗘𝗟𝗦 𝗙𝗢𝗥 𝗨𝗣𝗗𝗔𝗧𝗘𝗦 💜
#     Tᘜ Oᗯᑎᗴᖇ : https://t.me/TonyStark_Botz
#     Tᘜ ᑕOᗰᗰᑌᑎITY : https://t.me/Kanus_Network
#     ᘜITᕼᑌᗻ Iᗪ : https://github.com/TonyStark-Botz
#     BY : Kᴀɴᴜs Nᴇᴛᴡᴏʀᴋ™
#     ▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬
