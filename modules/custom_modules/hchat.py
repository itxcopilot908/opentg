import asyncio
import os
import random
import tempfile
from collections import defaultdict
from pyrogram import Client, filters, enums
from pyrogram.types import Message
from utils.scripts import import_library
from utils.config import gemini_key
from utils.db import db
from utils.misc import modules_help, prefix
from modules.custom_modules.elevenlabs import generate_elevenlabs_audio
from PIL import Image
import datetime
import pytz
import requests

genai = import_library("google.generativeai", "google-generativeai")
safety_settings = [{"category": cat, "threshold": "BLOCK_NONE"} for cat in [
    "HARM_CATEGORY_DANGEROUS_CONTENT", "HARM_CATEGORY_HARASSMENT", "HARM_CATEGORY_HATE_SPEECH",
    "HARM_CATEGORY_SEXUALLY_EXPLICIT", "HARM_CATEGORY_UNSPECIFIED"]]

generation_config = {
    "max_output_tokens": 40,
}

model = genai.GenerativeModel("gemini-2.0-flash", generation_config=generation_config)
model.safety_settings = safety_settings

collection = "custom.gchat"

enabled_users = db.get(collection, "enabled_users") or []
disabled_users = db.get(collection, "disabled_users") or []
gchat_for_all = db.get(collection, "gchat_for_all") or False

smileys = ["-.-", "):", ":)", "*.*", ")*"]

la_timezone = pytz.timezone("America/Los_Angeles")

ROLES_URL = "https://gist.githubusercontent.com/iTahseen/00890d65192ca3bd9b2a62eb034b96ab/raw/roles.json"

# gpic feature toggle and group/channel id management
def get_gpic_enabled():
    enabled = db.get(collection, "gpic_enabled")
    if enabled is None:
        enabled = True
        db.set(collection, "gpic_enabled", True)
    return enabled

def set_gpic_enabled(enabled: bool):
    db.set(collection, "gpic_enabled", enabled)

def get_gpic_group_id():
    group_id = db.get(collection, "gpic_group_id")
    if group_id is None:
        group_id = -1001234567890
    return group_id

def set_gpic_group_id(group_id: int):
    db.set(collection, "gpic_group_id", int(group_id))

# ========== Outgoing Reply Queue ==========
reply_queue = asyncio.Queue()
reply_worker_started = False

async def reply_worker(client):
    while True:
        # Now the tuple can contain a cleanup callback and its args
        reply_func, args, kwargs, cleanup = await reply_queue.get()
        try:
            await reply_func(*args, **kwargs)
            if cleanup:
                await cleanup()
        except Exception as e:
            try:
                await client.send_message("me", f"Reply queue error:\n{e}")
            except Exception:
                pass
        await asyncio.sleep(1.1)

def ensure_reply_worker(client):
    global reply_worker_started
    if not reply_worker_started:
        asyncio.create_task(reply_worker(client))
        reply_worker_started = True

async def send_reply(reply_func, args, kwargs, client, cleanup=None):
    ensure_reply_worker(client)
    await reply_queue.put((reply_func, args, kwargs, cleanup))

# ========== gvoice toggle ==========
def get_voice_generation_enabled():
    enabled = db.get(collection, "voice_generation_enabled")
    if enabled is None:
        enabled = True
        db.set(collection, "voice_generation_enabled", True)
    return enabled

def set_voice_generation_enabled(enabled: bool):
    db.set(collection, "voice_generation_enabled", enabled)

# ========== gpic logic (safe & efficient) ==========
async def fetch_bot_pics(client, max_photos=200):
    group_id = get_gpic_group_id()
    photos = []
    async for msg in client.get_chat_history(group_id, limit=max_photos):
        if msg.photo:
            photos.append(msg.photo.file_id)
    return photos

async def handle_gpic_message(client, chat_id, bot_response):
    if bot_response.startswith(".gpic"):
        if not get_gpic_enabled():
            await send_reply(client.send_message, (chat_id, "The gpic feature is currently disabled."), {}, client)
            return True
        parts = bot_response.split(maxsplit=2)
        n = 1
        caption = ""
        if len(parts) >= 2 and parts[1].isdigit():
            n = int(parts[1])
        if len(parts) == 3:
            caption = parts[2]
        photos = await fetch_bot_pics(client)
        if not photos:
            await send_reply(client.send_message, (chat_id, "No bot pictures available in the group/channel."), {}, client)
            return True
        selected = random.sample(photos, min(n, len(photos)))
        if len(selected) > 1:
            from pyrogram.types import InputMediaPhoto
            media = [InputMediaPhoto(pic, caption=caption if i == 0 else "") for i, pic in enumerate(selected)]
            await send_reply(client.send_media_group, (chat_id, media), {}, client)
        else:
            await send_reply(client.send_photo, (chat_id, selected[0]), {"caption": caption}, client)
        return True
    return False

async def fetch_roles():
    try:
        response = requests.get(ROLES_URL, timeout=5)
        response.raise_for_status()
        roles = response.json()
        if isinstance(roles, dict):
            default_role_name = db.get(collection, "default_role") or "default"
            if default_role_name in roles:
                roles["default"] = roles[default_role_name]
            return roles
        return {}
    except requests.exceptions.RequestException:
        return {}

def get_chat_history(user_id, user_message, user_name):
    chat_history = db.get(collection, f"chat_history.{user_id}") or []
    chat_history.append(f"{user_name}: {user_message}")
    db.set(collection, f"chat_history.{user_id}", chat_history)
    return chat_history

def build_prompt(bot_role, chat_history, user_message):
    timestamp = datetime.datetime.now(la_timezone).strftime("%Y-%m-%d %H:%M:%S")
    chat_context = "\n".join(chat_history)
    prompt = (
        f"Time: {timestamp}\n"
        f"Role: {bot_role}\n"
        f"Chat History:\n{chat_context}\n"
        f"User Message:\n{user_message}"
    )
    return prompt

async def generate_gemini_response(input_data, chat_history, user_id):
    retries = 3
    gemini_keys = db.get(collection, "gemini_keys") or [gemini_key]
    current_key_index = db.get(collection, "current_key_index") or 0
    while retries > 0:
        try:
            current_key = gemini_keys[current_key_index]
            genai.configure(api_key=current_key)
            model = genai.GenerativeModel("gemini-2.0-flash", generation_config=generation_config)
            model.safety_settings = safety_settings
            response = model.generate_content(input_data)
            bot_response = response.text.strip()
            chat_history.append(bot_response)
            db.set(collection, f"chat_history.{user_id}", chat_history)
            return bot_response
        except Exception as e:
            if "429" in str(e) or "invalid" in str(e).lower():
                retries -= 1
                current_key_index = (current_key_index + 1) % len(gemini_keys)
                db.set(collection, "current_key_index", current_key_index)
                await asyncio.sleep(4)
            else:
                raise e

async def upload_file_to_gemini(file_path, file_type):
    uploaded_file = genai.upload_file(file_path)
    while uploaded_file.state.name == "PROCESSING":
        await asyncio.sleep(10)
        uploaded_file = genai.get_file(uploaded_file.name)
    if uploaded_file.state.name == "FAILED":
        raise ValueError(f"{file_type.capitalize()} failed to process.")
    return uploaded_file

async def send_typing_action(client, chat_id, user_message):
    await client.send_chat_action(chat_id=chat_id, action=enums.ChatAction.TYPING)
    await asyncio.sleep(min(len(user_message) / 10, 5))

async def handle_voice_message(client, chat_id, bot_response):
    voice_generation_enabled = get_voice_generation_enabled()
    if not voice_generation_enabled:
        if bot_response.startswith(".el"):
            bot_response = bot_response[3:].strip()
        await send_reply(client.send_message, (chat_id, bot_response), {}, client)
        return True
    if bot_response.startswith(".el"):
        try:
            # Generate a unique temporary file for each voice (to prevent conflicts)
            with tempfile.NamedTemporaryFile(suffix=".mp3", delete=False) as tmpfile:
                audio_path = tmpfile.name
            await generate_elevenlabs_audio(text=bot_response[3:], output_path=audio_path)
            async def cleanup():
                if os.path.exists(audio_path):
                    os.remove(audio_path)
            await send_reply(client.send_voice, (chat_id,), {"voice": audio_path}, client, cleanup=cleanup)
            return True
        except Exception:
            bot_response = bot_response[3:].strip()
            await send_reply(client.send_message, (chat_id, bot_response), {}, client)
            return True
    return False

# ========== STICKER/GIF BUFFERED HANDLER ==========
sticker_gif_buffer = defaultdict(list)
sticker_gif_timer = {}

async def process_sticker_gif_buffer(client, user_id):
    try:
        await asyncio.sleep(8)
        msgs = sticker_gif_buffer.pop(user_id, [])
        sticker_gif_timer.pop(user_id, None)
        if not msgs:
            return
        last_msg = msgs[-1]
        random_smiley = random.choice(smileys)
        await asyncio.sleep(random.uniform(5, 10))
        await send_reply(last_msg.reply_text, (random_smiley,), {}, client)
    except Exception as e:
        await send_reply(client.send_message, ("me", f"An error occurred in the sticker/gif buffer:\n\n{str(e)}"), {}, client)

@Client.on_message(
    (filters.sticker | filters.animation) & filters.private & ~filters.me & ~filters.bot, group=1
)
async def handle_sticker_gif_buffered(client: Client, message: Message):
    user_id = message.from_user.id
    if user_id in disabled_users or (not gchat_for_all and user_id not in enabled_users):
        return
    sticker_gif_buffer[user_id].append(message)
    if sticker_gif_timer.get(user_id):
        sticker_gif_timer[user_id].cancel()
    sticker_gif_timer[user_id] = asyncio.create_task(process_sticker_gif_buffer(client, user_id))

@Client.on_message(filters.text & filters.private & ~filters.me & ~filters.bot, group=1)
async def gchat(client: Client, message: Message):
    try:
        user_id, user_name, user_message = message.from_user.id, message.from_user.first_name or "User", message.text.strip()
        if user_id in disabled_users or (not gchat_for_all and user_id not in enabled_users):
            return
        roles = await fetch_roles()
        default_role = roles.get("default")
        if not default_role:
            await send_reply(client.send_message, ("me", "Error: 'default' role is missing in roles.json."), {}, client)
            return
        bot_role = db.get(collection, f"custom_roles.{user_id}") or default_role
        chat_history = get_chat_history(user_id, user_message, user_name)
        if not hasattr(client, "message_buffer"):
            client.message_buffer = {}
            client.message_timers = {}
        if user_id not in client.message_buffer:
            client.message_buffer[user_id] = []
            client.message_timers[user_id] = None
        client.message_buffer[user_id].append(user_message)
        if client.message_timers[user_id]:
            client.message_timers[user_id].cancel()
        async def process_combined_messages():
            await asyncio.sleep(8)
            buffered_messages = client.message_buffer.pop(user_id, [])
            client.message_timers[user_id] = None
            if not buffered_messages:
                return
            combined_message = " ".join(buffered_messages)
            chat_history = get_chat_history(user_id, combined_message, user_name)
            await asyncio.sleep(random.choice([3, 5, 7]))
            await send_typing_action(client, message.chat.id, combined_message)
            gemini_keys = db.get(collection, "gemini_keys") or [gemini_key]
            current_key_index = db.get(collection, "current_key_index") or 0
            retries = len(gemini_keys) * 2
            while retries > 0:
                try:
                    current_key = gemini_keys[current_key_index]
                    genai.configure(api_key=current_key)
                    model = genai.GenerativeModel("gemini-2.0-flash", generation_config=generation_config)
                    model.safety_settings = safety_settings
                    prompt = build_prompt(bot_role, chat_history, combined_message)
                    response = model.start_chat().send_message(prompt)
                    bot_response = response.text.strip()
                    if await handle_gpic_message(client, message.chat.id, bot_response):
                        return
                    chat_history.append(bot_response)
                    db.set(collection, f"chat_history.{user_id}", chat_history)
                    if await handle_voice_message(client, message.chat.id, bot_response):
                        return
                    await send_reply(message.reply_text, (bot_response,), {}, client)
                    return
                except Exception as e:
                    if "429" in str(e) or "invalid" in str(e).lower():
                        retries -= 1
                        if retries % 2 == 0:
                            current_key_index = (current_key_index + 1) % len(gemini_keys)
                            db.set(collection, "current_key_index", current_key_index)
                        await asyncio.sleep(4)
                    else:
                        await send_reply(client.send_message, ("me", f"An error occurred in the `gchat` processing:\n\n{str(e)}"), {}, client)
                        return
        client.message_timers[user_id] = asyncio.create_task(process_combined_messages())
    except Exception as e:
        await send_reply(client.send_message, ("me", f"An error occurred in the `gchat` module:\n\n{str(e)}"), {}, client)

@Client.on_message(filters.private & ~filters.me & ~filters.bot, group=1)
async def handle_files(client: Client, message: Message):
    file_path = None
    try:
        user_id, user_name = message.from_user.id, message.from_user.first_name or "User"
        if user_id in disabled_users or (not gchat_for_all and user_id not in enabled_users):
            return
        roles = await fetch_roles()
        default_role = roles.get("default")
        if not default_role:
            await send_reply(client.send_message, ("me", "Error: 'default' role is missing in roles.json."), {}, client)
            return
        bot_role = db.get(collection, f"custom_roles.{user_id}") or default_role
        caption = message.caption.strip() if message.caption else ""
        chat_history = get_chat_history(user_id, caption, user_name)
        if not hasattr(client, "image_buffer"):
            client.image_buffer = defaultdict(list)
            client.image_timers = {}
        if message.photo:
            image_path = await client.download_media(message.photo)
            client.image_buffer[user_id].append(image_path)
            if client.image_timers.get(user_id) is None:
                async def process_images():
                    await asyncio.sleep(10)
                    image_paths = client.image_buffer.pop(user_id, [])
                    client.image_timers[user_id] = None
                    if not image_paths:
                        return
                    sample_images = [Image.open(img_path) for img_path in image_paths]
                    prompt_text = "User has sent multiple images." + (f" Caption: {caption}" if caption else "")
                    prompt = build_prompt(bot_role, chat_history, prompt_text)
                    input_data = [prompt] + sample_images
                    response = await generate_gemini_response(input_data, chat_history, user_id)
                    if await handle_gpic_message(client, message.chat.id, response):
                        return
                    if await handle_voice_message(client, message.chat.id, response):
                        return
                    await send_reply(message.reply, (response,), {"reply_to_message_id": message.id}, client)
                client.image_timers[user_id] = asyncio.create_task(process_images())
            return
        file_type = None
        if message.video or message.video_note:
            file_type, file_path = "video", await client.download_media(message.video or message.video_note)
        elif message.audio or message.voice:
            file_type, file_path = "audio", await client.download_media(message.audio or message.voice)
        elif message.document and message.document.file_name.endswith(".pdf"):
            file_type, file_path = "pdf", await client.download_media(message.document)
        elif message.document:
            file_type, file_path = "document", await client.download_media(message.document)
        if file_path and file_type:
            uploaded_file = await upload_file_to_gemini(file_path, file_type)
            prompt_text = f"User has sent a {file_type}." + (f" Caption: {caption}" if caption else "")
            prompt = build_prompt(bot_role, chat_history, prompt_text)
            input_data = [prompt, uploaded_file]
            response = await generate_gemini_response(input_data, chat_history, user_id)
            if await handle_gpic_message(client, message.chat.id, response):
                return
            if await handle_voice_message(client, message.chat.id, response):
                return
            await send_reply(message.reply, (response,), {"reply_to_message_id": message.id}, client)
    except Exception as e:
        await send_reply(client.send_message, ("me", f"An error occurred in the `handle_files` function:\n\n{str(e)}"), {}, client)
    finally:
        if file_path and os.path.exists(file_path):
            os.remove(file_path)

@Client.on_message(filters.command(["gchat", "gc"], prefix) & filters.me)
async def gchat_command(client: Client, message: Message):
    try:
        parts = message.text.strip().split()
        if len(parts) < 2:
            await send_reply(message.edit_text, ("<b>Usage:</b> gchat `on`, `off`, `del`, `all`, or `r` [user_id].",), {}, client)
            return
        command = parts[1].lower()
        user_id = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else message.chat.id
        if command == "on":
            if user_id in disabled_users:
                disabled_users.remove(user_id)
                db.set(collection, "disabled_users", disabled_users)
            if user_id not in enabled_users:
                enabled_users.append(user_id)
                db.set(collection, "enabled_users", enabled_users)
            await send_reply(message.edit_text, (f"<b>ON</b> [{user_id}].",), {}, client)
        elif command == "off":
            if user_id not in disabled_users:
                disabled_users.append(user_id)
                db.set(collection, "disabled_users", disabled_users)
            if user_id in enabled_users:
                enabled_users.remove(user_id)
                db.set(collection, "enabled_users", enabled_users)
            await send_reply(message.edit_text, (f"<b>OFF</b> [{user_id}].",), {}, client)
        elif command == "del":
            db.set(collection, f"chat_history.{user_id}", None)
            await send_reply(message.edit_text, (f"<b>Deleted</b> [{user_id}].",), {}, client)
        elif command == "all":
            global gchat_for_all
            gchat_for_all = not gchat_for_all
            db.set(collection, "gchat_for_all", gchat_for_all)
            await send_reply(message.edit_text, (f"{'enabled' if gchat_for_all else 'disabled'} for all.",), {}, client)
        elif command == "r":
            changed = False
            if user_id in enabled_users:
                enabled_users.remove(user_id)
                db.set(collection, "enabled_users", enabled_users)
                changed = True
            if user_id in disabled_users:
                disabled_users.remove(user_id)
                db.set(collection, "disabled_users", disabled_users)
                changed = True
            await send_reply(
                message.edit_text,
                (f"<b>Removed</b> [{user_id}] from enabled/disabled users." if changed else f"<b>User</b> [{user_id}] not in enabled/disabled users.",),
                {}, client)
        else:
            await send_reply(message.edit_text, ("<b>Usage:</b> `gchat on`, `off`, `del`, `all`, or `r`.",), {}, client)
        await send_reply(message.delete, (), {}, client)
    except Exception as e:
        await send_reply(client.send_message, ("me", f"An error occurred in the `gchat` command:\n\n{str(e)}"), {}, client)

@Client.on_message(filters.command("gswitch", prefix) & filters.me)
async def switch_role(client: Client, message: Message):
    try:
        roles = await fetch_roles()
        if not roles:
            await send_reply(client.send_message, ("me", "Error: Failed to fetch roles."), {}, client)
            await send_reply(message.edit_text, ("<b>Failed to fetch roles.</b>",), {}, client)
            return
        user_id = message.chat.id
        parts = message.text.strip().split()
        if len(parts) == 1:
            available_roles = "\n".join([f"- {role}" for role in roles.keys()])
            await send_reply(message.edit_text, (f"<b>Available roles:</b>\n\n{available_roles}",), {}, client)
            return
        role_name = parts[1].lower()
        if role_name in roles:
            db.set(collection, f"custom_roles.{user_id}", roles[role_name])
            db.set(collection, f"chat_history.{user_id}", None)
            await send_reply(message.edit_text, (f"Switched to: <b>{role_name}</b>",), {}, client)
        else:
            await send_reply(message.edit_text, (f"Role <b>{role_name}</b> not found.",), {}, client)
        await send_reply(message.delete, (), {}, client)
    except Exception as e:
        await send_reply(client.send_message, ("me", f"Error in switch command:\n\n{str(e)}"), {}, client)

@Client.on_message(filters.command("role", prefix) & filters.me)
async def set_custom_role(client: Client, message: Message):
    try:
        roles = await fetch_roles()
        default_role = roles.get("default")
        if not default_role:
            await send_reply(client.send_message, ("me", "Error: 'default' role is missing."), {}, client)
            return
        parts = message.text.strip().split()
        user_id = message.chat.id
        custom_role = None
        if len(parts) == 2 and parts[1].isdigit():
            user_id = int(parts[1])
        elif len(parts) > 2 and parts[1].isdigit():
            user_id = int(parts[1])
            custom_role = " ".join(parts[2:]).strip()
        elif len(parts) > 1:
            custom_role = " ".join(parts[1:]).strip()
        if not custom_role:
            db.set(collection, f"custom_roles.{user_id}", default_role)
            db.set(collection, f"chat_history.{user_id}", None)
            await send_reply(message.edit_text, (f"Role reset [{user_id}].",), {}, client)
        else:
            db.set(collection, f"custom_roles.{user_id}", custom_role)
            db.set(collection, f"chat_history.{user_id}", None)
            await send_reply(message.edit_text, (f"Role set [{user_id}]!\n<b>New Role:</b> {custom_role}",), {}, client)
        await send_reply(message.delete, (), {}, client)
    except Exception as e:
        await send_reply(client.send_message, ("me", f"Error in `role` command:\n\n{str(e)}"), {}, client)

@Client.on_message(filters.command("default", prefix) & filters.me)
async def set_default_role(client: Client, message: Message):
    try:
        parts = message.text.strip().split()
        if len(parts) < 2:
            await send_reply(message.edit_text, ("<b>Usage:</b> setdefaultrole <role_name>",), {}, client)
            return
        role_name = parts[1].lower()
        roles = await fetch_roles()
        if role_name in roles:
            db.set(collection, "default_role", role_name)
            await send_reply(message.edit_text, (f"<b>Default role updated to:</b> {role_name}",), {}, client)
        else:
            await send_reply(message.edit_text, (f"<b>Error:</b> Role '{role_name}' not found in roles.json",), {}, client)
    except Exception as e:
        await send_reply(client.send_message, ("me", f"An error occurred in the `setdefaultrole` command:\n\n{str(e)}"), {}, client)

@Client.on_message(filters.command("setgkey", prefix) & filters.me)
async def set_gemini_key(client: Client, message: Message):
    try:
        command = message.text.strip().split()
        subcommand, key = command[1] if len(command) > 1 else None, command[2] if len(command) > 2 else None
        gemini_keys = db.get(collection, "gemini_keys") or []
        current_key_index = db.get(collection, "current_key_index") or 0
        if subcommand == "add" and key:
            gemini_keys.append(key)
            db.set(collection, "gemini_keys", gemini_keys)
            await send_reply(message.edit_text, ("New Gemini API key added successfully!",), {}, client)
        elif subcommand == "set" and key:
            index = int(key) - 1
            if 0 <= index < len(gemini_keys):
                current_key_index = index
                db.set(collection, "current_key_index", current_key_index)
                genai.configure(api_key=gemini_keys[current_key_index])
                model = genai.GenerativeModel("gemini-2.0-flash")
                model.safety_settings = safety_settings
                await send_reply(message.edit_text, (f"Current Gemini API key set to key {key}.",), {}, client)
            else:
                await send_reply(message.edit_text, (f"Invalid key index: {key}.",), {}, client)
        elif subcommand == "del" and key:
            index = int(key) - 1
            if 0 <= index < len(gemini_keys):
                del gemini_keys[index]
                db.set(collection, "gemini_keys", gemini_keys)
                if current_key_index >= len(gemini_keys):
                    current_key_index = max(0, len(gemini_keys) - 1)
                    db.set(collection, "current_key_index", current_key_index)
                await send_reply(message.edit_text, (f"Gemini API key {key} deleted successfully!",), {}, client)
            else:
                await send_reply(message.edit_text, (f"Invalid key index: {key}.",), {}, client)
        else:
            keys_list = "\n".join([f"{i + 1}. {key}" for i, key in enumerate(gemini_keys)])
            current_key = gemini_keys[current_key_index] if gemini_keys else "None"
            await send_reply(message.edit_text, (f"<b>Gemini API keys:</b>\n\n<code>{keys_list}</code>\n\n<b>Current key:</b> <code>{current_key}</code>",), {}, client)
        await asyncio.sleep(1)
    except Exception as e:
        await send_reply(client.send_message, ("me", f"An error occurred in the `setgkey` command:\n\n{str(e)}"), {}, client)

@Client.on_message(filters.command("gvoice", prefix) & filters.me)
async def gvoice_toggle(client: Client, message: Message):
    try:
        enabled = not get_voice_generation_enabled()
        set_voice_generation_enabled(enabled)
        status = "ENABLED" if enabled else "DISABLED"
        await send_reply(message.edit_text, (f"Voice generation is now globally <b>{status}</b>.",), {}, client)
        await send_reply(message.delete, (), {}, client)
    except Exception as e:
        await send_reply(client.send_message, ("me", f"An error occurred in the `gvoice` toggle command:\n\n{str(e)}"), {}, client)

@Client.on_message(filters.command("gpic", prefix) & filters.me)
async def gpic_admin(client: Client, message: Message):
    try:
        parts = message.text.strip().split(maxsplit=2)
        if len(parts) < 2:
            status = "ENABLED" if get_gpic_enabled() else "DISABLED"
            current_group = get_gpic_group_id()
            await send_reply(message.edit_text, (
                f"gpic is currently <b>{status}</b>.\n"
                f"Current group/channel ID: <code>{current_group}</code>\n"
                f"Usage:\n"
                f"- <code>gpic on</code> / <code>gpic off</code>\n"
                f"- <code>gpic group &lt;group_id&gt;</code>"
            ), {}, client)
            return
        cmd = parts[1].lower()
        if cmd == "on":
            set_gpic_enabled(True)
            await send_reply(message.edit_text, ("gpic feature ENABLED.",), {}, client)
        elif cmd == "off":
            set_gpic_enabled(False)
            await send_reply(message.edit_text, ("gpic feature DISABLED.",), {}, client)
        elif cmd == "group":
            if len(parts) < 3 or not parts[2].lstrip("-").isdigit():
                await send_reply(message.edit_text, ("Usage: gpic group <group_id>",), {}, client)
                return
            set_gpic_group_id(int(parts[2]))
            await send_reply(message.edit_text, (f"gpic group/channel set to <code>{parts[2]}</code>."), {}, client)
        else:
            await send_reply(message.edit_text, ("Unknown gpic command."), {}, client)
        await send_reply(message.delete, (), {}, client)
    except Exception as e:
        await send_reply(client.send_message, ("me", f"gpic admin error:\n{e}"), {}, client)

modules_help["gchat"] = {
    "gchat on [user_id]": "Enable gchat for the user.",
    "gchat off [user_id]": "Disable gchat for the user.",
    "gchat del [user_id]": "Delete chat history for the user.",
    "gchat all": "Toggle gchat for all users.",
    "gchat r [user_id]": "Remove user from enabled/disabled lists so they can be used with all subcommands.",
    "role [user_id] <custom role>": "Set a custom role for the user.",
    "switch": "Switch gchat modes.",
    "default": "Set a default role for all users.",
    "setgkey add <key>": "Add a Gemini API key.",
    "setgkey set <index>": "Set the Gemini API key.",
    "setgkey del <index>": "Delete a Gemini API key.",
    "setgkey": "Show all Gemini API keys.",
    "gvoice": "Globally toggle voice reply for everyone.",
    "gpic on/off": "Globally enable or disable gpic feature.",
    "gpic group <group_id>": "Set the group/channel used for .gpic",
    "gpic [n] [caption]": "Send n random bot pictures (from configured group/channel) with optional caption."
}
