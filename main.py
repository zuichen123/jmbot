import sys
import websockets
import json
import os
import re
import uvicorn
import jmcomic
from fastapi import FastAPI, Request
import gc
import asyncio
import psutil
import yaml
import multiprocessing
import time
from datetime import datetime
import logging
from logging.handlers import TimedRotatingFileHandler
import subprocess

# ====================== 基础配置加载 ======================
CONFIG_PATH = "config.yml"

def load_config():
    if not os.path.exists(CONFIG_PATH):
        default_config = {
            "admin_id": 627585966,
            "http_port": 8071,
            "websocket_url": "ws://127.0.0.1:13001",
            "websocket_token": "",
            "file_dir": "./pdf/",
            "log_dir": "./logs",
            "remote_user": "user",
            "remote_host": "127.0.0.1",
            "remote_temp_dir": "/tmp/",
            "local_ssh_key": "~/.ssh/id_rsa",
            "docker_internal_path": "/app/temp/",
            "max_concurrent_downloads": 2,
            "max_episodes": 20,
            "banned_id": [],
            "banned_user": [],
            "banned_group": [],
            "banned_tags": []
        }
        with open(CONFIG_PATH, "w", encoding="utf-8") as f:
            yaml.dump(default_config, f, allow_unicode=True)
        return default_config
    
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

_config = load_config()

# 映射配置到变量
ADMIN_ID = _config.get("admin_id")
HTTP_PORT = _config.get("http_port")
WEBSOCKET_URL = _config.get("websocket_url")
WEBSOCKET_TOKEN = _config.get("websocket_token")
FILE_DIR = _config.get("file_dir")
LOG_DIR = _config.get("log_dir")
REMOTE_USER = _config.get("remote_user")
REMOTE_HOST = _config.get("remote_host")
REMOTE_TEMP_DIR = _config.get("remote_temp_dir")
LOCAL_SSH_KEY = _config.get("local_ssh_key")
DOCKER_INTERNAL_PATH = _config.get("docker_internal_path")
MAX_CONCURRENT_DOWNLOADS = _config.get("max_concurrent_downloads", 2)
MAX_EPISODES = _config.get("max_episodes", 20)

# 黑名单列表
banned_id = [str(i) for i in _config.get("banned_id", [])]
banned_user = [str(u) for u in _config.get("banned_user", [])]
banned_group = [str(g) for g in _config.get("banned_group", [])]
banned_tags = _config.get("banned_tags", [])

app = FastAPI()
download_semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)

# ====================== 日志系统配置 ======================
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "jm_bot.log")

file_handler = TimedRotatingFileHandler(LOG_FILE, when="h", interval=4, backupCount=14, encoding="utf-8")
file_handler.suffix = "%Y-%m-%d_%H-%M.log"
log_formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")
file_handler.setFormatter(log_formatter)
console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)

logger = logging.getLogger("JM_BOT")
logger.setLevel(logging.INFO)
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# ====================== 工具函数 ======================
def log(tag: str, msg: str, level="info"):
    full_msg = f"{tag} {msg}"
    if level == "error":
        logger.error(full_msg)
    elif level == "warning":
        logger.warning(full_msg)
    else:
        logger.info(full_msg)

def get_total_memory_mb():
    process = psutil.Process(os.getpid())
    main_mem = process.memory_info().rss
    child_mem = 0
    try:
        for child in process.children(recursive=True):
            try:
                child_mem += child.memory_info().rss
            except psutil.NoSuchProcess:
                pass
    except psutil.NoSuchProcess:
        pass
    return main_mem / 1024 / 1024, child_mem / 1024 / 1024

def save_config():
    _config["banned_id"] = banned_id
    _config["banned_user"] = banned_user
    _config["banned_group"] = banned_group
    _config["banned_tags"] = banned_tags
    _config["max_episodes"] = MAX_EPISODES
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        yaml.dump(_config, f, allow_unicode=True, sort_keys=False, indent=4)

def scp_file_to_remote(local_file_path, remote_temp_filename=None):
    if not os.path.exists(local_file_path):
        log("[❌ SCP]", f"本地文件不存在：{local_file_path}", "error")
        return None
    if remote_temp_filename is None:
        remote_temp_filename = f"{os.path.basename(local_file_path)}"
    remote_file_path = os.path.join(REMOTE_TEMP_DIR, remote_temp_filename)
    scp_cmd = ["scp", "-i", LOCAL_SSH_KEY, "-o", "StrictHostKeyChecking=no", local_file_path, f"{REMOTE_USER}@{REMOTE_HOST}:{remote_file_path}"]
    try:
        subprocess.run(scp_cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8")
        return remote_file_path
    except subprocess.CalledProcessError as e:
        log("[❌ SCP]", f"scp上传失败：{e.stderr}", "error")
        return None

def delete_remote_file(remote_file_path):
    ssh_cmd = ["ssh", "-i", LOCAL_SSH_KEY, "-o", "StrictHostKeyChecking=no", f"{REMOTE_USER}@{REMOTE_HOST}", f"rm -f '{remote_file_path}'"]
    try:
        subprocess.run(ssh_cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8")
        return True
    except subprocess.CalledProcessError:
        return False

# ================ 信息发送类 ================
class NapcatWebSocketBot:
    def __init__(self, websocket_url, token=None):
        self.websocket_url = websocket_url
        self.headers = {}
        if token:
            self.headers["Authorization"] = f"Bearer {token}"

    async def _call_api(self, action, params, echo_prefix, timeout=30.0):
        """通用API调用方法，处理WebSocket连接并等待正确的echo响应"""
        echo_val = f"{echo_prefix}_{int(time.time()*1000)}"
        payload = {"action": action, "params": params, "echo": echo_val}
        try:
            async with websockets.connect(self.websocket_url, extra_headers=self.headers) as websocket:
                await websocket.send(json.dumps(payload))
                # 循环接收消息，直到找到匹配的echo或超时
                start_wait = time.time()
                while time.time() - start_wait < timeout:
                    try:
                        # 内部recv超时设短一点，以便能频繁检查外部循环的timeout
                        resp = await asyncio.wait_for(websocket.recv(), timeout=5.0)
                        resp_data = json.loads(resp)
                        if resp_data.get("echo") == echo_val:
                            return resp_data
                    except asyncio.TimeoutError:
                        continue
                log("[❌ Sender]", f"等待响应超时({timeout}s): {echo_val}")
                return None
        except Exception as e:
            log("[❌ Sender]", f"API调用异常({action}): {e}")
            return None

    async def send_private_message(self, user_id, message):
        params = {"user_id": user_id, "message": [{"type": "text", "data": {"text": message}}]}
        await self._call_api("send_private_msg", params, f"private_text_{user_id}")

    async def send_group_message(self, group_id, message):
        params = {"group_id": group_id, "message": [{"type": "text", "data": {"text": message}}]}
        await self._call_api("send_group_msg", params, f"group_text_{group_id}")

    async def send_file(self, target_id, file_path, is_group=False):
        log("[📤 Sync]", f"正在同步文件到远程: {os.path.basename(file_path)}")
        # 使用 to_thread 避免 SCP 阻塞事件循环
        remote_path = await asyncio.to_thread(scp_file_to_remote, file_path)
        if not remote_path: return False
        
        try:
            file_name = os.path.basename(remote_path)
            docker_path = os.path.join(DOCKER_INTERNAL_PATH, file_name)
            file_url = f"file://{docker_path}"
            
            action = "send_group_msg" if is_group else "send_private_msg"
            key = "group_id" if is_group else "user_id"
            params = {key: target_id, "message": [{"type": "file", "data": {"file": file_url}}]}
            
            # 文件发送可能很慢，设置 10 分钟超时
            resp_data = await self._call_api(action, params, f"file_{target_id}", timeout=600.0)
            
            if resp_data and resp_data.get("status") == "ok":
                return True
            else:
                log("[❌ Sender]", f"文件发送失败: {resp_data}")
                return False
        finally:
            # 无论发送成功与否，都尝试清理远程临时文件
            await asyncio.to_thread(delete_remote_file, remote_path)

# ====================== 全局状态 ======================
bot = NapcatWebSocketBot(WEBSOCKET_URL, token=WEBSOCKET_TOKEN)
client = jmcomic.JmOption.default().new_jm_client()

# ====================== 下载核心 ======================
def jm_download_worker(number, result_dict):
    try:
        option = jmcomic.create_option_by_file('./option.yml')
        jmcomic.download_album(number, option)
        result_dict["result"] = True
    except Exception as e:
        result_dict["error"] = str(e)
        result_dict["result"] = False

async def jm_download_async_wrapper(number):
    manager = multiprocessing.Manager()
    result_dict = manager.dict()
    
    p = multiprocessing.Process(target=jm_download_worker, args=(number, result_dict))
    p.start()

    timeout = 1800
    start_time = time.time()
    
    try:
        while p.is_alive():
            await asyncio.sleep(2) 
            if time.time() - start_time > timeout:
                log("[⚠️ JM]", f"下载超时({timeout}s)，终止: {number}")
                p.terminate()
                break
    except asyncio.CancelledError:
        log("[⚠️ JM]", f"任务被取消，终止进程: {number}")
        if p.is_alive():
            p.terminate()
        raise

    p.join()
    success = result_dict.get("result", False)
    error_msg = result_dict.get("error", "")
    
    del manager, result_dict
    gc.collect()
    
    if not success and error_msg:
        log("[❌ JM]", f"Worker报错: {error_msg}")
        
    return success

def find_file_by_name(title):
    safe_title = title.replace("?", "_").replace("/", "_").replace('"', "_")
    file_name = f"{safe_title}.pdf"
    file_path = os.path.join(FILE_DIR, file_name)
    if os.path.exists(file_path):
        return file_path, file_name
    return None, None

# ====================== 业务逻辑 ======================

async def process_single_jm_task(number, message_type, group_id, user_id, skip_start_msg=False):
    async def send_reply(text):
        if message_type == "group":
            await bot.send_group_message(group_id, text)
        else:
            await bot.send_private_message(user_id, text)

    if str(number) in banned_id or str(user_id) in banned_user:
        log("[🚫 Request]", f"本子{number} 请求驳回-黑名单")
        return

    if download_semaphore.locked() and not skip_start_msg:
        await send_reply(f"⏳ 正在处理其他任务，本子 {number} 已加入队列...")

    async with download_semaphore:
        log("[🏁 Task]", f"开始处理本子: {number}")
        
        try:
            # 适配 new.py 的检索方式
            page = await asyncio.to_thread(client.search_site, search_query=str(number))
            album = page.single_album
            title = album.title
            
            if not title:
                await send_reply(f"❌ 本子 {number} 标题获取失败")
                return

            # 标签屏蔽检查
            tags = album.tags
            hit_tags = [t for t in tags if t in banned_tags]
            if hit_tags:
                log("[🚫 Tag]", f"本子 {number} 触发标签屏蔽: {hit_tags}")
                await send_reply(f"❌ 本子 {number} 包含屏蔽标签: {', '.join(hit_tags)}")
                return

            if len(album.episode_list) > MAX_EPISODES:
                await send_reply(f"❌ 本子 {number} 章节过多(>{MAX_EPISODES})，跳过")
                return

        except Exception as e:
            log("[❌ JM]", f"信息检索失败 {number}: {e}")
            await send_reply(f"❌ 本子 {number} 无效或网络错误")
            return

        file_path, file_name = find_file_by_name(title)
        if file_path:
            log("[✅ Cache]", f"本地命中: {number}")
            if not skip_start_msg:
                await send_reply(f"📂 本地已存在 {number}，正在发送...")
        else:
            if not skip_start_msg:
                await send_reply(f"🚀 开始下载 {number} ({title})")
            success = await jm_download_async_wrapper(number)
            
            if not success:
                await send_reply(f"❌ 本子 {number} 下载失败")
                return
            
            file_path, file_name = find_file_by_name(title)
        
        if file_path and os.path.exists(file_path):
            file_size = os.path.getsize(file_path) / (1024 * 1024)
            tags_str = ", ".join(album.tags)
            info_msg = f"✅ 发送中：\nID：{number}\nTitle：{title}\nTags：{tags_str}\nSize：{file_size:.2f}MB"
            
            await send_reply(info_msg)
            
            is_group = (message_type == "group")
            target = group_id if is_group else user_id
            
            sent = await bot.send_file(target, file_path, is_group=is_group)
            if sent:
                log("[✅ Done]", f"本子 {number} 交付成功")
            else:
                log("[❌ Send]", f"本子 {number} 发送失败")
        else:
            log("[❌ File]", f"本子 {number} 文件未找到")

# ====================== 消息提取与正则 ======================

def extract_jm_ids(text):
    if not text: return []
    clean_text = re.sub(r'\[CQ:.*?\]', '', text)
    results = set()
    matches_prefix = re.findall(r'(?i)jm(\d+)', clean_text)
    for m in matches_prefix:
        if len(m) <= 9:
            results.add(m)
    matches_digit = re.findall(r'\b(\d{5,9})\b', clean_text) 
    for m in matches_digit:
        results.add(m)
    return list(results)

# ====================== 核心消息路由 ======================
@app.post("/")
async def root(request: Request):
    try:
        data = await request.json()
        asyncio.create_task(handle_message_event(data))
        return {"status": "success"}
    except Exception as e:
        log("[❌ System]", f"请求错误: {e}")
        return {"status": "error"}

async def handle_message_event(data):
    global MAX_EPISODES
    post_type = data.get("post_type")
    if post_type != "message":
        return

    message_type = data.get("message_type")
    raw_message = data.get("raw_message", "").strip()
    user_id = data.get("user_id")
    group_id = data.get("group_id")
    
    async def reply(text):
        if message_type == "group":
            await bot.send_group_message(group_id, text)
        else:
            await bot.send_private_message(user_id, text)

    # 帮助指令 (所有人可用)
    if raw_message == "禁漫帮助":
        help_text = (
            "📖 JMComic Bot 使用帮助\n"
            "====================\n"
            "1️⃣ 下载本子：直接发送禁漫ID（5-9位数字）\n"
            "   示例：482282 或 jm482282\n"
            "   支持单次发送多个ID进行批量下载\n\n"
            "2️⃣ 管理员指令：\n"
            "   - 开启/关闭禁漫功能 (群聊可用)\n"
            "   - 屏蔽标签 [标签名]\n"
            "   - 取消屏蔽标签 [标签名]\n"
            "   - 查看屏蔽标签\n"
            "   - 设置章节限制 [数字]\n\n"
            f"⚠️ 限制：章节数超过 {MAX_EPISODES} 的本子将不会下载"
        )
        await reply(help_text)
        return

    # 管理员指令
    if user_id == ADMIN_ID:
        if raw_message == "开启禁漫功能" and message_type == "group":
            if str(group_id) in banned_group:
                banned_group.remove(str(group_id))
                save_config()
                await reply("✅ 禁漫功能已开启")
            return
        elif raw_message == "关闭禁漫功能" and message_type == "group":
            if str(group_id) not in banned_group:
                banned_group.append(str(group_id))
                save_config()
                await reply("🚫 禁漫功能已关闭")
            return
        elif raw_message.startswith("屏蔽标签 "):
            tag = raw_message[5:].strip()
            if tag and tag not in banned_tags:
                banned_tags.append(tag)
                save_config()
                await reply(f"✅ 已添加屏蔽标签: {tag}")
            return
        elif raw_message.startswith("取消屏蔽标签 "):
            tag = raw_message[7:].strip()
            if tag in banned_tags:
                banned_tags.remove(tag)
                save_config()
                await reply(f"✅ 已移除屏蔽标签: {tag}")
            return
        elif raw_message == "查看屏蔽标签":
            msg = "🚫 当前屏蔽标签：\n" + (", ".join(banned_tags) if banned_tags else "无")
            await reply(msg)
            return
        elif raw_message.startswith("设置章节限制 "):
            try:
                new_limit = int(raw_message[7:].strip())
                MAX_EPISODES = new_limit
                save_config()
                await reply(f"✅ 章节限制已更新为: {MAX_EPISODES}")
            except ValueError:
                await reply("❌ 请输入有效的数字")
            return

    if str(group_id) in banned_group and user_id != ADMIN_ID:
        return

    ids = extract_jm_ids(raw_message)
    if not ids:
        return

    log("[🔍 Detect]", f"消息包含ID: {ids} 来自 {user_id}")

    # 如果检测到多个ID，发送汇总消息并静默单个任务的开始提示
    is_bulk = len(ids) > 1
    if is_bulk:
        summary_msg = f"🚀 正在下载 {len(ids)} 个本子"
        log("[🚀 Bulk]", summary_msg)
        await reply(summary_msg)

    for jm_id in ids:
        asyncio.create_task(
            process_single_jm_task(jm_id, message_type, group_id, user_id, skip_start_msg=is_bulk)
        )

# ====================== 内存守护 ======================
async def periodic_cleanup():
    while True:
        await asyncio.sleep(300)
        gc.collect()
        main, child = get_total_memory_mb()
        log("[🚀 Status]", f"Mem: {main:.1f}MB / Child: {child:.1f}MB | Tasks: {len(asyncio.all_tasks())}")
        
        if (main + child) > 800:
            log("[⚠️ OOM]", "内存严重超标，自杀重启...")
            sys.exit(0)

# ====================== 入口 ======================
async def main():
    log("[🚀 Init]", "服务启动...")
    asyncio.create_task(periodic_cleanup())
    
    config = uvicorn.Config(app, host="0.0.0.0", port=HTTP_PORT, access_log=False)
    server = uvicorn.Server(config)
    await server.serve()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
