import sys
import websockets
import json
import os
import re
import uvicorn
import jmcomic
from jmcomic import JmcomicText
from difflib import SequenceMatcher
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
import tempfile
import zipfile
from PyPDF2 import PdfReader, PdfWriter
import html
import shutil
import hashlib
import secrets
import string

# ====================== 基础配置 (已适配 NapCat Docker版) ======================
app = FastAPI()
admin_id = 627585966  # 管理者QQ号

# ⚠️ 端口适配：使用新的 8071 端口，避免与旧机器人冲突
HTTP_PORT = 8071  

# ⚠️ WebSocket适配：指向 Docker 映射出来的 13001 端口
WEBSOCKET_URL = "ws://10.0.0.101:13001"
WEBSOCKET_TOKEN = "1"  # ✅ 新增：Token 鉴权

FILE_DIR = "./pdf/"
LOG_DIR = "./logs"

FILE_SEND_TIMEOUT_SECONDS = 120

# ====================== 转发重定向配置 ======================
REDIRECT_GROUP_ID = 1083663846
REDIRECT_THRESHOLD = 10
FORWARD_BATCH_SIZE = 80

# ====================== 关键路径配置 (Docker 适配) ======================
# SCP 目标地址：这是宿主机上的实际路径 (NapCat 的 config 目录)
REMOTE_USER = "zuichen"
REMOTE_HOST = "10.0.0.101"  # ✅ 修改：使用本机回环地址 (前提是本机 SSH key 已配好)
REMOTE_TEMP_DIR = "/home/zuichen/Server/Napcat2/.config/QQ/temp/" 
LOCAL_SSH_KEY = "/home/zuichen/.ssh/id_rsa"

# ✅ 新增：Docker 容器内部看到的路径
# 宿主机的 .config/QQ 挂载到了容器的 /app/.config/QQ
DOCKER_INTERNAL_PATH = "/app/.config/QQ/temp/"

RANDOM_PASSWORD_LENGTH = 10

# 读取配置文件
with open("config.yml", "r", encoding="utf-8") as f:
    _config = yaml.safe_load(f)

banned_id: list[str] = [str(id) for id in _config.get("banned_id", [])]
banned_user: list[str] = [str(user) for user in _config.get("banned_user", [])]
banned_group: list[str] = [str(group) for group in _config.get("banned_group", [])]

send_mode_global: str = _config.get("send_mode_global", "pdf")
send_mode_group: dict = _config.get("send_mode_group", {})

send_name_mode_global: str = _config.get("send_name_mode_global", "full")
send_name_mode_group: dict = _config.get("send_name_mode_group", {})

enc_enabled_global: bool = bool(_config.get("enc_enabled_global", False))
enc_enabled_group: dict = _config.get("enc_enabled_group", {})

enc_password_global: str = _config.get("enc_password_global", "")
enc_password_group: dict = _config.get("enc_password_group", {})

random_password_enabled_global: bool = bool(_config.get("random_password_enabled_global", False))
random_password_enabled_group: dict = _config.get("random_password_enabled_group", {})

regex_enabled_global: bool = bool(_config.get("regex_enabled_global", False))
regex_enabled_group: dict = _config.get("regex_enabled_group", {})

# ====================== 去重配置 ======================
DEDUP_WINDOW_SECONDS = 12 * 60 * 60
recent_requests: dict[str, dict[str, float]] = {}

# ====================== 搜索状态配置 ======================
search_pending: dict[str, dict] = {} # scope -> {jm_id: str, title: str, time: float}

# ====================== 队列与转发状态 ======================
pending_counts: dict[str, int] = {}
forward_buffers: dict[str, dict] = {}
redirect_scopes: set[str] = set()

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
    for child in process.children(recursive=True):
        try:
            child_mem += child.memory_info().rss
        except psutil.NoSuchProcess:
            pass
    return main_mem / 1024 / 1024, child_mem / 1024 / 1024

def update_config():
    with open("config.yml", "w", encoding="utf-8") as f:
        yaml.dump(_config, f, allow_unicode=True, sort_keys=False, indent=4)

def is_filename_too_long_error(err: Exception) -> bool:
    if isinstance(err, OSError) and getattr(err, "errno", None) == 36:
        return True
    return "File name too long" in str(err)

def get_fs_name_max(path: str) -> int:
    try:
        return os.pathconf(path, "PC_NAME_MAX")
    except (AttributeError, ValueError, OSError):
        return 255

def trim_to_max_bytes(name: str, max_len: int) -> str:
    if max_len <= 0:
        return name
    while len(name.encode("utf-8")) > max_len and name:
        name = name[:-1]
    return name

def shorten_filename(original_name: str, max_len: int) -> str:
    stem, ext = os.path.splitext(original_name)
    safe_stem = sanitize_filename_component(stem)
    safe_ext = sanitize_filename_component(ext.lstrip("."))
    ext_part = f".{safe_ext}" if safe_ext else ""
    hash_suffix = hashlib.md5(original_name.encode("utf-8")).hexdigest()[:8]

    base_max = max_len - len(ext_part)
    if base_max <= len(hash_suffix) + 1:
        minimal = trim_to_max_bytes(hash_suffix, max_len - len(ext_part))
        return trim_to_max_bytes(f"{minimal}{ext_part}", max_len)

    prefix_len = base_max - len(hash_suffix) - 1
    prefix = trim_to_max_bytes(safe_stem, prefix_len)
    result = f"{prefix}_{hash_suffix}{ext_part}"
    return trim_to_max_bytes(result, max_len)

def describe_file(file_path: str) -> str:
    try:
        if not file_path or not os.path.exists(file_path):
            return f"path={file_path} exists=False"
        size_mb = os.path.getsize(file_path) / (1024 * 1024)
        return f"path={file_path} exists=True size={size_mb:.2f}MB"
    except Exception as e:
        return f"path={file_path} info_error={e}"

def scp_file_to_remote(local_file_path, remote_temp_filename=None):
    if not os.path.exists(local_file_path):
        log("[❌ SCP]", f"本地文件不存在：{local_file_path}", "error")
        return None

    if remote_temp_filename is None:
        remote_temp_filename = f"{os.path.basename(local_file_path)}"
    remote_file_path = os.path.join(REMOTE_TEMP_DIR, remote_temp_filename)

    scp_cmd = [
        "scp",
        "-i", LOCAL_SSH_KEY,
        "-o", "StrictHostKeyChecking=no",
        local_file_path,
        f"{REMOTE_USER}@{REMOTE_HOST}:{remote_file_path}"
    ]

    try:
        subprocess.run(
            scp_cmd,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            encoding="utf-8"
        )
        log("[✅ SCP]", f"文件已上传到远程：{remote_file_path}")
        return remote_file_path
    except subprocess.CalledProcessError as e:
        log("[❌ SCP]", f"scp上传失败：{e.stderr}", "error")
        return None

def delete_remote_file(remote_file_path):
    # 这里我们简化逻辑，因为是本机，可以直接用 os.remove 删除
    # 如果必须用 SSH 删除，保持原有逻辑即可
    # 为了保险起见，这里还是保留 SSH 逻辑，或者你可以改为 os.remove(remote_file_path) 如果权限允许
    ssh_cmd = [
        "ssh",
        "-i", LOCAL_SSH_KEY,
        "-o", "StrictHostKeyChecking=no",
        f"{REMOTE_USER}@{REMOTE_HOST}",
        f"rm -f '{remote_file_path}'" # 加引号防止文件名空格
    ]

    try:
        subprocess.run(
            ssh_cmd,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            encoding="utf-8"
        )
        log("[✅ Cleaner]", f"临时文件已删除：{remote_file_path}")
        return True
    except subprocess.CalledProcessError as e:
        log("[❌ Cleaner]", f"删除临时文件失败：{e.stderr}", "error")
        return False

def get_send_mode(group_id: int | None):
    if group_id is not None:
        group_mode = send_mode_group.get(str(group_id))
        if group_mode in ("pdf", "zip"):
            return group_mode
    return send_mode_global if send_mode_global in ("pdf", "zip") else "pdf"

def set_global_send_mode(mode: str):
    global send_mode_global
    send_mode_global = mode
    _config["send_mode_global"] = mode
    update_config()

def set_group_send_mode(group_id: int, mode: str):
    send_mode_group[str(group_id)] = mode
    _config["send_mode_group"] = send_mode_group
    update_config()

def get_send_name_mode(group_id: int | None):
    if group_id is not None:
        group_mode = send_name_mode_group.get(str(group_id))
        if group_mode in ("jm", "full"):
            return group_mode
    return send_name_mode_global if send_name_mode_global in ("jm", "full") else "full"

def set_global_send_name_mode(mode: str):
    global send_name_mode_global
    send_name_mode_global = mode
    _config["send_name_mode_global"] = mode
    update_config()

def set_group_send_name_mode(group_id: int, mode: str):
    send_name_mode_group[str(group_id)] = mode
    _config["send_name_mode_group"] = send_name_mode_group
    update_config()

def get_enc_enabled(group_id: int | None):
    if group_id is not None:
        group_enabled = enc_enabled_group.get(str(group_id))
        if isinstance(group_enabled, bool):
            return group_enabled
    return bool(enc_enabled_global)

def set_global_enc_enabled(enabled: bool):
    global enc_enabled_global
    enc_enabled_global = enabled
    _config["enc_enabled_global"] = enabled
    update_config()

def set_group_enc_enabled(group_id: int, enabled: bool):
    enc_enabled_group[str(group_id)] = enabled
    _config["enc_enabled_group"] = enc_enabled_group
    update_config()

def get_random_password_enabled(group_id: int | None):
    if group_id is not None:
        group_enabled = random_password_enabled_group.get(str(group_id))
        if isinstance(group_enabled, bool):
            return group_enabled
    return bool(random_password_enabled_global)

def set_global_random_password_enabled(enabled: bool):
    global random_password_enabled_global
    random_password_enabled_global = enabled
    _config["random_password_enabled_global"] = enabled
    update_config()

def set_group_random_password_enabled(group_id: int, enabled: bool):
    random_password_enabled_group[str(group_id)] = enabled
    _config["random_password_enabled_group"] = random_password_enabled_group
    update_config()

def get_enc_password(group_id: int | None):
    if group_id is not None:
        group_pwd = enc_password_group.get(str(group_id))
        if group_pwd:
            return str(group_pwd)
    return str(enc_password_global) if enc_password_global else ""

def set_global_enc_password(password: str):
    global enc_password_global
    enc_password_global = password
    _config["enc_password_global"] = password
    update_config()

def set_group_enc_password(group_id: int, password: str):
    enc_password_group[str(group_id)] = password
    _config["enc_password_group"] = enc_password_group
    update_config()

def get_regex_enabled(group_id: int | None):
    if group_id is not None:
        group_enabled = regex_enabled_group.get(str(group_id))
        if isinstance(group_enabled, bool):
            return group_enabled
    return bool(regex_enabled_global)

def set_global_regex_enabled(enabled: bool):
    global regex_enabled_global
    regex_enabled_global = enabled
    _config["regex_enabled_global"] = enabled
    update_config()

def set_group_regex_enabled(group_id: int, enabled: bool):
    regex_enabled_group[str(group_id)] = enabled
    _config["regex_enabled_group"] = regex_enabled_group
    update_config()

def sanitize_filename_component(value: str) -> str:
    sanitized = re.sub(r'[\\/:*?"<>|]+', "_", value)
    return sanitized.strip()

def sanitize_pdf_title(title: str) -> str:
    sanitized = re.sub(r'[\\/:*?"<>|]+', "_", title)
    sanitized = re.sub(r"\s+", " ", sanitized).strip().strip(".")
    return sanitized

def sanitize_filename_for_transfer(filename: str) -> str:
    stem, ext = os.path.splitext(filename)
    safe_stem = sanitize_filename_component(stem)
    safe_ext = sanitize_filename_component(ext.lstrip("."))
    if safe_ext:
        return f"{safe_stem}.{safe_ext}"
    return safe_stem

def sanitize_filename_for_transfer_strict(filename: str) -> str:
    stem, ext = os.path.splitext(filename)
    safe_stem = re.sub(r"[^A-Za-z0-9._-]+", "_", stem).strip("._")
    if not safe_stem:
        safe_stem = "file"
    safe_ext = re.sub(r"[^A-Za-z0-9]+", "", ext.lstrip("."))
    if safe_ext:
        return f"{safe_stem}.{safe_ext}"
    return safe_stem

def generate_random_password(length: int = RANDOM_PASSWORD_LENGTH) -> str:
    alphabet = string.ascii_letters + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(max(4, length)))

def generate_random_jm_id() -> str:
    length = secrets.randbelow(7) + 3
    first_digit = secrets.choice("123456789")
    remaining = "".join(secrets.choice("0123456789") for _ in range(length - 1))
    return f"{first_digit}{remaining}"

def prepare_send_file_with_name(file_path: str, desired_base_name: str) -> tuple[str, bool]:
    base_name = sanitize_filename_component(desired_base_name).strip()
    if not base_name:
        base_name = "JM"
    ext = os.path.splitext(file_path)[1]
    temp_dir = tempfile.gettempdir()
    name_max = get_fs_name_max(temp_dir)

    desired_name = f"{base_name}{ext}"
    if len(desired_name.encode("utf-8")) > name_max:
        desired_name = shorten_filename(desired_name, name_max)

    desired_name = sanitize_filename_for_transfer(desired_name)
    desired_path = os.path.join(temp_dir, desired_name)

    if os.path.basename(file_path) == desired_name:
        return file_path, False

    if os.path.exists(desired_path):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        desired_path = os.path.join(
            temp_dir,
            f"{os.path.splitext(desired_name)[0]}_{timestamp}{os.path.splitext(desired_name)[1]}"
        )

    shutil.copyfile(file_path, desired_path)
    return desired_path, True

def prepare_file_for_scp(file_path: str, force_safe: bool = False) -> tuple[str, bool]:
    base_name = os.path.basename(file_path)
    temp_dir = tempfile.gettempdir()
    name_max = get_fs_name_max(temp_dir)

    if not force_safe and len(base_name.encode("utf-8")) <= name_max:
        return file_path, False

    if force_safe:
        safe_name = sanitize_filename_for_transfer_strict(base_name)
    else:
        safe_name = shorten_filename(base_name, name_max)
        safe_name = sanitize_filename_for_transfer(safe_name)

    safe_name = trim_to_max_bytes(safe_name, name_max)

    safe_path = os.path.join(temp_dir, safe_name)
    if os.path.exists(safe_path):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_path = os.path.join(
            temp_dir,
            f"{os.path.splitext(safe_name)[0]}_{timestamp}{os.path.splitext(safe_name)[1]}"
        )

    shutil.copyfile(file_path, safe_path)
    return safe_path, True

def randomize_file_hash(file_path: str) -> tuple[str, bool, str | None]:
    if not os.path.exists(file_path):
        return file_path, False, None

    base_name = os.path.basename(file_path)
    temp_dir = tempfile.mkdtemp(prefix="jm_hash_")
    randomized_path = os.path.join(temp_dir, base_name)

    shutil.copyfile(file_path, randomized_path)

    try:
        with open(randomized_path, "ab") as f:
            f.write(b"\n")
            f.write(secrets.token_bytes(16))
    except Exception as e:
        log("[❌ HASH]", f"随机化文件哈希失败: {e}", "error")
        try:
            os.remove(randomized_path)
        except Exception:
            pass
        try:
            os.rmdir(temp_dir)
        except Exception:
            pass
        return file_path, False, None

    return randomized_path, True, temp_dir

def build_encrypted_pdf(pdf_path: str, password: str):
    if not os.path.exists(pdf_path):
        return None
    base_name = os.path.splitext(os.path.basename(pdf_path))[0]
    safe_password = sanitize_filename_component(password) or "password"
    temp_dir = tempfile.gettempdir()
    name_max = get_fs_name_max(temp_dir)

    file_name = f"{base_name}_{safe_password}.pdf"
    if len(file_name.encode("utf-8")) > name_max:
        file_name = shorten_filename(file_name, name_max)
    enc_path = os.path.join(temp_dir, file_name)

    if os.path.exists(enc_path):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        enc_path = os.path.join(temp_dir, f"{os.path.splitext(file_name)[0]}_{timestamp}{os.path.splitext(file_name)[1]}")

    try:
        reader = PdfReader(pdf_path)
        writer = PdfWriter()
        for page in reader.pages:
            writer.add_page(page)
        writer.encrypt(password)
        with open(enc_path, "wb") as f:
            writer.write(f)
        return enc_path
    except Exception as e:
        if is_filename_too_long_error(e):
            try:
                file_name = shorten_filename(os.path.basename(file_name), name_max)
                enc_path = os.path.join(temp_dir, file_name)
                reader = PdfReader(pdf_path)
                writer = PdfWriter()
                for page in reader.pages:
                    writer.add_page(page)
                writer.encrypt(password)
                with open(enc_path, "wb") as f:
                    writer.write(f)
                return enc_path
            except Exception as retry_error:
                log("[❌ PDF]", f"加密失败: {retry_error}", "error")
        else:
            log("[❌ PDF]", f"加密失败: {e}", "error")
        try:
            if os.path.exists(enc_path):
                os.remove(enc_path)
        except Exception:
            pass
        return None

def build_zip_for_file(file_path: str, zip_base_name: str):
    if not os.path.exists(file_path):
        return None
    temp_dir = tempfile.gettempdir()
    name_max = get_fs_name_max(temp_dir)

    safe_zip_base_name = sanitize_filename_for_transfer(zip_base_name)
    zip_file_name = f"{safe_zip_base_name}.zip"
    if len(zip_file_name.encode("utf-8")) > name_max:
        zip_file_name = shorten_filename(zip_file_name, name_max)

    zip_path = os.path.join(temp_dir, zip_file_name)
    if os.path.exists(zip_path):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        zip_path = os.path.join(
            temp_dir,
            f"{os.path.splitext(zip_file_name)[0]}_{timestamp}{os.path.splitext(zip_file_name)[1]}"
        )
    try:
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.write(file_path, arcname=os.path.basename(file_path))
        return zip_path
    except Exception as e:
        if is_filename_too_long_error(e):
            try:
                zip_file_name = shorten_filename(os.path.basename(zip_file_name), name_max)
                zip_path = os.path.join(temp_dir, zip_file_name)
                with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                    zf.write(file_path, arcname=os.path.basename(file_path))
                return zip_path
            except Exception as retry_error:
                log("[❌ ZIP]", f"压缩失败: {retry_error}", "error")
        else:
            log("[❌ ZIP]", f"压缩失败: {e}", "error")
        try:
            if os.path.exists(zip_path):
                os.remove(zip_path)
        except Exception:
            pass
        return None

def strip_cq_codes(message: str) -> str:
    return re.sub(r"\[CQ:[^\]]*\]", "", message)

def extract_jm_numbers(message: str) -> list[str]:
    cleaned_message = strip_cq_codes(message)
    return re.findall(r"\d+", cleaned_message)

def extract_jm_numbers_with_regex(message: str) -> list[str]:
    cleaned_message = strip_cq_codes(message)
    matches = re.findall(r"\bjm(\d+)\b", cleaned_message, flags=re.IGNORECASE)
    return matches

def extract_text_from_event(data) -> str:
    message = data.get("message")
    if isinstance(message, list):
        text_parts = []
        for segment in message:
            if segment.get("type") == "text":
                text_parts.append(segment.get("data", {}).get("text", ""))
        return "".join(text_parts)
    if isinstance(message, str):
        return message
    return data.get("raw_message", "")

def extract_jm_numbers_from_event(data, regex_enabled: bool) -> list[str]:
    text = extract_text_from_event(data)
    if regex_enabled:
        return extract_jm_numbers_with_regex(text)
    return extract_jm_numbers(text)

def is_short_number(number: str | int) -> bool:
    return len(str(number)) < 4

def get_request_scope(message_type: str, group_id: int | None, user_id: int | None) -> str:
    if message_type == "group" and group_id is not None:
        return f"group:{group_id}"
    return f"private:{user_id}"

def cleanup_recent_requests(scope_key: str):
    now = time.time()
    scope_map = recent_requests.get(scope_key, {})
    expired_keys = [k for k, ts in scope_map.items() if now - ts > DEDUP_WINDOW_SECONDS]
    for key in expired_keys:
        del scope_map[key]
    if scope_map:
        recent_requests[scope_key] = scope_map
    else:
        recent_requests.pop(scope_key, None)

def is_recent_request(scope_key: str, number: str | int) -> bool:
    cleanup_recent_requests(scope_key)
    scope_map = recent_requests.get(scope_key, {})
    ts = scope_map.get(str(number))
    return ts is not None and (time.time() - ts) <= DEDUP_WINDOW_SECONDS

def mark_request(scope_key: str, number: str | int):
    recent_requests.setdefault(scope_key, {})[str(number)] = time.time()

def normalize_search_keyword(keyword: str) -> str:
    cleaned = html.unescape(keyword).strip()
    while True:
        updated = re.sub(r'^\s*(?:\([^\)]*\)|\[[^\]]*\])\s*', "", cleaned)
        if updated == cleaned:
            break
        cleaned = updated.strip()
    return cleaned

def rename_pdf_after_download(number: str | int, title: str | None):
    if number is None or not os.path.isdir(FILE_DIR):
        return None

    source_path = None
    for name in (f"{number}.pdf", f"JM{number}.pdf"):
        candidate = os.path.join(FILE_DIR, name)
        if os.path.exists(candidate):
            source_path = candidate
            break

    if not source_path:
        return None

    if not title:
        return source_path

    sanitized_title = sanitize_pdf_title(title)
    if not sanitized_title:
        return source_path

    desired_path = os.path.join(FILE_DIR, f"{sanitized_title}.pdf")
    if os.path.abspath(source_path) == os.path.abspath(desired_path):
        return source_path
    if os.path.exists(desired_path):
        return desired_path

    try:
        os.rename(source_path, desired_path)
        return desired_path
    except OSError as e:
        if is_filename_too_long_error(e):
            log("[⚠️ JM]", f"文件名过长，降级命名: {e}", "warning")
        else:
            log("[⚠️ JM]", f"重命名失败: {e}", "warning")
            return source_path

    fallback_path = os.path.join(FILE_DIR, f"JM{number}.pdf")
    if os.path.abspath(source_path) == os.path.abspath(fallback_path):
        return source_path
    if os.path.exists(fallback_path):
        return fallback_path

    try:
        os.rename(source_path, fallback_path)
        return fallback_path
    except OSError as e:
        log("[⚠️ JM]", f"重命名失败: {e}", "warning")
        return source_path

def increment_pending(scope_key: str, delta: int = 1):
    pending_counts[scope_key] = pending_counts.get(scope_key, 0) + delta
    if pending_counts[scope_key] <= 0:
        pending_counts.pop(scope_key, None)

def should_redirect(scope_key: str) -> bool:
    return scope_key in redirect_scopes

def stage_file_for_forward(file_path: str) -> tuple[str | None, str | None, str | None]:
    safe_path, cleanup_local = prepare_file_for_scp(file_path, force_safe=False)
    try:
        remote_file_path = scp_file_to_remote(safe_path)
        if not remote_file_path:
            return None, None, None
    finally:
        if cleanup_local:
            try:
                os.remove(safe_path)
            except Exception:
                pass

    file_name = os.path.basename(remote_file_path)
    docker_file_path = os.path.join(DOCKER_INTERNAL_PATH, file_name)
    file_url = f"file://{docker_file_path}"
    return remote_file_path, file_url, file_name

def build_forward_node(file_url: str, display_name: str, uploader_id: int | str | None, uploader_name: str | None):
    try:
        user_id = int(uploader_id) if uploader_id is not None else 10000
    except (ValueError, TypeError):
        user_id = 10000
    nickname = uploader_name or "文件助手"
    return {
        "type": "node",
        "data": {
            "user_id": user_id,
            "nickname": nickname,
            "content": [{
                "type": "file",
                "data": {
                    "file": file_url,
                    "name": display_name,
                }
            }]
        }
    }

def build_forward_text_node(text: str, uploader_id: int | str | None, uploader_name: str | None):
    try:
        user_id = int(uploader_id) if uploader_id is not None else 10000
    except (ValueError, TypeError):
        user_id = 10000
    nickname = uploader_name or "文件助手"
    return {
        "type": "node",
        "data": {
            "user_id": user_id,
            "nickname": nickname,
            "content": [{
                "type": "text",
                "data": {
                    "text": text,
                }
            }]
        }
    }

async def flush_forward_buffer(scope_key: str, force: bool = False):
    buffer = forward_buffers.get(scope_key)
    if not buffer:
        return

    items = buffer.get("items", [])
    message_type = buffer.get("message_type")
    group_id = buffer.get("group_id")
    user_id = buffer.get("user_id")

    while items and (force or len(items) >= FORWARD_BATCH_SIZE):
        batch_size = min(len(items), FORWARD_BATCH_SIZE)
        if batch_size <= 0:
            break
        batch = items[:batch_size]
        nodes = [item["node"] for item in batch]

        if message_type == "group":
            success = await bot.send_group_forward_message(group_id, nodes)
        else:
            success = await bot.send_private_forward_message(user_id, nodes)

        if not success:
            log("[❌ Forward]", f"合并转发失败，scope={scope_key}", "error")
            break

        for item in batch:
            remote_file_path = item.get("remote_file_path")
            if remote_file_path:
                delete_remote_file(remote_file_path)

        del items[:batch_size]

    if not items:
        forward_buffers.pop(scope_key, None)

async def append_forward_buffer(scope_key: str, message_type: str, group_id: int | None, user_id: int | None, node: dict, remote_file_path: str | None):
    if scope_key not in forward_buffers:
        forward_buffers[scope_key] = {
            "message_type": message_type,
            "group_id": group_id,
            "user_id": user_id,
            "items": [],
        }

    forward_buffers[scope_key]["items"].append({
        "node": node,
        "remote_file_path": remote_file_path,
    })

    await flush_forward_buffer(scope_key, force=False)

# ================ 信息发送类 (已升级支持 Token) ================
class NapcatWebSocketBot:
    def __init__(self, websocket_url, token=None):
        self.websocket_url = websocket_url
        # ✅ 构建鉴权请求头
        self.headers = {}
        if token:
            self.headers["Authorization"] = f"Bearer {token}"

    async def _recv_until_echo(self, websocket, expected_echo, timeout=10):
        end_time = time.monotonic() + timeout
        while True:
            remaining = end_time - time.monotonic()
            if remaining <= 0:
                return None
            try:
                resp = await asyncio.wait_for(websocket.recv(), timeout=remaining)
            except asyncio.TimeoutError:
                return None

            try:
                resp_data = json.loads(resp)
            except json.JSONDecodeError:
                continue

            if resp_data.get("echo") == expected_echo:
                return resp_data

    def _extract_message_id(self, resp_data):
        if not resp_data:
            return None
        data = resp_data.get("data")
        if isinstance(data, dict):
            return data.get("message_id")
        if isinstance(data, int):
            return data
        return resp_data.get("message_id")

    async def _send_message_payload(self, action, params, message, echo, timeout=10):
        payload = {
            "action": action,
            "params": {
                **params,
                "message": message,
            },
            "echo": echo,
        }
        try:
            async with websockets.connect(self.websocket_url, extra_headers=self.headers) as websocket:
                await websocket.send(json.dumps(payload))
                resp_data = await self._recv_until_echo(websocket, payload["echo"], timeout=timeout)
                if not resp_data:
                    log("[❌ message_sender]", f"发送消息失败: 未收到响应 action={action} params={params}", "error")
                    return None
                if resp_data.get("status") == "ok":
                    return self._extract_message_id(resp_data)
                log("[❌ message_sender]", f"发送消息失败 action={action} params={params} resp={resp_data}")
                return None
        except Exception as e:
            log("[❌ message_sender]", f"发送消息失败 action={action} params={params} error={e}")
            return None

    async def _send_forward_failure_notice(self, params, failed_count: int):
        text = f"⚠️ {failed_count} items failed to stage and were skipped in merged forwarding."
        if "group_id" in params:
            await self._send_message_payload(
                "send_group_msg",
                {"group_id": params["group_id"]},
                [{"type": "text", "data": {"text": text}}],
                f"group_forward_notice_{params['group_id']}_{int(time.time())}",
            )
        elif "user_id" in params:
            await self._send_message_payload(
                "send_private_msg",
                {"user_id": params["user_id"]},
                [{"type": "text", "data": {"text": text}}],
                f"private_forward_notice_{params['user_id']}_{int(time.time())}",
            )

    async def send_private_message(self, user_id, message):
        echo = f"private_text_{user_id}_{int(time.time())}"
        return await self._send_message_payload(
            "send_private_msg",
            {"user_id": user_id},
            [{"type": "text", "data": {"text": message}}],
            echo,
        )

    async def send_group_message(self, group_id, message):
        echo = f"group_text_{group_id}_{int(time.time())}"
        return await self._send_message_payload(
            "send_group_msg",
            {"group_id": group_id},
            [{"type": "text", "data": {"text": message}}],
            echo,
        )

    async def send_group_message_segments(self, group_id, segments, echo=None):
        if echo is None:
            echo = f"group_segments_{group_id}_{int(time.time() * 1000)}"
        return await self._send_message_payload(
            "send_group_msg",
            {"group_id": group_id},
            segments,
            echo,
            timeout=FILE_SEND_TIMEOUT_SECONDS,
        )

    def _is_rich_media_transfer_failed(self, resp_data):
        if not resp_data:
            return False
        if resp_data.get("retcode") == 1200:
            return True
        message = (resp_data.get("message") or resp_data.get("wording") or "").lower()
        return "rich media transfer failed" in message

    async def _send_file_payload(self, action, params, file_path, echo, force_safe=False):
        log("[📦 message_sender]", f"准备发送文件 action={action} params={params} {describe_file(file_path)}")
        safe_path, cleanup_local = prepare_file_for_scp(file_path, force_safe=force_safe)
        try:
            remote_file_path = scp_file_to_remote(safe_path)
            if not remote_file_path:
                log("[❌ message_sender]", f"文件上传失败，无法发送 action={action} params={params} {describe_file(safe_path)}")
                return False, None, None
        finally:
            if cleanup_local:
                try:
                    os.remove(safe_path)
                except Exception:
                    pass

        file_name = os.path.basename(remote_file_path)
        docker_file_path = os.path.join(DOCKER_INTERNAL_PATH, file_name)
        file_url = f"file://{docker_file_path}"

        payload = {
            "action": action,
            "params": {
                **params,
                "message": [{"type": "file", "data": {"file": file_url}}],
            },
            "echo": echo,
        }
        try:
            async with websockets.connect(self.websocket_url, extra_headers=self.headers) as websocket:
                await websocket.send(json.dumps(payload))
                resp_data = await self._recv_until_echo(websocket, payload["echo"], timeout=FILE_SEND_TIMEOUT_SECONDS)

                delete_remote_file(remote_file_path)

                if not resp_data:
                    log("[❌ message_sender]", f"发送文件失败: 未收到响应 action={action} params={params} file_url={file_url}", "error")
                    return False, None, None
                if resp_data.get("status") == "ok":
                    return True, resp_data, self._extract_message_id(resp_data)
                log("[❌ message_sender]", f"发送文件失败 action={action} params={params} file_url={file_url} resp={resp_data}")
                return False, resp_data, None
        except Exception as e:
            log("[❌ message_sender]", f"发送文件失败 action={action} params={params} file_path={file_path} error={e}")
            delete_remote_file(remote_file_path)
            return False, None, None

    async def send_private_file(self, user_id, file_path):
        success, resp_data, _ = await self._send_file_payload(
            "send_private_msg",
            {"user_id": user_id},
            file_path,
            f"private_file_{user_id}",
            force_safe=False,
        )

        if not success and self._is_rich_media_transfer_failed(resp_data):
            log("[⚠️ message_sender]", "富媒体发送失败，尝试使用安全文件名重试", "warning")
            success, resp_data, _ = await self._send_file_payload(
                "send_private_msg",
                {"user_id": user_id},
                file_path,
                f"private_file_{user_id}_safe",
                force_safe=True,
            )

        if success:
            log("[✅ message_sender]", "私聊本子发送成功")
            return True

        return False

    async def send_group_file(self, group_id, file_path):
        success, resp_data, _ = await self._send_file_payload(
            "send_group_msg",
            {"group_id": group_id},
            file_path,
            f"group_file_{group_id}",
            force_safe=False,
        )

        if not success and self._is_rich_media_transfer_failed(resp_data):
            log("[⚠️ message_sender]", "富媒体发送失败，尝试使用安全文件名重试", "warning")
            success, resp_data, _ = await self._send_file_payload(
                "send_group_msg",
                {"group_id": group_id},
                file_path,
                f"group_file_{group_id}_safe",
                force_safe=True,
            )

        if success:
            log("[✅ message_sender]", "群聊本子发送成功")
            return True

        return False

    async def _stage_nodes_for_forward(self, nodes, staging_group_id):
        message_ids = []
        failed_count = 0
        for idx, node in enumerate(nodes):
            content = node.get("data", {}).get("content")
            if not content:
                log("[❌ message_sender]", "合并转发预发送失败: 节点内容为空", "error")
                failed_count += 1
                continue
            echo = f"stage_{staging_group_id}_{int(time.time() * 1000)}_{idx}"
            message_id = await self.send_group_message_segments(staging_group_id, content, echo=echo)
            if not message_id:
                log("[❌ message_sender]", "合并转发预发送失败: 未获取 message_id", "error")
                failed_count += 1
                continue
            message_ids.append(message_id)
        return message_ids, failed_count

    async def _send_forward_payload(self, action, params, nodes, echo):
        if len(nodes) > FORWARD_BATCH_SIZE:
            nodes = nodes[:FORWARD_BATCH_SIZE]

        message_ids, failed_count = await self._stage_nodes_for_forward(nodes, REDIRECT_GROUP_ID)
        if failed_count > 0:
            await self._send_forward_failure_notice(params, failed_count)

        if not message_ids:
            log("[❌ message_sender]", "合并转发失败: 预发送未完成", "error")
            return False

        forward_nodes = [{"type": "node", "data": {"id": message_id}} for message_id in message_ids]

        payload = {
            "action": action,
            "params": {
                **params,
                "message": forward_nodes,
                "news": [{"text": f"{len(forward_nodes)}个文件"}],
                "prompt": "本子合集",
                "summary": f"本子合集（共{len(forward_nodes)}个文件）",
                "source": "文件收集助手",
            },
            "echo": echo,
        }
        try:
            async with websockets.connect(self.websocket_url, extra_headers=self.headers) as websocket:
                await websocket.send(json.dumps(payload))
                resp_data = await self._recv_until_echo(websocket, payload["echo"])
                if not resp_data:
                    log("[❌ message_sender]", "合并转发失败: 未收到响应", "error")
                    return False
                if resp_data.get("status") == "ok":
                    return True
                log("[❌ message_sender]", f"合并转发失败: {resp_data}")
                return False
        except Exception as e:
            log("[❌ message_sender]", f"合并转发失败: {e}")
            return False

    async def send_group_forward_message(self, group_id, nodes):
        return await self._send_forward_payload(
            "send_group_forward_msg",
            {"group_id": group_id},
            nodes,
            f"group_forward_{group_id}_{int(time.time())}",
        )

    async def send_private_forward_message(self, user_id, nodes):
        return await self._send_forward_payload(
            "send_private_forward_msg",
            {"user_id": user_id},
            nodes,
            f"private_forward_{user_id}_{int(time.time())}",
        )

# ====================== 全局状态管理 (传入 Token) ======================
bot = NapcatWebSocketBot(WEBSOCKET_URL, token=WEBSOCKET_TOKEN)
client = jmcomic.JmOption.default().new_jm_client()
max_episodes = 20
jm_functioning = True
jm_is_running = False
jm_task_queue: asyncio.Queue = asyncio.Queue()

def get_jm_condition(group_id: int):
    return str(group_id) not in banned_group

def set_jm_condition(condition):
    global jm_functioning
    jm_functioning = condition

def get_jm_running():
    return jm_is_running

def set_jm_running(condition):
    global jm_is_running
    jm_is_running = condition

def set_download_max_epiosdes(num):
    global max_episodes
    max_episodes = num

def get_download_max_epiosdes():
    return max_episodes

# ====================== 下载逻辑 (保持不变) ======================
def jm_download_worker(number, result_dict):
    try:
        log("[🟢 JM]", f"开始下载本子: {number}")
        option = jmcomic.create_option_by_file('./option.yml')
        jmcomic.download_album(number, option)
        result_dict["result"] = True
        log("[📦 JM]", f"本子 {number} 下载完成")
    except Exception as e:
        log("[❌ JM]", f"下载失败: {e}")
        result_dict["result"] = False

def jm_download(number):
    manager = multiprocessing.Manager()
    result_dict = manager.dict()
    p = multiprocessing.Process(target=jm_download_worker, args=(number, result_dict))
    p.start()

    timeout = 1800
    start_time = time.time()
    
    while p.is_alive():
        time.sleep(3)
        main_mem, child_mem = get_total_memory_mb()
        log("[⬇️ DOWNLOADER]", f"下载期间检测内存，主: {main_mem:.2f} MB，子: {child_mem:.2f} MB")
        if time.time() - start_time > timeout:
            log("[⚠️ JM]", "下载超时，终止进程")
            p.terminate()
            break
    p.join()

    success = result_dict.get("result", False)
    del manager, result_dict
    gc.collect()
    return success

def find_file_by_number(number, title=None):
    candidates = []
    if number is not None:
        num_str = str(number)
        candidates.extend([f"{num_str}.pdf", f"JM{num_str}.pdf"])
    if title:
        safe_title = sanitize_pdf_title(title)
        candidates.append(f"{safe_title}.pdf")

    for file_name in candidates:
        file_path = os.path.join(FILE_DIR, file_name)
        if os.path.exists(file_path):
            return file_path, file_name

    if number is not None and os.path.isdir(FILE_DIR):
        num_str = str(number)
        for file_name in os.listdir(FILE_DIR):
            if file_name.lower().endswith(".pdf") and num_str in file_name:
                file_path = os.path.join(FILE_DIR, file_name)
                if os.path.exists(file_path):
                    return file_path, file_name

    return None, None

# ====================== 搜索逻辑 ======================
def similarity_score(query: str, title: str) -> float:
    return SequenceMatcher(None, query.lower(), title.lower()).ratio()

def search_jm_impl(keyword: str):
    try:
        # Step 1: Search by keyword
        page = client.search_site(search_query=keyword, page=1)
        results = []
        for album_id, title in page.iter_id_title():
            jm_id = JmcomicText.parse_to_jm_id(album_id)
            score = similarity_score(keyword, title)
            results.append((jm_id, title, score))
        
        if not results:
            return None
            
        # Sort by score desc
        results.sort(key=lambda x: -x[2])
        best_id, best_title, best_score = results[0]
        
        # Step 2: Get details
        # Using search_site with ID to get full album info including tags
        detail_page = client.search_site(search_query=str(best_id))
        return getattr(detail_page, "single_album", None)
        
    except Exception as e:
        log("[❌ Search]", f"搜索失败: {e}", "error")
        return None

# ====================== 主要命令处理 ======================
async def send_redirected_file(scope_key, message_type, group_id, user_id, file_path, uploader_id, uploader_name, info_message):
    log(
        "[🔁 Redirect]",
        f"开始重定向 scope={scope_key} target_group={REDIRECT_GROUP_ID} source_group={group_id} user_id={user_id} {describe_file(file_path)}",
    )

    info_sent = await bot.send_group_message(REDIRECT_GROUP_ID, info_message)
    if not info_sent:
        log(
            "[⚠️ Redirect]",
            f"发送本子信息到重定向群失败 target_group={REDIRECT_GROUP_ID} scope={scope_key}",
            "warning",
        )
    else:
        log("[✅ Redirect]", f"信息已发送到重定向群 target_group={REDIRECT_GROUP_ID} message_id={info_sent}")

    send_to_redirect = await bot.send_group_file(REDIRECT_GROUP_ID, file_path)
    if not send_to_redirect:
        log(
            "[⚠️ Redirect]",
            f"发送文件到重定向群失败 target_group={REDIRECT_GROUP_ID} scope={scope_key} {describe_file(file_path)}",
            "warning",
        )
    else:
        log("[✅ Redirect]", f"文件已发送到重定向群 target_group={REDIRECT_GROUP_ID}")

    remote_file_path, file_url, display_name = stage_file_for_forward(file_path)
    if not remote_file_path or not file_url:
        log(
            "[❌ Redirect]",
            f"转发文件准备失败 scope={scope_key} target_group={REDIRECT_GROUP_ID} {describe_file(file_path)}",
            "error",
        )
        return False

    log("[🔁 Redirect]", f"转发节点准备完成 scope={scope_key} remote={remote_file_path} url={file_url}")

    info_node = build_forward_text_node(info_message, uploader_id, uploader_name)
    file_node = build_forward_node(file_url, display_name, uploader_id, uploader_name)

    await append_forward_buffer(scope_key, message_type, group_id, user_id, info_node, None)
    await append_forward_buffer(scope_key, message_type, group_id, user_id, file_node, remote_file_path)
    return True

async def process_jm_command(number, message_type, group_id, user_id, scope_key, uploader_id, uploader_name, redirect_enabled):
    title = " "
    short_number = is_short_number(number)
    try:
        page = await asyncio.to_thread(client.search_site, search_query=str(number))
        album = getattr(page, "single_album", None)
        if not album:
            if short_number:
                return None
            log("[🚫 JM]", "未找到本子信息")
            return "❌ 未能成功下载（可能ID错误或网络失败）"
        title = album.title
        if not title:
            log("[🚫 JM]", "本子标题为空，无法下载")
            return "❌ 本子标题为空"
        if len(album.episode_list) > get_download_max_epiosdes():
            log("[🚫 JM]", "本子章节太多，不支持下载")
            return f"❌ 本子章节过多(>{get_download_max_epiosdes()})"

        file_path, file_name = find_file_by_number(number, title)
        if file_path:
            log("[✅ JM]", f"本地已存在该本子{number}：{file_name}")
            if not redirect_enabled:
                await send_message(message_type, group_id, user_id, f"📘 本地已存在本子 {number}")
            success = True
        else:
            if not redirect_enabled:
                await send_message(message_type, group_id, user_id, f"⏳ 正在下载本子 {number}")
            success = await asyncio.to_thread(jm_download, number)
    except Exception as e:
        log("[❌ JM]", f"本子 {number} 下载失败 {e}")
        if short_number:
            return None
        return "❌ 未能成功下载（可能ID错误或网络失败）"

    if success:
        renamed_path = rename_pdf_after_download(number, title)
        if renamed_path:
            file_path = renamed_path
            file_name = os.path.basename(renamed_path)
        else:
            file_path, file_name = find_file_by_number(number, title)

        if not file_path:
            log("[❌ JM]", f"下载本子{number}：{file_name}完成，但未找到PDF文件")
            return "❌ 下载完成但未找到PDF文件"

        send_mode = get_send_mode(group_id if message_type == "group" else None)
        enc_enabled = get_enc_enabled(group_id if message_type == "group" else None)
        random_password_enabled = get_random_password_enabled(group_id if message_type == "group" else None)
        send_name_mode = get_send_name_mode(group_id if message_type == "group" else None)
        password = ""

        temp_files = []
        temp_dirs = []
        send_path = file_path
        base_name = os.path.splitext(os.path.basename(file_path))[0]
        zip_base_name = base_name

        if enc_enabled:
            if random_password_enabled:
                password = generate_random_password()
            else:
                password = get_enc_password(group_id if message_type == "group" else None)

            if not password:
                return "❌ 未设置加密密码，请先使用 /jm passwd <密码> 设置"
            enc_path = build_encrypted_pdf(file_path, password)
            if not enc_path:
                return "❌ 文件加密失败"
            temp_files.append(enc_path)
            send_path = enc_path
            safe_password = sanitize_filename_component(password) or "password"
            zip_base_name = f"{base_name}_{safe_password}"

        if send_mode == "zip":
            zip_path = build_zip_for_file(send_path, zip_base_name)
            if not zip_path:
                return "❌ 文件压缩失败"
            temp_files.append(zip_path)
            send_path = zip_path

        send_base_name = f"JM{number}" if send_name_mode == "jm" else sanitize_pdf_title(title) or f"JM{number}"
        send_path_for_delivery, cleanup_send_copy = prepare_send_file_with_name(send_path, send_base_name)
        if cleanup_send_copy:
            temp_files.append(send_path_for_delivery)

        randomized_path, cleanup_randomized, temp_dir = randomize_file_hash(send_path_for_delivery)
        if cleanup_randomized:
            temp_files.append(randomized_path)
        if temp_dir:
            temp_dirs.append(temp_dir)

        send_path_for_delivery = randomized_path

        file_size = os.path.getsize(send_path_for_delivery) / (1024 * 1024)
        file_label = "ZIP" if send_path_for_delivery.endswith(".zip") else "PDF"
        msg = f"✅ 天堂正在发送：\n车牌号：{number}\n本子名：{title}\n文件类型：{file_label}\n文件大小：({file_size:.2f}MB)"
        if enc_enabled and password:
            msg += f"\n密码：{password}"

        if redirect_enabled:
            send_result = await send_redirected_file(
                scope_key,
                message_type,
                group_id,
                user_id,
                send_path_for_delivery,
                uploader_id,
                uploader_name,
                msg
            )
        else:
            if message_type == "group":
                send_result = await bot.send_group_file(group_id, send_path_for_delivery)
            else:
                send_result = await bot.send_private_file(user_id, send_path_for_delivery)

        for temp_file in temp_files:
            try:
                os.remove(temp_file)
            except Exception:
                pass

        for temp_dir in temp_dirs:
            try:
                os.rmdir(temp_dir)
            except Exception:
                pass

        if send_result:
            log("[✅ JM]", f"本子 {number} 处理完成并发送完成")
        return None if redirect_enabled else msg
    else:
        log("[❌ DOWNLOADER]", "下载失败或超时")
        return "❌ 下载失败或超时"

async def look_jm_information(number):
    try:
        log("[⭕ JM]", f"正在检索本子{number}信息")
        page = await asyncio.to_thread(client.search_site, search_query=str(number))
        album = page.single_album
        log("[🟢 JM]", f"本子{number}信息检索成功")
        return (
            f"🆔ID：{number}\n"
            f"⭕标题：{album.title}\n"
            f"💬描述：{album.description}\n"
            f"👥角色：{album.actors}\n"
            f"🏷标签：{album.tags}\n"
            f"⚛章节：{len(album.episode_list)}\n"
            f"👁浏览：{album.views}"
        )
    except Exception:
        log("[❌ JM]", f"本子{number}信息检索失败")
        return "❌ 查询失败"

# ====================== HTTP事件接收 ======================
@app.post("/")
async def root(request: Request):
    try:
        data = await request.json()
        asyncio.create_task(handle_message_event(data))
        return {"status": "success"}
    except Exception as e:
        log("[❌ System]", f"请求处理出错: {e}")
        return {"status": "error", "message": str(e)}

# ================== 消息处理 ==================
async def send_message(message_type, group_id, user_id, message):
    if message_type == "group" and group_id:
        await bot.send_group_message(group_id, message)
    elif message_type == "private" and user_id:
        await bot.send_private_message(user_id, message)

def requester_information(message_type, group_name, nickname, group_id, user_id, number, request_type):
    number = str(number)
    user_id = str(user_id)
    msg = ""
    tag = "[🔴 Request]" if (number in banned_id or user_id in banned_user) else "[🟢 Request]"
    
    if message_type == 'group':
        msg += f"{group_name}群({group_id})中"
        user_str = f"被封禁的用户 {nickname}({user_id})" if user_id in banned_user else f"{nickname}({user_id})"
        msg += user_str
        type_str = f"请求被封禁的{request_type}本子：{number}" if number in banned_id else f"请求本子：{number}"
        msg += type_str
    elif message_type == 'private':
        msg += f"私聊中"
        user_str = f"被封禁的用户 {nickname}({user_id})" if user_id in banned_user else f"{nickname}({user_id})"
        msg += user_str
        type_str = f"请求被封禁的{request_type}本子：{number}" if number in banned_id else f"请求本子：{number}"
        msg += type_str

    log(tag, msg)

def get_help_message():
    return (
        "📖 使用说明：\n"
        "1) /jm <ID>：下载并发送本子\n"
        "2) /jm look <ID>：查看本子信息\n"
        "3) /jm search <本子名>：搜索本子并下载（需确认）\n"
        "4) /jm goodluck | /goodluck | 随机本子：随机本子下载\n"
        "5) /jm mode pdf|zip：设置发送格式（群聊设置群专用，私聊设置全局）\n"
        "6) /jm enc on|off：设置是否加密（群聊设置群专用，私聊设置全局）\n"
        "7) /jm passwd <密码>：设置加密密码（群聊设置群专用，私聊设置全局）\n"
        "8) /jm randpwd on|off：启用随机密码加密（群聊设置群专用，私聊设置全局）\n"
        "9) /jm fname jm|full：设置发送文件命名方式（群聊设置群专用，私聊设置全局）\n"
        "10) /jm regex on|off：设置正则模式（群聊设置群专用，私聊设置全局）\n"
        "11) /jm help：查看帮助\n\n"
        "🔧 管理命令（仅管理员）：\n"
        "• /jm on：开启禁漫功能\n"
        "• /jm off：关闭禁漫功能\n"
        "• /jm addban <ID>：封禁本子\n"
        "• /jm delban <ID>：解封本子\n"
        "• /jm setmax <num>：设置最大章节数\n"
    )

async def enqueue_downloads(numbers, message_type, group_id, user_id, data):
    if message_type == "group" and str(group_id) in banned_group:
        await send_message(message_type, group_id, user_id, "❌ 禁漫功能未开启")
        return

    if str(user_id) in banned_user:
        await send_message(message_type, group_id, user_id, "❌ 禁止下载或用户被封禁")
        return

    scope_key = get_request_scope(message_type, group_id, user_id)
    is_batch = len(numbers) > 1
    queued_count = 0

    current_pending = pending_counts.get(scope_key, 0)
    if current_pending + len(numbers) > REDIRECT_THRESHOLD:
        if scope_key not in redirect_scopes:
            redirect_scopes.add(scope_key)
            log("[🔁 Redirect]", f"触发重定向 scope={scope_key} target_group={REDIRECT_GROUP_ID}")

    for number in numbers:
        requester_information(
            message_type,
            data.get('group_name'),
            data.get('sender').get('nickname') if data.get('sender') else None,
            group_id,
            user_id,
            number,
            "下载"
        )
        if str(number) in banned_id:
            await send_message(message_type, group_id, user_id, "❌ 禁止下载或用户被封禁")
            continue

        if is_recent_request(scope_key, number):
            if not is_short_number(number):
                await send_message(
                    message_type,
                    group_id,
                    user_id,
                    f"⏳ 本子 {number} 在过去12小时内已请求过，已跳过"
                )
            continue

        await jm_task_queue.put({
            "number": number,
            "message_type": message_type,
            "group_id": group_id,
            "user_id": user_id,
            "scope_key": scope_key,
            "uploader_id": user_id,
            "uploader_name": data.get("sender", {}).get("nickname") if data.get("sender") else None,
        })
        mark_request(scope_key, number)
        increment_pending(scope_key, 1)
        queued_count += 1

        if pending_counts.get(scope_key, 0) > REDIRECT_THRESHOLD and scope_key not in redirect_scopes:
            redirect_scopes.add(scope_key)
            log("[🔁 Redirect]", f"触发重定向 scope={scope_key} target_group={REDIRECT_GROUP_ID}")

        if not is_batch:
            queue_size = jm_task_queue.qsize()
            await send_message(
                message_type,
                group_id,
                user_id,
                f"✅ 本子 {number} 已加入队列，当前队列：{queue_size}"
            )

    if is_batch and queued_count > 0:
        queue_size = jm_task_queue.qsize()
        await send_message(
            message_type,
            group_id,
            user_id,
            f"✅ 已加入队列，正在下载 {queued_count} 个本子，当前队列：{queue_size}"
        )

async def jm_task_worker():
    while True:
        task = await jm_task_queue.get()
        scope_key = task.get("scope_key")
        try:
            set_jm_running(True)
            redirect_enabled = should_redirect(scope_key) if scope_key else False
            response = await process_jm_command(
                task["number"],
                task["message_type"],
                task["group_id"],
                task["user_id"],
                scope_key,
                task.get("uploader_id"),
                task.get("uploader_name"),
                redirect_enabled
            )
            if response is not None:
                await send_message(task["message_type"], task["group_id"], task["user_id"], response)
        except Exception as e:
            log("[❌ JM]", f"队列任务处理失败: {e}", "error")
        finally:
            jm_task_queue.task_done()
            if scope_key:
                increment_pending(scope_key, -1)
                await flush_forward_buffer(scope_key, force=pending_counts.get(scope_key, 0) == 0)
                if pending_counts.get(scope_key, 0) == 0 and scope_key in redirect_scopes:
                    redirect_scopes.discard(scope_key)
                    log("[🔁 Redirect]", f"重定向结束 scope={scope_key}")
            if jm_task_queue.empty():
                set_jm_running(False)

# ====================== 消息事件处理 (保持不变) ======================
async def handle_message_event(data):
    post_type = data.get("post_type")
    if post_type != "message":
        return

    message_type = data.get("message_type")
    raw_message = data.get("raw_message", "").strip()
    user_id = data.get("user_id")
    group_id = data.get("group_id")

    match_HELP = re.match(r"^/jm\s+help$", raw_message)
    match_MODE = re.match(r"^/jm\s+mode\s+(pdf|zip)$", raw_message)
    match_ENC = re.match(r"^/jm\s+enc\s+(on|off)$", raw_message)
    match_RANDPWD = re.match(r"^/jm\s+randpwd\s+(on|off)$", raw_message)
    match_FNAME = re.match(r"^/jm\s+fname\s+(jm|full)$", raw_message)
    match_REGEX = re.match(r"^/jm\s+regex\s+(on|off)$", raw_message)
    match_PASSWD = re.match(r"^/jm\s+passwd\s+(.+)$", raw_message)
    match_ON = re.match(r"^/jm\s+on$", raw_message)
    match_OFF = re.match(r"^/jm\s+off$", raw_message)
    match_ADDBAN = re.match(r"^/jm\s+addban\s+(\d+)$", raw_message)
    match_DELBAN = re.match(r"^/jm\s+delban\s+(\d+)$", raw_message)
    match_MDE = re.match(r"^/jm\s+setmax\s+(\d+)$", raw_message)
    match_JML = re.match(r"^/jm\s+look\s+(\d+)$", raw_message)
    match_SEARCH = re.match(r"^/jm\s+search\s+(.+)$", raw_message, flags=re.DOTALL)
    match_GOODLUCK = re.match(r"^/jm\s+goodluck$", raw_message)
    match_GOODLUCK_ALT = re.match(r"^/goodluck$", raw_message)
    match_GOODLUCK_CN = re.match(r"^随机本子$", raw_message)

    if match_HELP:
        await send_message(message_type, group_id, user_id, get_help_message())
        return

    if match_MODE:
        if user_id != admin_id:
            await send_message(message_type, group_id, user_id, "❌ 仅管理员可设置发送格式")
            return
        mode = match_MODE.group(1)
        if message_type == "group" and group_id:
            set_group_send_mode(group_id, mode)
            await send_message(message_type, group_id, user_id, f"✅ 本群发送格式已设置为：{mode.upper()}")
        else:
            set_global_send_mode(mode)
            await send_message(message_type, group_id, user_id, f"✅ 全局发送格式已设置为：{mode.upper()}")
        return

    if match_ENC:
        if user_id != admin_id:
            await send_message(message_type, group_id, user_id, "❌ 仅管理员可设置加密开关")
            return
        enabled = match_ENC.group(1) == "on"
        if message_type == "group" and group_id:
            set_group_enc_enabled(group_id, enabled)
            state = "开启" if enabled else "关闭"
            await send_message(message_type, group_id, user_id, f"✅ 本群加密已{state}")
        else:
            set_global_enc_enabled(enabled)
            state = "开启" if enabled else "关闭"
            await send_message(message_type, group_id, user_id, f"✅ 全局加密已{state}")
        return

    if match_RANDPWD:
        if user_id != admin_id:
            await send_message(message_type, group_id, user_id, "❌ 仅管理员可设置随机密码开关")
            return
        enabled = match_RANDPWD.group(1) == "on"
        if message_type == "group" and group_id:
            set_group_random_password_enabled(group_id, enabled)
            state = "开启" if enabled else "关闭"
            await send_message(message_type, group_id, user_id, f"✅ 本群随机密码已{state}")
        else:
            set_global_random_password_enabled(enabled)
            state = "开启" if enabled else "关闭"
            await send_message(message_type, group_id, user_id, f"✅ 全局随机密码已{state}")
        return

    if match_FNAME:
        if user_id != admin_id:
            await send_message(message_type, group_id, user_id, "❌ 仅管理员可设置发送文件命名方式")
            return
        mode = match_FNAME.group(1)
        if message_type == "group" and group_id:
            set_group_send_name_mode(group_id, mode)
            label = "JM号" if mode == "jm" else "全名"
            await send_message(message_type, group_id, user_id, f"✅ 本群发送文件命名已设置为：{label}")
        else:
            set_global_send_name_mode(mode)
            label = "JM号" if mode == "jm" else "全名"
            await send_message(message_type, group_id, user_id, f"✅ 全局发送文件命名已设置为：{label}")
        return

    if match_REGEX:
        if user_id != admin_id:
            await send_message(message_type, group_id, user_id, "❌ 仅管理员可设置正则模式")
            return
        enabled = match_REGEX.group(1) == "on"
        if message_type == "group" and group_id:
            set_group_regex_enabled(group_id, enabled)
            state = "开启" if enabled else "关闭"
            await send_message(message_type, group_id, user_id, f"✅ 本群正则模式已{state}")
        else:
            set_global_regex_enabled(enabled)
            state = "开启" if enabled else "关闭"
            await send_message(message_type, group_id, user_id, f"✅ 全局正则模式已{state}")
        return

    if match_PASSWD:
        if user_id != admin_id:
            await send_message(message_type, group_id, user_id, "❌ 仅管理员可设置加密密码")
            return
        password = match_PASSWD.group(1).strip()
        if not password:
            await send_message(message_type, group_id, user_id, "❌ 密码不能为空")
            return
        if message_type == "group" and group_id:
            set_group_enc_password(group_id, password)
            await send_message(message_type, group_id, user_id, "✅ 本群加密密码已设置")
        else:
            set_global_enc_password(password)
            await send_message(message_type, group_id, user_id, "✅ 全局加密密码已设置")
        return

    if match_GOODLUCK or match_GOODLUCK_ALT or match_GOODLUCK_CN:
        jm_id = generate_random_jm_id()
        await send_message(message_type, group_id, user_id, f"🎲 随机本子ID：JM{jm_id}")
        await enqueue_downloads([jm_id], message_type, group_id, user_id, data)
        return

    if match_ON and user_id == admin_id:
        log("[🟢 Admin]", "✅ 开启禁漫功能")
        await send_message(message_type, group_id, user_id, "✅ 禁漫功能已开启")
        if str(group_id) in banned_group:
            banned_group.remove(str(group_id))
            _config["banned_group"] = banned_group
            update_config()
        return
    if match_OFF and user_id == admin_id:
        log("[🟢 Admin]", "🚫 关闭禁漫功能")
        await send_message(message_type, group_id, user_id, "🚫 禁漫功能已关闭")
        if str(group_id) not in banned_group:
            banned_group.append(str(group_id))
            _config["banned_group"] = banned_group
            update_config()
        return
    if match_ADDBAN and user_id == admin_id:
        ban_id = match_ADDBAN.group(1)
        if ban_id not in banned_id:
            banned_id.append(ban_id)
            _config["banned_id"] = banned_id
            update_config()
            log("[🟢 Admin]", f"添加封禁本子：{ban_id}")
            await send_message(message_type, group_id, user_id, f"✅ 已封禁本子ID：{ban_id}")
        return
    if match_DELBAN and user_id == admin_id:
        ban_id = match_DELBAN.group(1)
        if ban_id in banned_id:
            banned_id.remove(ban_id)
            _config["banned_id"] = banned_id
            update_config()
            log("[🟢 Admin]", f"删除封禁本子：{ban_id}")
            await send_message(message_type, group_id, user_id, f"✅ 已解封本子ID：{ban_id}")
        return
    if match_MDE and user_id == admin_id:
        num = int(match_MDE.group(1))
        set_download_max_epiosdes(num)
        log("[🟢 Admin]", f"📘 章节数阈值已设为 {num}")
        await send_message(message_type, group_id, user_id, f"📘 章节数阈值已设为 {num}")
        return

    if match_JML:
        number = match_JML.group(1)
        requester_information(
            message_type,
            data.get('group_name'),
            data.get('sender').get('nickname'),
            group_id,
            user_id,
            number,
            "检索"
        )
        if str(group_id) not in banned_group:
            await send_message(message_type, group_id, user_id, f"🔍 正在检索本子 {number}")
            info = await look_jm_information(number)
            await send_message(message_type, group_id, user_id, info)
        else:
            await send_message(message_type, group_id, user_id, "❌ 禁漫功能未开启")
        return

    if match_SEARCH:
        keyword = match_SEARCH.group(1).strip()
        keyword = normalize_search_keyword(keyword)
        scope_key = get_request_scope(message_type, group_id, user_id)
        
        if str(group_id) in banned_group and message_type == "group":
            await send_message(message_type, group_id, user_id, "❌ 禁漫功能未开启")
            return

        await send_message(message_type, group_id, user_id, f"🔍 正在搜索：{keyword} ...")
        
        album = await asyncio.to_thread(search_jm_impl, keyword)
        if album:
            # Store pending
            search_pending[scope_key] = {
                "jm_id": album.album_id,
                "title": album.title,
                "time": time.time()
            }
            
            tags_str = ', '.join(album.tags) if hasattr(album, 'tags') else "无"
            msg = (
                f"✅ 找到最佳匹配：\n"
                f"🆔 JM{album.album_id}\n"
                f"📖 标题：{album.title}\n"
                f"🏷 标签：{tags_str}\n"
                f"❓ 是否下载？请在10分钟内回复“确认”"
            )
            await send_message(message_type, group_id, user_id, msg)
        else:
            await send_message(message_type, group_id, user_id, "❌ 未找到相关本子")
        return

    # Check confirmation
    if raw_message == "确认":
        scope_key = get_request_scope(message_type, group_id, user_id)
        pending = search_pending.get(scope_key)
        if pending:
            if time.time() - pending["time"] < 600: # 10 mins
                jm_id = pending["jm_id"]
                title = pending["title"]
                del search_pending[scope_key]
                await send_message(message_type, group_id, user_id, f"✅ 已确认，开始处理本子：{title}")
                await enqueue_downloads([jm_id], message_type, group_id, user_id, data)
            else:
                # Expired
                del search_pending[scope_key]
        return

    regex_enabled = get_regex_enabled(group_id if message_type == "group" else None)
    numbers = extract_jm_numbers_from_event(data, regex_enabled)
    if numbers:
        await enqueue_downloads(numbers, message_type, group_id, user_id, data)

# ====================== 内存管理任务 ======================
async def periodic_cleanup():
    while True:
        await asyncio.sleep(300)
        if hasattr(gc, "collect"):
            gc.collect()
        main_mem, child_mem = get_total_memory_mb()
        log("[🚀 SYSTEM]", f"内存检测 - 主: {main_mem:.2f} MB ，子: {child_mem:.2f} MB")

        if get_jm_running():
            continue

        if (main_mem + child_mem) > 600:
            log("[⚠️ SYSTEM]", "内存超限，准备重启")
            sys.exit(0)

# ====================== 主函数入口 ======================
async def main():
    log("[🚀 SYSTEM]", "Napcat QQ机器人启动中...")
    log("[📁 SYSTEM]", f"文件目录: {os.path.abspath(FILE_DIR)}")
    log("[🌐 SYSTEM]", f"WebSocket服务器: {WEBSOCKET_URL}")
    log("[🔗 SYSTEM]", f"HTTP监听端口: {HTTP_PORT}")
    log("[🌍 REMOTE]", f"SCP目标: {REMOTE_USER}@{REMOTE_HOST}:{REMOTE_TEMP_DIR}")
    
    # 打印版本信息，方便排查
    log("[📦 SYSTEM]", f"Websockets Version: {websockets.__version__}")
    
    asyncio.create_task(periodic_cleanup())
    asyncio.create_task(jm_task_worker())

    config = uvicorn.Config(app, host="0.0.0.0", port=HTTP_PORT, loop="asyncio", access_log=False)
    server = uvicorn.Server(config)
    await server.serve()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log("[🛑 SYSTEM]", "用户手动终止程序")
    except Exception as e:
        log("[❌ SYSTEM]", f"程序异常退出：{e}", "error")
        sys.exit(1)
