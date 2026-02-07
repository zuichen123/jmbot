import argparse
import asyncio
import json
import logging
import os
import subprocess
import time
from typing import Any

import websockets
from websockets.exceptions import ConnectionClosed


DEFAULT_WS_URL = "ws://10.0.0.101:13001"
DEFAULT_TOKEN = "1"
DEFAULT_REDIRECT_GROUP_ID = 1083663846
DEFAULT_TARGET_GROUP_ID = 609976107
DEFAULT_REMOTE_USER = "zuichen"
DEFAULT_REMOTE_HOST = "10.0.0.101"
DEFAULT_REMOTE_TEMP_DIR = "/home/zuichen/Server/Napcat2/.config/QQ/temp/"
DEFAULT_DOCKER_TEMP_DIR = "/app/.config/QQ/temp/"
DEFAULT_SSH_KEY = "/home/zuichen/.ssh/id_rsa"


def build_logger():
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return logging.getLogger("forward_test")


logger = build_logger()


def get_auth_headers(token: str | None) -> dict[str, str]:
    if token:
        return {"Authorization": f"Bearer {token}"}
    return {}


async def recv_until_echo(websocket, expected_echo: str, timeout: int):
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


def extract_message_id(resp_data: dict[str, Any] | None):
    if not resp_data:
        return None
    data = resp_data.get("data")
    if isinstance(data, dict):
        return data.get("message_id")
    if isinstance(data, int):
        return data
    return resp_data.get("message_id")


def scp_file_to_remote(local_file_path: str, remote_user: str, remote_host: str, remote_temp_dir: str, ssh_key: str):
    if not os.path.exists(local_file_path):
        logger.error("Local file does not exist: %s", local_file_path)
        return None

    remote_file_path = os.path.join(remote_temp_dir, os.path.basename(local_file_path))
    scp_cmd = [
        "scp",
        "-i", ssh_key,
        "-o", "StrictHostKeyChecking=no",
        local_file_path,
        f"{remote_user}@{remote_host}:{remote_file_path}",
    ]

    try:
        subprocess.run(
            scp_cmd,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            encoding="utf-8",
        )
        logger.info("Uploaded file to remote: %s", remote_file_path)
        return remote_file_path
    except subprocess.CalledProcessError as e:
        logger.error("SCP upload failed: %s", e.stderr)
        return None


def delete_remote_file(remote_user: str, remote_host: str, remote_file_path: str, ssh_key: str):
    ssh_cmd = [
        "ssh",
        "-i", ssh_key,
        "-o", "StrictHostKeyChecking=no",
        f"{remote_user}@{remote_host}",
        f"rm -f '{remote_file_path}'",
    ]
    try:
        subprocess.run(
            ssh_cmd,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            encoding="utf-8",
        )
        logger.info("Deleted remote file: %s", remote_file_path)
        return True
    except subprocess.CalledProcessError as e:
        logger.error("Remote delete failed: %s", e.stderr)
        return False


async def send_group_msg(ws_url: str, token: str | None, group_id: int, message, echo: str, timeout: int, retries: int):
    payload = {
        "action": "send_group_msg",
        "params": {"group_id": group_id, "message": message},
        "echo": echo,
    }
    headers = get_auth_headers(token)

    last_error = None
    for attempt in range(1, retries + 1):
        try:
            async with websockets.connect(ws_url, extra_headers=headers) as websocket:
                await websocket.send(json.dumps(payload))
                resp_data = await recv_until_echo(websocket, echo, timeout)
                return resp_data
        except ConnectionClosed as e:
            last_error = e
            logger.error(
                "WebSocket closed during send_group_msg (attempt %s/%s): code=%s reason=%s",
                attempt,
                retries,
                getattr(e, "code", None),
                getattr(e, "reason", None),
            )
        except Exception as e:
            last_error = e
            logger.error("send_group_msg failed (attempt %s/%s): %s", attempt, retries, e)

        await asyncio.sleep(1)

    if last_error:
        raise last_error
    return None


async def send_group_forward_msg(ws_url: str, token: str | None, group_id: int, nodes, echo: str, timeout: int, retries: int):
    payload = {
        "action": "send_group_forward_msg",
        "params": {"group_id": group_id, "message": nodes},
        "echo": echo,
    }
    headers = get_auth_headers(token)

    last_error = None
    for attempt in range(1, retries + 1):
        try:
            async with websockets.connect(ws_url, extra_headers=headers) as websocket:
                await websocket.send(json.dumps(payload))
                resp_data = await recv_until_echo(websocket, echo, timeout)
                return resp_data
        except ConnectionClosed as e:
            last_error = e
            logger.error(
                "WebSocket closed during send_group_forward_msg (attempt %s/%s): code=%s reason=%s",
                attempt,
                retries,
                getattr(e, "code", None),
                getattr(e, "reason", None),
            )
        except Exception as e:
            last_error = e
            logger.error("send_group_forward_msg failed (attempt %s/%s): %s", attempt, retries, e)

        await asyncio.sleep(1)

    if last_error:
        raise last_error
    return None


async def run_test(args):
    if not args.token:
        logger.warning("No token provided. If NapCat requires auth, set --token or NAPCAT_WS_TOKEN.")

    staged_ids = []

    text_content = [{"type": "text", "data": {"text": args.text}}]
    text_echo = f"stage_text_{args.redirect_group_id}_{int(time.time() * 1000)}"
    text_resp = await send_group_msg(
        args.ws_url,
        args.token,
        args.redirect_group_id,
        text_content,
        text_echo,
        args.timeout,
        args.retries,
    )
    logger.info("Stage text response: %s", text_resp)
    text_id = extract_message_id(text_resp)
    if not text_id:
        logger.error("Failed to stage text message.")
        return
    staged_ids.append(text_id)

    remote_file_path = None
    if args.file_path:
        remote_file_path = scp_file_to_remote(
            args.file_path,
            args.remote_user,
            args.remote_host,
            args.remote_temp_dir,
            args.ssh_key,
        )
        if not remote_file_path:
            logger.error("File staging failed before sending.")
            return

        file_url = f"file://{os.path.join(args.docker_temp_dir, os.path.basename(remote_file_path))}"
        file_content = [{"type": "file", "data": {"file": file_url}}]
        file_echo = f"stage_file_{args.redirect_group_id}_{int(time.time() * 1000)}"
        file_resp = await send_group_msg(
            args.ws_url,
            args.token,
            args.redirect_group_id,
            file_content,
            file_echo,
            args.timeout,
            args.retries,
        )
        logger.info("Stage file response: %s", file_resp)
        file_id = extract_message_id(file_resp)
        if not file_id:
            logger.error("Failed to stage file message.")
            return
        staged_ids.append(file_id)

    forward_nodes = [{"type": "node", "data": {"id": message_id}} for message_id in staged_ids]
    forward_echo = f"forward_{args.target_group_id}_{int(time.time())}"
    forward_resp = await send_group_forward_msg(
        args.ws_url,
        args.token,
        args.target_group_id,
        forward_nodes,
        forward_echo,
        args.timeout,
        args.retries,
    )
    logger.info("Forward response: %s", forward_resp)

    if remote_file_path:
        delete_remote_file(args.remote_user, args.remote_host, remote_file_path, args.ssh_key)


def parse_args():
    parser = argparse.ArgumentParser(description="Test merged forward staging and forwarding.")
    parser.add_argument("--ws-url", default=os.getenv("NAPCAT_WS_URL", DEFAULT_WS_URL))
    parser.add_argument("--token", default=os.getenv("NAPCAT_WS_TOKEN", DEFAULT_TOKEN))
    parser.add_argument("--redirect-group-id", type=int, default=DEFAULT_REDIRECT_GROUP_ID)
    parser.add_argument("--target-group-id", type=int, default=DEFAULT_TARGET_GROUP_ID)
    parser.add_argument("--text", default="Forward test message")
    parser.add_argument("--file-path", default=None)
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--remote-user", default=os.getenv("NAPCAT_REMOTE_USER", DEFAULT_REMOTE_USER))
    parser.add_argument("--remote-host", default=os.getenv("NAPCAT_REMOTE_HOST", DEFAULT_REMOTE_HOST))
    parser.add_argument("--remote-temp-dir", default=os.getenv("NAPCAT_REMOTE_TEMP_DIR", DEFAULT_REMOTE_TEMP_DIR))
    parser.add_argument("--docker-temp-dir", default=os.getenv("NAPCAT_DOCKER_TEMP_DIR", DEFAULT_DOCKER_TEMP_DIR))
    parser.add_argument("--ssh-key", default=os.getenv("NAPCAT_SSH_KEY", DEFAULT_SSH_KEY))
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(run_test(args))
