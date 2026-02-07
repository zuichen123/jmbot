import argparse
import asyncio
import os
import tempfile
import time
import shutil

import main as app


def create_temp_files(count: int, directory: str) -> list[str]:
    files = []
    timestamp = int(time.time())
    for i in range(count):
        filename = f"forward_test_{timestamp}_{i + 1}.txt"
        path = os.path.join(directory, filename)
        with open(path, "w", encoding="utf-8") as f:
            f.write(f"Forward test file {i + 1} created at {timestamp}\n")
        files.append(path)
    return files


async def run_test(mode: str, group_id: int, user_id: int, file_count: int, text_count: int, uploader_id: int, uploader_name: str):
    total_nodes = file_count + text_count
    if total_nodes == 0:
        raise RuntimeError("No nodes to send. Increase --files or --texts.")
    if total_nodes > app.FORWARD_BATCH_SIZE:
        raise RuntimeError(f"Total nodes ({total_nodes}) exceed batch limit ({app.FORWARD_BATCH_SIZE}).")

    temp_dir = tempfile.mkdtemp(prefix="forward_test_")
    local_files = []
    remote_files = []

    try:
        local_files = create_temp_files(file_count, temp_dir)

        nodes = []
        for i in range(text_count):
            text = f"Forward test message {i + 1}"
            nodes.append(app.build_forward_text_node(text, uploader_id, uploader_name))

        for file_path in local_files:
            remote_file_path, file_url, display_name = app.stage_file_for_forward(file_path)
            if not remote_file_path or not file_url:
                raise RuntimeError(f"Failed to stage test file: {file_path}")

            nodes.append(app.build_forward_node(file_url, display_name, uploader_id, uploader_name))
            remote_files.append(remote_file_path)

        if mode == "group":
            success = await app.bot.send_group_forward_message(group_id, nodes)
        else:
            success = await app.bot.send_private_forward_message(user_id, nodes)

        if not success:
            raise RuntimeError("Failed to send forward message.")
    finally:
        for remote_file_path in remote_files:
            app.delete_remote_file(remote_file_path)

        for file_path in local_files:
            if os.path.exists(file_path):
                os.remove(file_path)

        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir, ignore_errors=True)


def parse_args():
    parser = argparse.ArgumentParser(description="Test merged forward messages with files and text.")
    parser.add_argument("--mode", choices=["group", "private"], default="group", help="Forward target type.")
    parser.add_argument("--group-id", type=int, default=app.REDIRECT_GROUP_ID, help="Target group ID for group mode.")
    parser.add_argument("--user-id", type=int, default=app.admin_id, help="Target user ID for private mode.")
    parser.add_argument("--files", type=int, default=3, help="Number of small files to create.")
    parser.add_argument("--texts", type=int, default=2, help="Number of text nodes to include.")
    parser.add_argument("--uploader-id", type=int, default=app.admin_id, help="Uploader user ID for nodes.")
    parser.add_argument("--uploader-name", type=str, default="ForwardTest", help="Uploader name for nodes.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(
        run_test(
            mode=args.mode,
            group_id=args.group_id,
            user_id=args.user_id,
            file_count=args.files,
            text_count=args.texts,
            uploader_id=args.uploader_id,
            uploader_name=args.uploader_name,
        )
    )
