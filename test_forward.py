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


async def run_test(group_id: int, file_count: int, text_count: int, uploader_id: int, uploader_name: str):
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
            safe_path, cleanup_local = app.prepare_file_for_scp(file_path, force_safe=False)
            try:
                remote_file_path = app.scp_file_to_remote(safe_path)
                if not remote_file_path:
                    raise RuntimeError(f"Failed to upload test file: {file_path}")

                file_name = os.path.basename(remote_file_path)
                docker_file_path = os.path.join(app.DOCKER_INTERNAL_PATH, file_name)
                file_url = f"file://{docker_file_path}"

                nodes.append(app.build_forward_node(file_url, file_name, uploader_id, uploader_name))
                remote_files.append(remote_file_path)
            finally:
                if cleanup_local and os.path.exists(safe_path):
                    os.remove(safe_path)

        if not nodes:
            raise RuntimeError("No nodes were generated for the forward message.")

        success = await app.bot.send_group_forward_message(group_id, nodes)
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
    parser.add_argument("--group-id", type=int, default=app.REDIRECT_GROUP_ID, help="Target group ID.")
    parser.add_argument("--files", type=int, default=3, help="Number of small files to create.")
    parser.add_argument("--texts", type=int, default=2, help="Number of text nodes to include.")
    parser.add_argument("--uploader-id", type=int, default=app.admin_id, help="Uploader user ID for nodes.")
    parser.add_argument("--uploader-name", type=str, default="ForwardTest", help="Uploader name for nodes.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(
        run_test(
            group_id=args.group_id,
            file_count=args.files,
            text_count=args.texts,
            uploader_id=args.uploader_id,
            uploader_name=args.uploader_name,
        )
    )
