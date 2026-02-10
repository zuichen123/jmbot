# JMBot（NapCat + JMComic）

一个能在 QQ 里收消息、自动下载禁漫本子、再把文件发回去的机器人。

项目核心能力：

- 支持群聊/私聊触发下载
- 支持 `PDF` / `ZIP` 两种发送格式
- 支持 PDF 加密、随机密码、文件命名策略
- 支持搜索后确认下载
- 支持批量请求自动重定向 + 合并转发
- 支持请求去重、超时控制、内存守护重启

这份文档写成了保姆级，照着做基本不会翻车。

## 目录

- [1. 项目结构](#1-项目结构)
- [2. 运行原理（先看这个很省心）](#2-运行原理先看这个很省心)
- [3. 准备环境](#3-准备环境)
- [4. 安装依赖](#4-安装依赖)
- [5. 配置文件（重点）](#5-配置文件重点)
- [6. 启动方式](#6-启动方式)
- [7. 命令总览](#7-命令总览)
- [8. 常见工作流示例](#8-常见工作流示例)
- [9. 排错指南](#9-排错指南)
- [10. 生产部署建议](#10-生产部署建议)
- [11. 安全与合规提醒](#11-安全与合规提醒)
- [12. License](#12-license)

## 1. 项目结构

```text
jmbot/
├─ main.py                # 主程序：HTTP收事件 + 命令解析 + 下载队列 + 文件发送
├─ requirements.txt       # Python依赖
├─ config.example.yml     # 主配置模板（首次运行会复制为 config.yml）
├─ option.yml             # jmcomic 下载与转PDF配置
├─ .gitignore
└─ README.md
```

## 2. 运行原理（先看这个很省心）

整个流程就 8 步：

1. NapCat 把 QQ 消息事件 `POST` 到本项目 HTTP 服务。
2. `main.py` 解析消息内容（命令或提取数字 ID）。
3. 下载任务进入 `asyncio.Queue`，由 worker 串行消费。
4. 用子进程跑 `jmcomic.download_album` 下载，防止主进程被卡死。
5. 下载后按配置做处理：重命名、可选加密、可选 zip、可选 hash 扰动。
6. 文件通过 `scp` 或本地拷贝送到 NapCat 可访问目录。
7. 通过 WebSocket 调 NapCat action 发文件回群/私聊。
8. 清理临时文件，周期检查内存，超限后退出等待守护进程拉起。

一句话版本：**事件驱动 + 队列串行 + 子进程下载隔离 + WebSocket 发送**。

## 3. 准备环境

### 3.1 Python

- 推荐 `Python 3.10+`

### 3.2 NapCat

需要能提供：

- HTTP 事件上报（到本项目）
- WebSocket API（本项目连过去发消息）

### 3.3 网络与权限

- 机器人所在机器要能访问禁漫站点（或你的代理链路）
- 如果 `transfer_mode: scp`，本机要能免密 SSH 到远程 NapCat 宿主机

## 4. 安装依赖

```bash
pip install -r requirements.txt
```

## 5. 配置文件（重点）

首次运行会自动把 `config.example.yml` 复制成 `config.yml`。

### 5.1 `config.yml` 关键配置

下面按“必须先配”和“进阶可选”来写。

#### 必须先配（先跑起来）

- `admin_id`：管理员 QQ 号（只有他能改关键配置）
- `websocket_url`：NapCat WebSocket 地址
- `websocket_token`：NapCat 鉴权 token
- `http_host` / `http_port`：本服务监听地址
- `file_dir`：PDF 文件目录（默认 `./pdf/`）
- `transfer_mode`：`scp` 或 `local`

#### 传输相关（文件发不出去常看这里）

- `remote_user` / `remote_host`：`scp` 目标机器
- `remote_temp_dir`：目标机器临时目录（NapCat 能读到）
- `local_ssh_key`：本机私钥路径
- `docker_internal_path`：NapCat 容器内映射路径

#### 发送策略

- `send_mode_global`: `pdf` 或 `zip`
- `send_mode_group`: 某些群单独覆盖发送格式
- `send_name_mode_global`: `full`（标题全名）或 `jm`（JM+ID）
- `send_name_mode_group`: 群级命名覆盖

#### 加密策略

- `enc_enabled_global`: 全局是否启用 PDF 加密
- `enc_enabled_group`: 群级加密开关
- `enc_password_global`: 全局固定密码
- `enc_password_group`: 群级固定密码
- `random_password_enabled_global`: 是否每次随机密码
- `random_password_enabled_group`: 群级随机密码开关
- `random_password_length`: 随机密码长度

#### 稳定性与限流

- `download_timeout`: 单任务下载超时
- `search_timeout`: 搜索确认有效期
- `dedup_window_seconds`: 去重窗口（默认 12 小时）
- `memory_cleanup_interval`: 内存巡检间隔
- `memory_limit_mb`: 内存阈值，超过自动退出
- `redirect_threshold`: 同 scope 待处理数超过后开启重定向
- `forward_batch_size`: 合并转发批量节点大小
- `max_episodes`: 允许下载的最大章节数

#### 黑名单

- `banned_id`: 禁止下载的本子 ID
- `banned_user`: 禁止用户
- `banned_group`: 禁止群

### 5.2 `option.yml`（jmcomic）

这个文件控制下载和 PDF 生成：

- `dir_rule.base_dir`: 图片下载目录
- `download.threading.image`: 并发下载图片数
- `download.threading.photo`: 并发章节数
- `plugins.after_album.img2pdf`: 下载后转 PDF
- `filename_rule: Aid`: 用专辑 ID 命名，能避免超长文件名

默认这套就挺稳，别一上来把并发拉太高。

### 5.3 最小可用配置示例

```yaml
admin_id: 123456789
http_host: "0.0.0.0"
http_port: 8071
websocket_url: "ws://127.0.0.1:13001"
websocket_token: "your-token"

file_dir: ./pdf/
log_dir: ./logs

transfer_mode: local
remote_temp_dir: /tmp/
docker_internal_path: /app/.config/QQ/temp/

send_mode_global: pdf
send_name_mode_global: full

enc_enabled_global: false
random_password_enabled_global: false

dedup_window_seconds: 43200
download_timeout: 1800
search_timeout: 600
memory_limit_mb: 600
max_episodes: 20
```

## 6. 启动方式

```bash
python main.py
```

看到这些日志说明启动正常：

- `Napcat QQ机器人启动中...`
- `HTTP监听: ...`
- `WebSocket服务器: ...`

## 7. 命令总览

### 普通功能

- `/jm <ID>`：下载并发送
- `/jm look <ID>`：查看本子信息
- `/jm search <关键词>`：搜索最佳匹配，回复“确认”再下载
- `/jm goodluck`、`/goodluck`、`随机本子`：随机本子
- `/jm help`：查看帮助

### 配置类命令（管理员）

- `/jm mode pdf|zip`：发送格式
- `/jm enc on|off`：加密开关
- `/jm passwd <密码>`：设置加密密码
- `/jm randpwd on|off`：随机密码开关
- `/jm fname jm|full`：发送文件命名方式
- `/jm regex on|off`：提取模式（纯数字/`jm123456`）

### 管理命令（管理员）

- `/jm on`：开启群内禁漫功能
- `/jm off`：关闭群内禁漫功能
- `/jm addban <ID>`：封禁本子
- `/jm delban <ID>`：解封本子
- `/jm setmax <num>`：最大章节阈值

## 8. 常见工作流示例

### 8.1 普通下载

1. 群友发：`/jm 123456`
2. Bot 回：已加入队列
3. 下载完成后回：本子信息 + 文件

### 8.2 搜索确认下载

1. 发：`/jm search 某关键词`
2. Bot 回：最佳匹配 + 标签 + “10分钟内回复确认”
3. 回复：`确认`
4. 开始下载并发送

### 8.3 批量号牌触发重定向

1. 一次消息中带多个 ID
2. 待处理数超阈值后自动进入重定向模式
3. 文件先发中转群，再以合并转发推回原会话

## 9. 排错指南

### 9.1 收得到消息但发不出文件

- 检查 `transfer_mode` 是否和当前部署一致
- 检查 `remote_temp_dir` 与 `docker_internal_path` 映射是否一致
- 检查 SSH 密钥权限、远程目录写权限

### 9.2 提示富媒体发送失败

程序会自动回退到“安全文件名”再试一次。

- 还是失败就检查 NapCat 侧文件协议/路径权限
- 检查是否被风控或文件大小超限

### 9.3 下载超时

- 提高 `download_timeout`
- 降低 `option.yml` 中并发参数
- 检查网络连通性和代理链路

### 9.4 经常重启

- 看日志里是否内存超限触发
- 适当调高 `memory_limit_mb`
- 或降低并发、减少批量任务峰值

### 9.5 搜索后“确认”没反应

- `search_timeout` 过期了
- scope 不一致（群里搜要在同群确认，私聊搜要在私聊确认）

## 10. 生产部署建议

- 用 `systemd` / `supervisor` / Docker restart policy 做守护
- 日志目录单独挂载，定期清理
- SSH key 使用最小权限账号
- 不把 `config.yml` 里的敏感字段直接公开到仓库
- 先在测试群验证再上线生产群

## 11. 安全与合规提醒

- 请遵守所在地法律法规与平台服务条款
- 项目仅供技术学习与自动化实践
- 任何滥用、侵权行为与作者无关

## 12. License

本项目使用 MIT License，详见 `LICENSE`。

## 13. 作者

- GitHub: `zuichen123`
