#!/usr/bin/env bash
# Ubuntu 24.04 一键部署脚本（在虚拟环境中运行，使用 systemd 自启动）
# 用法：在服务器上进入项目根目录后执行：
#   sudo bash depoly.sh

set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
SERVICE_NAME="web_main"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
VENV_DIR="${PROJECT_DIR}/.venv"

echo "[+] 项目目录: ${PROJECT_DIR}"

# 需要 root 权限以写入 /etc/systemd/system
if [[ ${EUID} -ne 0 ]]; then
  echo "[!] 请使用 sudo 运行：sudo bash depoly.sh"
  exit 1
fi

# 推断部署用户（优先使用发起 sudo 的用户）
DEPLOY_USER="${SUDO_USER:-$(whoami)}"
echo "[+] 部署用户: ${DEPLOY_USER}"

echo "[+] 安装系统依赖 (python3, venv, pip)"
apt-get update -y
apt-get install -y python3 python3-venv python3-pip

echo "[+] 创建虚拟环境: ${VENV_DIR}"
if [[ ! -d "${VENV_DIR}" ]]; then
  python3 -m venv "${VENV_DIR}"
fi

echo "[+] 安装 Python 依赖 (requirements.txt)"
if [[ -f "${PROJECT_DIR}/requirements.txt" ]]; then
  "${VENV_DIR}/bin/pip" install --upgrade pip
  "${VENV_DIR}/bin/pip" install -r "${PROJECT_DIR}/requirements.txt"
else
  echo "[!] 未找到 requirements.txt，跳过依赖安装"
fi

echo "[+] 生成 systemd 单元文件: ${SERVICE_FILE}"
cat > "${SERVICE_FILE}" <<EOF
[Unit]
Description=EMA/MA 自动交易系统 WebMain
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${DEPLOY_USER}
WorkingDirectory=${PROJECT_DIR}
Environment=PYTHONUNBUFFERED=1
ExecStart=${VENV_DIR}/bin/python ${PROJECT_DIR}/web_main.py
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

echo "[+] 重新加载 systemd 守护进程"
systemctl daemon-reload

echo "[+] 设置开机自启并立即启动服务: ${SERVICE_NAME}"
systemctl enable "${SERVICE_NAME}.service"
systemctl restart "${SERVICE_NAME}.service"

echo "[+] 查看服务状态 (按需退出)："
systemctl status "${SERVICE_NAME}.service" --no-pager -n 50 || true

echo "[✔] 部署完成。常用命令："
echo "    查看日志：sudo journalctl -u ${SERVICE_NAME}.service -f"
echo "    重启服务：sudo systemctl restart ${SERVICE_NAME}.service"
echo "    停止服务：sudo systemctl stop ${SERVICE_NAME}.service"
echo "    开机自启：sudo systemctl enable ${SERVICE_NAME}.service"
echo "    取消自启：sudo systemctl disable ${SERVICE_NAME}.service"