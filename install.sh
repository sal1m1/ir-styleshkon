#!/usr/bin/env bash
set -euo pipefail
apt-get update
apt-get install -y ffmpeg python3 python3-venv python3-pip
mkdir -p /opt/ir-styleshkon /var/lib/ir-styleshkon
cp -r . /opt/ir-styleshkon/
cd /opt/ir-styleshkon
python3 -m venv .venv
. .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
if [ ! -f .env ]; then cp .env.example .env; fi
echo "Installation files copied. Configure /opt/ir-styleshkon/.env and install systemd unit."
