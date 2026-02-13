#!/bin/bash
# ログ世代削除スクリプト
# 使用方法: cleanup_logs.sh [保持日数（デフォルト: 30）]

DAYS=${1:-30}
LOG_DIR=~/stock_agent/logs

find "$LOG_DIR" -name "*.log" -type f -mtime +"$DAYS" -delete
