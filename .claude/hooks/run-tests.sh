#!/bin/bash
# scripts/またはweb/のPythonファイル編集後にpytestを自動実行するPostToolUseフック
INPUT=$(cat)
FILE_PATH=$(echo "$INPUT" | jq -r '.tool_input.file_path // empty')

if [[ "$FILE_PATH" == */scripts/*.py || "$FILE_PATH" == */web/*.py ]]; then
  cd /home/pekumirui/stock_agent
  TEST_OUTPUT=$(venv/bin/python -m pytest tests/ -q --tb=line 2>&1)
  EXIT_CODE=$?

  if [ $EXIT_CODE -ne 0 ]; then
    jq -n --arg output "$TEST_OUTPUT" --arg file "$FILE_PATH" '{
      hookSpecificOutput: {
        hookEventName: "PostToolUse",
        additionalContext: ("Tests FAILED after editing " + $file + ":\n" + $output)
      }
    }'
  fi
fi

exit 0
