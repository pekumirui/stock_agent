#!/bin/bash
# .envファイルへの編集をブロックするPreToolUseフック
INPUT=$(cat)
FILE_PATH=$(echo "$INPUT" | jq -r '.tool_input.file_path // empty')

if [[ "$FILE_PATH" == *.env* ]]; then
  jq -n '{
    hookSpecificOutput: {
      hookEventName: "PreToolUse",
      permissionDecision: "deny",
      permissionDecisionReason: ".envファイルは秘密情報を含むため編集できません"
    }
  }'
  exit 0
fi

exit 0
