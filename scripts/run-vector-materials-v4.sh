#!/usr/bin/env bash
set -euo pipefail

cd /usr/docker/gaogao-api/src

KEY_FILE=/usr/docker/openclaw/workspace/report-agent/config/research-keys.json
if [[ -f "$KEY_FILE" ]]; then
  export OPENAI_API_KEY="$(
    python3 - <<'PY'
import json
with open('/usr/docker/openclaw/workspace/report-agent/config/research-keys.json', encoding='utf-8') as f:
    print(json.load(f).get('openaiEmbeddingApiKey', ''))
PY
  )"
fi

export PGVECTOR_DATABASE_URL="${PGVECTOR_DATABASE_URL:-postgres://postgres:passok@127.0.0.1:5432/news}"
export PGVECTOR_NEWS_TABLE="${PGVECTOR_NEWS_TABLE:-vector_materials_text_embedding_v4}"
export PGVECTOR_EMBEDDING_MODEL="${PGVECTOR_EMBEDDING_MODEL:-text-embedding-v4}"
export PGVECTOR_EMBEDDING_DIMENSIONS="${PGVECTOR_EMBEDDING_DIMENSIONS:-1024}"
export PGVECTOR_EMBEDDING_BASE_URL="${PGVECTOR_EMBEDDING_BASE_URL:-https://dashscope.aliyuncs.com/compatible-mode/v1}"
export VECTOR_BACKFILL_MAX_TEXT_CHARS="${VECTOR_BACKFILL_MAX_TEXT_CHARS:-600}"
export VECTOR_BACKFILL_BATCH_SIZE="${VECTOR_BACKFILL_BATCH_SIZE:-10}"

exec npx tsx scripts/backfill-vector-materials.ts \
  --pg-table="$PGVECTOR_NEWS_TABLE" \
  --embedding-model="$PGVECTOR_EMBEDDING_MODEL" \
  --embedding-dimensions="$PGVECTOR_EMBEDDING_DIMENSIONS" \
  --embedding-base-url="$PGVECTOR_EMBEDDING_BASE_URL" \
  --max-text-chars="$VECTOR_BACKFILL_MAX_TEXT_CHARS" \
  --batch-size="$VECTOR_BACKFILL_BATCH_SIZE" \
  "$@"
