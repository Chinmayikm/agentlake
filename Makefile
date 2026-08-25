.PHONY: gateway

gateway:
	.venv/bin/uvicorn services.gateway.app:create_app --factory --reload --port 8100

# Non-streaming:
#   curl -s localhost:8100/v1/chat -H 'content-type: application/json' \
#     -d '{"model_alias": "fast", "messages": [{"role": "user", "content": "hi"}]}' | jq
#
# Streaming (SSE):
#   curl -N -s localhost:8100/v1/chat -H 'content-type: application/json' \
#     -d '{"model_alias": "fast", "stream": true, "messages": [{"role": "user", "content": "hi"}]}'
#
# Health / stats:
#   curl -s localhost:8100/v1/health | jq
#   curl -s localhost:8100/v1/stats | jq
