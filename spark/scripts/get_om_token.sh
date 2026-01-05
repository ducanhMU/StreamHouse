#!/bin/bash
set -e

# CONFIGURATION
OM_SERVER_URL="http://${OM_SERVER_HOST:-openmetadata-server}:${OM_SERVER_PORT:-8585}"
OM_API_ENDPOINT="${OM_SERVER_URL}/api/v1"
OM_ADMIN_USER="${OM_ADMIN_USER:-admin}"
OM_ADMIN_PASSWORD="${OM_ADMIN_PASSWORD:-admin}"
OM_BOT_NAME="${OM_BOT_NAME:-ingestion-bot}"
TOKEN_FILE="${TOKEN_FILE:-/tmp/om_token.txt}"
MAX_RETRIES=30
RETRY_DELAY=10

log_info() { echo -e "$(date '+%Y-%m-%d %H:%M:%S') - [INFO] $1"; }
log_warn() { echo -e "$(date '+%Y-%m-%d %H:%M:%S') - [WARN] $1"; }
log_error() { echo -e "$(date '+%Y-%m-%d %H:%M:%S') - [ERROR] $1"; }
source /opt/scripts/get_om_token.sh
wait_for_openmetadata() {
    log_info "Waiting for OpenMetadata at ${OM_SERVER_URL}..."
    local retry=0
    while [ $retry -lt $MAX_RETRIES ]; do
        if curl -sf "${OM_SERVER_URL}/api/v1/health-check" > /dev/null 2>&1; then
            return 0
        fi
        retry=$((retry + 1))
        sleep $RETRY_DELAY
    done
    return 1
}

get_access_token() {
    local auth_resp
    auth_resp=$(curl -sf -X POST "${OM_API_ENDPOINT}/users/login" \
        -H "Content-Type: application/json" \
        -d "{\"email\": \"${OM_ADMIN_USER}\", \"password\": \"${OM_ADMIN_PASSWORD}\"}" 2>&1)
    
    if command -v jq &> /dev/null; then
        echo "$auth_resp" | jq -r '.accessToken // empty'
    else
        echo "$auth_resp" | grep -o '"accessToken":"[^"]*"' | cut -d'"' -f4
    fi
}

get_bot_token() {
    local access_token="$1"
    local retry=0
    local bot_id=""
    
    log_info "Fetching Bot ID for: ${OM_BOT_NAME}..."
    
    # Retry loop to wait for bot creation (Cold Start fix)
    while [ $retry -lt $MAX_RETRIES ]; do
        local bot_resp
        bot_resp=$(curl -sf -X GET "${OM_API_ENDPOINT}/bots/name/${OM_BOT_NAME}" \
            -H "Authorization: Bearer ${access_token}" 2>/dev/null)

        if [ $? -eq 0 ]; then
            if command -v jq &> /dev/null; then
                bot_id=$(echo "$bot_resp" | jq -r '.id // empty')
            else
                bot_id=$(echo "$bot_resp" | grep -o '"id":"[^"]*"' | cut -d'"' -f4)
            fi
            
            if [ -n "$bot_id" ] && [ "$bot_id" != "null" ]; then
                log_info "Bot found: ${bot_id}"
                break
            fi
        fi
        
        retry=$((retry + 1))
        log_warn "Bot not ready yet. Retrying in ${RETRY_DELAY}s..."
        sleep $RETRY_DELAY
    done

    if [ -z "$bot_id" ]; then return 1; fi

    local token_resp
    token_resp=$(curl -sf -X PUT "${OM_API_ENDPOINT}/bots/${OM_BOT_NAME}/revoke/token" \
        -H "Authorization: Bearer ${access_token}" -H "Content-Type: application/json")
        
    if command -v jq &> /dev/null; then
        echo "$token_resp" | jq -r '.JWTToken // empty'
    else
        echo "$token_resp" | grep -o '"JWTToken":"[^"]*"' | cut -d'"' -f4
    fi
}

main() {
    if ! wait_for_openmetadata; then log_error "OM Server not ready"; exit 1; fi
    
    local access_token
    access_token=$(get_access_token)
    if [ -z "$access_token" ]; then log_error "Auth failed"; exit 1; fi
    
    local jwt_token
    jwt_token=$(get_bot_token "$access_token")
    if [ -z "$jwt_token" ]; then log_error "Bot token failed"; exit 1; fi
    
    echo "$jwt_token" > "$TOKEN_FILE"
    export OM_JWT_TOKEN="$jwt_token"
    
    # Create sourceable file
    echo "export OM_JWT_TOKEN='$jwt_token'" > /tmp/om_env.sh
    chmod 600 /tmp/om_env.sh
    
    log_info "Token successfully acquired."
}

main "$@"    