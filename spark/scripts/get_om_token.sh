#!/bin/bash
################################################################################
# spark/scripts/get_om_token.sh
# 
# Purpose: Automatically fetch JWT token from OpenMetadata server and 
#          configure Spark to use it for lineage tracking
#
# Usage: 
#   1. Called automatically during container startup (entrypoint)
#   2. Can be called manually: ./get_om_token.sh
#
# Requirements:
#   - curl, jq (for JSON parsing)
#   - OpenMetadata server must be running and healthy
#   - Network access to om-server:8585
################################################################################

set -e  # Exit on error

# ==========================================
# CONFIGURATION
# ==========================================
OM_SERVER_HOST="${OM_SERVER_HOST:-om-server}"
OM_SERVER_PORT="${OM_SERVER_PORT:-8585}"
OM_SERVER_URL="http://${OM_SERVER_HOST}:${OM_SERVER_PORT}"
OM_API_ENDPOINT="${OM_SERVER_URL}/api/v1"

# Default credentials (change in production!)
OM_ADMIN_USER="${OM_ADMIN_USER:-admin}"
OM_ADMIN_PASSWORD="${OM_ADMIN_PASSWORD:-admin}"

# Bot configuration
OM_BOT_NAME="${OM_BOT_NAME:-ingestion-bot}"

# Token storage
TOKEN_FILE="${TOKEN_FILE:-/tmp/om_token.txt}"
SPARK_CONF_DIR="${SPARK_CONF_DIR:-/opt/spark/conf_custom}"
SPARK_DEFAULTS_FILE="${SPARK_CONF_DIR}/spark-defaults.conf"

# Retry configuration
MAX_RETRIES=30
RETRY_DELAY=10

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# ==========================================
# HELPER FUNCTIONS
# ==========================================

log_info() {
    echo -e "${BLUE}[INFO]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

# ==========================================
# WAIT FOR OPENMETADATA SERVER
# ==========================================
wait_for_openmetadata() {
    log_info "Waiting for OpenMetadata server at ${OM_SERVER_URL}..."
    
    local retry=0
    while [ $retry -lt $MAX_RETRIES ]; do
        # Check health endpoint
        if curl -sf "${OM_SERVER_URL}/api/v1/health-check" > /dev/null 2>&1; then
            log_success "OpenMetadata server is healthy!"
            return 0
        fi
        
        retry=$((retry + 1))
        log_warn "Attempt ${retry}/${MAX_RETRIES}: Server not ready. Retrying in ${RETRY_DELAY}s..."
        sleep $RETRY_DELAY
    done
    
    log_error "OpenMetadata server is not responding after ${MAX_RETRIES} attempts"
    return 1
}

# ==========================================
# AUTHENTICATE AND GET ACCESS TOKEN
# ==========================================
get_access_token() {
    log_info "Authenticating with OpenMetadata as user: ${OM_ADMIN_USER}"
    
    local auth_response
    auth_response=$(curl -sf -X POST \
        "${OM_API_ENDPOINT}/users/login" \
        -H "Content-Type: application/json" \
        -d "{
            \"email\": \"${OM_ADMIN_USER}\",
            \"password\": \"${OM_ADMIN_PASSWORD}\"
        }" 2>&1)
    
    if [ $? -ne 0 ]; then
        log_error "Failed to authenticate with OpenMetadata"
        log_error "Response: ${auth_response}"
        return 1
    fi
    
    # Extract access token using jq (or grep/sed if jq not available)
    if command -v jq &> /dev/null; then
        local access_token
        access_token=$(echo "$auth_response" | jq -r '.accessToken // empty')
        
        if [ -z "$access_token" ] || [ "$access_token" = "null" ]; then
            log_error "Failed to extract access token from response"
            log_error "Response: ${auth_response}"
            return 1
        fi
        
        echo "$access_token"
        return 0
    else
        # Fallback: extract token without jq
        local access_token
        access_token=$(echo "$auth_response" | grep -o '"accessToken":"[^"]*"' | cut -d'"' -f4)
        
        if [ -z "$access_token" ]; then
            log_error "Failed to extract access token (jq not available, using grep)"
            return 1
        fi
        
        echo "$access_token"
        return 0
    fi
}

# ==========================================
# GET BOT JWT TOKEN
# ==========================================
get_bot_token() {
    local access_token="$1"
    
    log_info "Fetching JWT token for bot: ${OM_BOT_NAME}"
    
    # First, get the bot entity
    local bot_response
    bot_response=$(curl -sf -X GET \
        "${OM_API_ENDPOINT}/bots/name/${OM_BOT_NAME}" \
        -H "Authorization: Bearer ${access_token}" \
        -H "Content-Type: application/json" 2>&1)
    
    if [ $? -ne 0 ]; then
        log_error "Failed to fetch bot information"
        log_error "Response: ${bot_response}"
        return 1
    fi
    
    # Check if bot exists
    if command -v jq &> /dev/null; then
        local bot_id
        bot_id=$(echo "$bot_response" | jq -r '.id // empty')
        
        if [ -z "$bot_id" ] || [ "$bot_id" = "null" ]; then
            log_error "Bot '${OM_BOT_NAME}' not found"
            log_info "Available bots can be checked at: ${OM_SERVER_URL}/settings/bots"
            return 1
        fi
        
        log_info "Bot ID: ${bot_id}"
    fi
    
    # Generate/fetch the bot's JWT token
    log_info "Generating JWT token for bot..."
    
    local token_response
    token_response=$(curl -sf -X PUT \
        "${OM_API_ENDPOINT}/bots/${OM_BOT_NAME}/revoke/token" \
        -H "Authorization: Bearer ${access_token}" \
        -H "Content-Type: application/json" 2>&1)
    
    if [ $? -ne 0 ]; then
        log_error "Failed to generate bot token"
        log_error "Response: ${token_response}"
        return 1
    fi
    
    # Extract JWT token
    if command -v jq &> /dev/null; then
        local jwt_token
        jwt_token=$(echo "$token_response" | jq -r '.JWTToken // empty')
        
        if [ -z "$jwt_token" ] || [ "$jwt_token" = "null" ]; then
            log_error "Failed to extract JWT token from response"
            log_error "Response: ${token_response}"
            return 1
        fi
        
        echo "$jwt_token"
        return 0
    else
        # Fallback without jq
        local jwt_token
        jwt_token=$(echo "$token_response" | grep -o '"JWTToken":"[^"]*"' | cut -d'"' -f4)
        
        if [ -z "$jwt_token" ]; then
            log_error "Failed to extract JWT token (jq not available)"
            return 1
        fi
        
        echo "$jwt_token"
        return 0
    fi
}

# ==========================================
# SAVE TOKEN TO FILE
# ==========================================
save_token() {
    local token="$1"
    
    log_info "Saving token to: ${TOKEN_FILE}"
    echo "$token" > "$TOKEN_FILE"
    chmod 600 "$TOKEN_FILE"  # Secure the token file
    
    log_success "Token saved successfully"
}

# ==========================================
# UPDATE SPARK CONFIGURATION
# ==========================================
update_spark_config() {
    local token="$1"
    
    log_info "Updating Spark configuration: ${SPARK_DEFAULTS_FILE}"
    
    if [ ! -f "$SPARK_DEFAULTS_FILE" ]; then
        log_error "Spark configuration file not found: ${SPARK_DEFAULTS_FILE}"
        return 1
    fi
    
    # Backup original file
    cp "$SPARK_DEFAULTS_FILE" "${SPARK_DEFAULTS_FILE}.bak"
    
    # Remove any existing token configuration
    sed -i '/spark.openmetadata.transport.headers.Authentication/d' "$SPARK_DEFAULTS_FILE"
    
    # Add new token configuration
    echo "" >> "$SPARK_DEFAULTS_FILE"
    echo "# OpenMetadata JWT Token (Auto-generated by get_om_token.sh)" >> "$SPARK_DEFAULTS_FILE"
    echo "spark.openmetadata.transport.headers.Authentication    Bearer ${token}" >> "$SPARK_DEFAULTS_FILE"
    
    log_success "Spark configuration updated successfully"
}

# ==========================================
# EXPORT TOKEN AS ENVIRONMENT VARIABLE
# ==========================================
export_token_env() {
    local token="$1"
    
    export OM_JWT_TOKEN="$token"
    log_info "Token exported as environment variable: OM_JWT_TOKEN"
    
    # Also create a source-able file for other scripts
    local env_file="/tmp/om_env.sh"
    echo "export OM_JWT_TOKEN='${token}'" > "$env_file"
    chmod 600 "$env_file"
    
    log_info "Token also saved to: ${env_file} (source this file in other scripts)"
}

# ==========================================
# VERIFY TOKEN
# ==========================================
verify_token() {
    local token="$1"
    
    log_info "Verifying token validity..."
    
    local verify_response
    verify_response=$(curl -sf -X GET \
        "${OM_API_ENDPOINT}/users/auth-mechanism" \
        -H "Authorization: Bearer ${token}" \
        -H "Content-Type: application/json" 2>&1)
    
    if [ $? -eq 0 ]; then
        log_success "Token is valid!"
        return 0
    else
        log_error "Token verification failed"
        log_error "Response: ${verify_response}"
        return 1
    fi
}

# ==========================================
# MAIN EXECUTION
# ==========================================
main() {
    log_info "=========================================="
    log_info "OpenMetadata Token Fetcher for Spark"
    log_info "=========================================="
    
    # Check if token already exists and is valid
    if [ -f "$TOKEN_FILE" ]; then
        log_info "Existing token file found: ${TOKEN_FILE}"
        local existing_token
        existing_token=$(cat "$TOKEN_FILE")
        
        if verify_token "$existing_token" 2>/dev/null; then
            log_success "Existing token is still valid. No need to fetch new token."
            export_token_env "$existing_token"
            return 0
        else
            log_warn "Existing token is invalid or expired. Fetching new token..."
        fi
    fi
    
    # Wait for OpenMetadata to be ready
    if ! wait_for_openmetadata; then
        log_error "Cannot proceed without OpenMetadata server"
        exit 1
    fi
    
    # Get access token
    log_info "------------------------------------------"
    local access_token
    access_token=$(get_access_token)
    if [ $? -ne 0 ] || [ -z "$access_token" ]; then
        log_error "Failed to get access token"
        exit 1
    fi
    log_success "Access token obtained"
    
    # Get bot JWT token
    log_info "------------------------------------------"
    local jwt_token
    jwt_token=$(get_bot_token "$access_token")
    if [ $? -ne 0 ] || [ -z "$jwt_token" ]; then
        log_error "Failed to get bot JWT token"
        exit 1
    fi
    log_success "Bot JWT token obtained"
    
    # Verify token
    log_info "------------------------------------------"
    if ! verify_token "$jwt_token"; then
        log_error "Token verification failed"
        exit 1
    fi
    
    # Save token
    log_info "------------------------------------------"
    save_token "$jwt_token"
    
    # Update Spark configuration
    log_info "------------------------------------------"
    if ! update_spark_config "$jwt_token"; then
        log_warn "Failed to update Spark configuration, but token is available"
    fi
    
    # Export as environment variable
    log_info "------------------------------------------"
    export_token_env "$jwt_token"
    
    # Summary
    log_info "=========================================="
    log_success "Token setup complete!"
    log_info "Token file: ${TOKEN_FILE}"
    log_info "Environment variable: OM_JWT_TOKEN"
    log_info "Spark config: ${SPARK_DEFAULTS_FILE}"
    log_info "=========================================="
}

# Run main function
main "$@"