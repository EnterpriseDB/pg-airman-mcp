#!/bin/bash

# Don't exit immediately so we can debug issues
# set -e

# Function to replace localhost in a string with the Docker host
replace_localhost() {
    local input_str="$1"
    local docker_host=""

    if [[ -n "$KUBERNETES_SERVICE_HOST" ]]; then
        echo "Kubernetes detected: Using localhost as-is for sidecar communication" >&2
        echo "$input_str"
        return 0
    fi

    # Try to determine Docker host address
    if ping -c 1 -w 1 host.docker.internal >/dev/null 2>&1; then
        docker_host="host.docker.internal"
        echo "Docker Desktop detected: Using host.docker.internal for localhost" >&2
    elif ping -c 1 -w 1 172.17.0.1 >/dev/null 2>&1; then
        docker_host="172.17.0.1"
        echo "Docker on Linux detected: Using 172.17.0.1 for localhost" >&2
    else
        echo "WARNING: Cannot determine Docker host IP. Using original address." >&2
        return 1
    fi

    # Replace localhost with Docker host
    if [[ -n "$docker_host" ]]; then
        local new_str="${input_str/localhost/$docker_host}"
        echo "  Remapping: $input_str --> $new_str" >&2
        echo "$new_str"
        return 0
    fi

    # No replacement made
    echo "$input_str"
    return 1
}

# Create a new array for the processed arguments
processed_args=()
processed_args+=("$1")
shift 1

# Process remaining command-line arguments for postgres:// or postgresql:// URLs that contain localhost
for arg in "$@"; do
    if [[ "$arg" == *"postgres"*"://"*"localhost"* ]]; then
        echo "Found localhost in database connection: $arg" >&2
        if new_arg=$(replace_localhost "$arg"); then
            processed_args+=("$new_arg")
        else
            processed_args+=("$arg")
        fi
    else
        processed_args+=("$arg")
    fi
done

# --- Backward compatibility: bridge DATABASE_URI -> AIRMAN_MCP_DATABASE_URL ---
if [[ -n "$DATABASE_URI" ]]; then
    if [[ -z "$AIRMAN_MCP_DATABASE_URL" ]]; then
        echo "WARNING: DATABASE_URI is deprecated. Use AIRMAN_MCP_DATABASE_URL instead." >&2
        export AIRMAN_MCP_DATABASE_URL="$DATABASE_URI"
    else
        echo "WARNING: Both DATABASE_URI and AIRMAN_MCP_DATABASE_URL are set. Using AIRMAN_MCP_DATABASE_URL." >&2
    fi
fi

# Check and replace localhost in AIRMAN_MCP_DATABASE_URL if it exists
if [[ -n "$AIRMAN_MCP_DATABASE_URL" && "$AIRMAN_MCP_DATABASE_URL" == *"postgres"*"://"*"localhost"* ]]; then
    echo "Found localhost in AIRMAN_MCP_DATABASE_URL: $AIRMAN_MCP_DATABASE_URL" >&2
    if new_uri=$(replace_localhost "$AIRMAN_MCP_DATABASE_URL"); then
        export AIRMAN_MCP_DATABASE_URL="$new_uri"
    fi
fi


# Check if SSE or streamable-http transport is specified and set host/port options
has_sse=false
has_sse_host=false
has_streamable_http=false
has_streamable_http_host=false
has_streamable_http_port=false

for arg in "${processed_args[@]}"; do
    if [[ "$arg" == "--transport" ]]; then
        # Check next argument for "sse" or "streamable-http"
        for next_arg in "${processed_args[@]}"; do
            if [[ "$next_arg" == "sse" ]]; then
                has_sse=true
                break
            elif [[ "$next_arg" == "streamable-http" ]]; then
                has_streamable_http=true
                break
            fi
        done
    elif [[ "$arg" == "--transport=sse" ]]; then
        has_sse=true
    elif [[ "$arg" == "--transport=streamable-http" ]]; then
        has_streamable_http=true
    elif [[ "$arg" == "--sse-host"* ]]; then
        has_sse_host=true
    elif [[ "$arg" == "--streamable-http-host"* ]]; then
        has_streamable_http_host=true
    elif [[ "$arg" == "--streamable-http-port"* ]]; then
        has_streamable_http_port=true
    fi
done

# Add --sse-host if needed
if [[ "$has_sse" == true ]] && [[ "$has_sse_host" == false ]]; then
    echo "SSE transport detected, adding --sse-host=0.0.0.0" >&2
    processed_args+=("--sse-host=0.0.0.0")
fi

# Add --streamable-http-host if needed
if [[ "$has_streamable_http" == true ]] && [[ "$has_streamable_http_host" == false ]]; then
    echo "streamable-http transport detected, adding --streamable-http-host=0.0.0.0" >&2
    processed_args+=("--streamable-http-host=0.0.0.0")
fi

# Add --streamable-http-port if needed
if [[ "$has_streamable_http" == true ]] && [[ "$has_streamable_http_port" == false ]]; then
    echo "streamable-http transport detected, adding --streamable-http-port=8080" >&2
    processed_args+=("--streamable-http-port=8080")
fi

echo "----------------" >&2
echo "Executing MCP server command:" >&2
echo "${processed_args[@]}" >&2
echo "----------------" >&2

# Execute the command with the processed arguments
"${processed_args[@]}"

# Capture exit code from the Python process
exit_code=$?

# If the Python process failed, print additional debug info
if [ $exit_code -ne 0 ]; then
    echo "ERROR: Command failed with exit code $exit_code" >&2
    echo "Command was: ${processed_args[@]}" >&2
fi

# Return the exit code from the Python process
exit $exit_code
