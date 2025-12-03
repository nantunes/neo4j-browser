#!/bin/sh
set -e

# Start the Main Server (UI + Proxy)
echo "Starting Main Server on port 8080..."
node /app/server/app.js &
STATIC_PID=$!

# Start the Auth Server if keys exist and not disabled
if [ -z "$DISABLE_AUTH_SERVER" ] && [ -f "/app/auth_server/keys/private.key" ] && [ -f "/app/auth_server/keys/certificate.pem" ]; then
  echo "Starting Auth Server on port 9001..."
  node /app/auth_server/server.js &
  AUTH_PID=$!
else
  echo "Auth Server disabled or keys not found. Skipping."
  AUTH_PID=""
fi

# Wait for any process to exit
wait -n $STATIC_PID $AUTH_PID

# Exit with the status of the process that exited first
exit $?
