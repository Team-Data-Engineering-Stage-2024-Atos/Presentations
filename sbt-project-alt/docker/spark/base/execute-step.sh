#!/bin/bash

if [ "$ENABLE_INIT_DAEMON" = "true" ]; then
    echo "Execute step ${INIT_DAEMON_STEP} in pipeline"
    while true; do
        sleep 5
        echo -n '.'
        http_code=$(curl -sL -w "%{http_code}" -X PUT "${INIT_DAEMON_BASE_URI}/execute?step=${INIT_DAEMON_STEP}" -o /dev/null)
        [ "$http_code" = "204" ] && break
    done
    echo "Notified execution of step ${INIT_DAEMON_STEP}"
fi
