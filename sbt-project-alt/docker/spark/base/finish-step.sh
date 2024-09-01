#!/bin/bash

if [ "$ENABLE_INIT_DAEMON" = "true" ]; then
    echo "Finish step ${INIT_DAEMON_STEP} in pipeline"
    while true; do
        sleep 5
        echo -n '.'
        http_code=$(curl -sL -w "%{http_code}" -X PUT "${INIT_DAEMON_BASE_URI}/finish?step=${INIT_DAEMON_STEP}" -o /dev/null)
        [ "$http_code" = "204" ] && break
    done
    echo "Notified finish of step ${INIT_DAEMON_STEP}"
fi
