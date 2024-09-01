#!/bin/bash

if [ "$ENABLE_INIT_DAEMON" = "true" ]; then
    echo "Validating if step ${INIT_DAEMON_STEP} can start in pipeline"
    while true; do
        sleep 5
        echo -n '.'
        can_start=$(curl -s "${INIT_DAEMON_BASE_URI}/canStart?step=${INIT_DAEMON_STEP}")
        [ "$can_start" = "true" ] && break
    done
    echo "Can start step ${INIT_DAEMON_STEP}"
fi
