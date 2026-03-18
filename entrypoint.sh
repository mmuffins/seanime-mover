#!/bin/sh

set -eu

if [ -n "${PGID:-}" ]; then
    groupmod -o -g "${PGID}" seanime
fi

if [ -n "${PUID:-}" ]; then
    usermod -o -u "${PUID}" seanime
fi

if [ -n "${PUID:-}" ] || [ -n "${PGID:-}" ]; then
    exec gosu seanime "$@"
fi

exec "$@"
