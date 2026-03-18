#!/bin/sh

set -eu

if [ -n "${PGID:-}" ]; then
    groupmod -o -g "${PGID}" app
fi

if [ -n "${PUID:-}" ]; then
    usermod -o -u "${PUID}" app
fi

if [ -n "${PUID:-}" ] || [ -n "${PGID:-}" ]; then
    exec gosu app "$@"
fi

exec "$@"
