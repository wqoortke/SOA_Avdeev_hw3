#!/bin/sh
set -eu
if [ ! -s /data/redis.conf ]; then
    cat > /data/redis.conf <<EOF
bind 0.0.0.0
protected-mode no
appendonly yes
dir /data
replica-announce-ip ${REDIS_HOST}
EOF
    if [ "${INITIAL_REPLICA:-no}" = "yes" ]; then
        echo 'replicaof redis-master 6379' >> /data/redis.conf
    fi
fi
exec redis-server /data/redis.conf
