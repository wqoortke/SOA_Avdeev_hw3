#!/bin/sh
set -eu
if [ ! -s /data/sentinel.conf ]; then
cat > /data/sentinel.conf <<EOF
port 26379
bind 0.0.0.0
protected-mode no
sentinel resolve-hostnames yes
sentinel announce-hostnames yes
sentinel announce-ip ${SENTINEL_HOST}
sentinel monitor flight-cache redis-master 6379 2
sentinel down-after-milliseconds flight-cache 3000
sentinel failover-timeout flight-cache 15000
sentinel parallel-syncs flight-cache 1
EOF
fi
exec redis-server /data/sentinel.conf --sentinel
