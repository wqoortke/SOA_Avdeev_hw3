import logging
import os
from redis import RedisError
from redis.sentinel import Sentinel

log = logging.getLogger(__name__)


class FlightCache:
    def __init__(self):
        addresses = [item.rsplit(":", 1) for item in os.getenv(
            "REDIS_SENTINELS", "sentinel-1:26379,sentinel-2:26379,sentinel-3:26379"
        ).split(",")]
        sentinel = Sentinel([(host, int(port)) for host, port in addresses],
                            socket_timeout=0.3, socket_connect_timeout=0.3)
        self.redis = sentinel.master_for(os.getenv("REDIS_MASTER_NAME", "flight-cache"),
                                         socket_timeout=0.3, socket_connect_timeout=0.3)
        self.ttl = int(os.getenv("CACHE_TTL_SECONDS", "300"))
        if not 300 <= self.ttl <= 600:
            raise ValueError("CACHE_TTL_SECONDS must be between 300 and 600")

    def get(self, key, message_type):
        try:
            value = self.redis.get(key)
            if value is not None:
                result = message_type.FromString(value)
                log.info("cache hit key=%s", key)
                return result
        except (RedisError, ValueError) as exc:
            log.warning("cache unavailable; read PostgreSQL: %s", exc)
        log.info("cache miss key=%s", key)
        return None

    def set(self, key, message):
        try:
            self.redis.setex(key, self.ttl, message.SerializeToString())
        except RedisError as exc:
            log.warning("cache write skipped: %s", exc)

    def invalidate(self, revision):
        try:
            for key in self.redis.scan_iter(match="*:v*", count=100):
                if int(key.rsplit(b":v", 1)[1]) < revision:
                    self.redis.delete(key)
            log.info("cache invalidated revision=%s", revision)
        except (RedisError, ValueError) as exc:
            log.warning("cache cleanup deferred to TTL: %s", exc)
