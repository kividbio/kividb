"""The Celery app and tasks the harness runs. The broker is CELERY_COMPAT_URL; the result backend is
CELERY_COMPAT_BACKEND_URL, which defaults to the same URL."""
import os
import time

import redis
from celery import Celery

URL = os.environ["CELERY_COMPAT_URL"]
BACKEND_URL = os.environ.get("CELERY_COMPAT_BACKEND_URL") or URL
PREFIX = os.environ.get("CELERY_COMPAT_PREFIX", "cc:")

app = Celery("compat_tasks", broker=URL, backend=BACKEND_URL)
app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    result_expires=600,
    # A short visibility timeout and a sweep every second, so a task whose worker was killed comes
    # back within seconds (kombu's defaults: one hour, and a sweep about every 100 s).
    broker_transport_options={"visibility_timeout": 8, "unacked_restore_interval": 1,
                              "unacked_restore_throttle": 1},
    result_backend_transport_options={"visibility_timeout": 8},
    worker_prefetch_multiplier=1,
    broker_connection_retry_on_startup=True,
    task_routes={"compat_tasks.on_other_queue": {"queue": "compat-other"}},
)

_r = redis.Redis.from_url(URL)


@app.task
def add(x, y):
    return x + y


@app.task
def tsum(values):
    return sum(values)


@app.task
def mark(name):
    _r.rpush(PREFIX + "ran", name)
    _r.expire(PREFIX + "ran", 600)
    return name


@app.task(bind=True, max_retries=5, default_retry_delay=0.2)
def flaky(self, key, fail_times):
    n = _r.incr(PREFIX + key)
    _r.expire(PREFIX + key, 600)
    if n <= fail_times:
        raise self.retry(exc=RuntimeError(f"attempt {n}"))
    return n


@app.task(acks_late=True)
def slow_mark(name, seconds):
    _r.rpush(PREFIX + "started", name)
    _r.expire(PREFIX + "started", 600)
    time.sleep(seconds)
    _r.rpush(PREFIX + "done", name)
    _r.expire(PREFIX + "done", 600)
    return name


@app.task
def on_other_queue(x):
    return x * 2
