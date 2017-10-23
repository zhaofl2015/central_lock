# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from contextlib import contextmanager
import redis
import time


"""
With this module, you will get a central lock in block or unblock way.

First, you should connect to a redis server. For example,

    redis_client = redis.Redis(host="127.0.0.1", port=6379, db=0, socket_connect_timeout=3, socket_timeout=3)

After that, you will be able to use central_lock or central_lock_block in this way:

    with central_lock('YOUR_KEY_FOR_THIS_LOCK', timeout=None, retry_cnt=3) as lock:
        if lock:
            # do things where should be protected by the central lock
            ...

    or:

    with central_lock_block('YOUR_KEY_FOR_THIS_LOCK', timeout=None, retry_cnt=3, interval=1) as lock:
        if lock:
            # do things where should be protected by the central lock
            ...

    central_lock will failed in these circumstances:
    1. use timeout parameter but set timeout failed after try retry_cnt times
    2. fail to get lock

    but central_lock will never block, it will return instantaneously.

    central_lock will always wait for the lock, and set the lock with timeout if setting the timeout.
    If not, it will retry every interval seconds.
"""


redis_client = redis.Redis(host="127.0.0.1", port=6379, db=0, socket_connect_timeout=3, socket_timeout=3)


@contextmanager
def central_lock(key, timeout=None, retry_cnt=3):
    try:
        if redis_client.setnx(key, 1) is True:
            if timeout:
                while retry_cnt:
                    if redis_client.expire(key, timeout) is True:
                        break
                    retry_cnt -= 1

                if retry_cnt == 0:
                    redis_client.delete(key)
                    yield False
            yield True
        else:
            yield False

    finally:
        redis_client.delete(key)


@contextmanager
def central_lock_block(key, timeout=None, retry_cnt=3, interval=1):
    try:
        while True:
            if redis_client.setnx(key, 1) is True:
                if timeout:
                    while retry_cnt:
                        if redis_client.expire(key, timeout) is True:
                            break
                        retry_cnt -= 1

                    if retry_cnt == 0:
                        redis_client.delete(key)
                        continue
                yield True
                break
            else:
                time.sleep(interval)
                continue

    finally:
        redis_client.delete(key)
