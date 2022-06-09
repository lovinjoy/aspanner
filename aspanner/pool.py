#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" Pool
"""
__author__ = 'Zagfai'
__date__ = '2022-06'

import logging
import asyncio
import time

from google.cloud import spanner_v1

logger = logging.getLogger(__name__)


class BurstyLimitSessionPool:
    """Default session pool.
    Using async queue, ** NOT thread safe **
    """

    IDLE_TIME_CHECK_SESSION = 60

    def __init__(self, db, target_size=5, limit_size=10, timeout=10):
        self._db = db
        self._target_size = target_size
        self._limit_size = limit_size
        self._timeout = timeout
        self.closed = False

        self.session_queue = asyncio.queues.Queue(maxsize=self._target_size)
        self.working_sessions = {}

    async def _new_session_list(self):
        return await self._database.create_batch_session(size=self._target_size)

    async def get(self):
        if self.closed:
            raise ValueError("Pool is already closed.")

        try:
            session = self.session_queue.get_nowait()
        except asyncio.queues.QueueEmpty:
            if len(self.working_sessions) < self._limit_size:
                session = await self._db.session_create()
            else:
                logger.warning("Spanner session usage reached limitation.")
                session = await asyncio.wait_for(self.session_queue.get(), timeout=self._timeout)

        if time.time() - session.approximate_last_use_time.timestamp() > self.IDLE_TIME_CHECK_SESSION:
            exist = await self._db.session_get(session)
            if not exist:
                session = await self._db.session_create()

        self.working_sessions[session.name] = session
        return session

    async def put(self, session):
        if not isinstance(session, spanner_v1.types.spanner.Session):
            raise ValueError("Session must be a spanner session")

        try:
            self.session_queue.put_nowait(session)
        except asyncio.queues.QueueFull:
            await self._db.session_delete(session)
        del self.working_sessions[session.name]

    async def clear(self):
        """Delete all sessions in the pool."""

        self.closed = True

        if self.working_sessions:
            await asyncio.sleep(1)

        while True:
            try:
                session = self.session_queue.get_nowait()
            except asyncio.queues.QueueEmpty:
                break
            else:
                await self._db.session_delete(session)

        if self.working_sessions:
            logger.warning(f"Cleaning up working_sessions, size: {len(self.working_sessions)}")
            await asyncio.sleep(self._timeout)
            for session in self.working_sessions:
                await self._db.session_delete(session)

        logger.debug("Session pool cleared.")
