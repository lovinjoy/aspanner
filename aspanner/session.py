#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" Session Management
"""
__author__ = 'Zagfai'
__date__ = '2022-06'

import google.api_core.exceptions


class SessionOperations:
    """ Only work after inherited by Aspanner Class. """
    async def session_get(self, session):
        if not isinstance(session, str):
            session = session.name

        try:
            return await self._client.get_session(name=session)
        except (google.api_core.exceptions.NotFound, google.api_core.exceptions.InvalidArgument) as e:
            if e.message.startswith('Session not found'):
                return None
            elif e.message.startswith('Invalid GetSession request'):
                return None
            raise

    async def session_create(self):
        session = await self._client.create_session(database=self.name_database)
        return session

    async def session_create_many(self, size):
        """ NOTICE: API do not guarantee return session size as request. """
        resp = await self._client.batch_create_sessions({
            'database': self.name_database,
            'session_count': size
        })
        return [i for i in resp.session]

    async def session_delete(self, session):
        """ Delete a session response from session_create(). """

        if not isinstance(session, str):
            session = session.name
        try:
            await self._client.delete_session(name=session)
        except google.api_core.exceptions.NotFound as e:
            if not e.message.startswith('Session not found'):
                raise

    async def session_list(self):
        pager = await self._client.list_sessions(database=self.name_database)
        sessions = []
        async for list_sessions_resp in pager.pages:
            for i in list_sessions_resp.sessions:
                sessions.append(i)
        return sessions
