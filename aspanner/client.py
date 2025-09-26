#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" Client
In aspanner, client provides database operations and session operations.
"""
__author__ = 'Zagfai'
__date__ = '2022-06'

import os
import grpc

from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials
from google.cloud import spanner_v1

from .session import SessionOperations
from .pool import BurstyLimitSessionPool
from .transaction import Transaction


class Aspanner(SessionOperations):
    """ Spanner Database Modules. High level api easy to use, non-admin usage.
        Using async queue, ** NOT thread safe **

        Initialize and close:
            db = aspanner.ASpanner('project id', 'instance id', 'database id')
            db.close()

        Single Operation:
            db.read('table', ['col1', 'col2', ...], [pkey1, pkey2, ...], index='index name')
                -> List[Dict(col:value)]

            db.insert('table', ['col1', 'col2', ...], [(), (), ...])
            db.update('table', ['col1', 'col2', ...], [(), (), ...])  ** MUST include primary key
            db.insert_or_update('table', ['col1', 'col2', ...], [(), (), ...])
            db.replace('table', ['col1', 'col2', ...], [(), (), ...])
            db.delete('table', [pkey1, pkey2, ...])

        Transaction:
            Changes do not have return values, all commit at the end of transaction, or raise exceptions.

            transaction = db.transaction()
            transaction.commit()
            transaction.rollback()
            transaction.close()

            Mutations
                ... like Single Operations

            DML:
                db.query("SELECT * FROM tb_user WHERE pkey=@pkey", {'pkey': '12345'})
                db.execute("UPDATE tb_user SET exp=@exp WHERE pkey=@pkey", {'pkey': '12345', 'exp': 3})

                Read Your Writes: 在每次操作，所执行语句虽未提交（commit），但能读取变化。Mutations则不能。
                    例如，读取用户经验，通过UPDATE语句修改，再次执行SELECT读出用户经验时，数值变化。
                Constraint Checking: 每次操作会检测约束，Mutations则会在提交变更（commit)时检测约束。

        Snapshot:
            只读事务，提高速度，所有读取，以begin_snapshot()的调用时间为准的快照。


        ** Do NOT mix mutations and DMLs in one transaction. **
        一个Transaction内，DML与Mutations不宜混用，除非你非常牛逼
    """

    def __init__(self, project, instance, database, timeout=10,
                 pool=None, default_pool_size=5, default_pool_limit=10):
        """ Init a Spanner Database async

        NOTICE: .close() should be called before shutdown.

        Args:
            instance: string name of instance.
            database: string name of database.
            timeout: a default timeout for all operations.
            pool: session pool, None for a BurstyLimitPool.

        Return:
            A database instance.
        """
        self._closed = False

        emulator_host = os.getenv("SPANNER_EMULATOR_HOST", None)
        if emulator_host:
            self._client = spanner_v1.services.spanner.SpannerAsyncClient(
                transport=spanner_v1.services.spanner.transports.SpannerGrpcAsyncIOTransport(
                    host=emulator_host,
                    credentials=AnonymousCredentials(),
                    channel=grpc.aio.insecure_channel(target=emulator_host)
                )
            )
        else:
            self._client = spanner_v1.services.spanner.SpannerAsyncClient()

        self.name_project = f'projects/{project}'
        self.name_instance = f'{self.name_project}/instances/{instance}'
        self.name_database = f'{self.name_instance}/databases/{database}'
        self._timeout = timeout
        if pool is None:
            pool = BurstyLimitSessionPool(
                    self, target_size=default_pool_size, limit_size=default_pool_limit, timeout=self._timeout)
        self._pool = pool

    def transaction(self):
        return Transaction(self)

    def snapshot(self, min_read_timestamp=None, max_staleness=None,
                 read_timestamp=None, exact_staleness=None):
        """Start a new read-only transaction, read data at specified timestamp, the begin of snapshot."""
        txn = Transaction(
            self, read_only=True, min_read_timestamp=min_read_timestamp,
            max_staleness=max_staleness, read_timestamp=read_timestamp, exact_staleness=exact_staleness)
        return txn

    def _single_use_session_transaction(func):
        async def wrapped_func(self, *args, **kwargs):
            try:
                session = await self._pool.get()
                txn = Transaction(self)
                txn._session = session
                kwargs['transaction'] = txn
                return await func(self, *args, **kwargs)
            finally:
                await self._pool.put(session)
        return wrapped_func

    @_single_use_session_transaction
    async def read(self, *args, **kwargs):
        """ Single use operation, faster do an operation do not need to begin transaction"""
        transaction = kwargs.pop('transaction')
        return await transaction.read(*args, **kwargs)

    @_single_use_session_transaction
    async def insert(self, table, columns, values, transaction=None):
        transaction.insert(table, columns, values)
        return await transaction.commit()

    @_single_use_session_transaction
    async def update(self, table, columns, values, transaction=None):
        """Update one or more existing table rows."""
        transaction.update(table, columns, values)
        return await transaction.commit()

    @_single_use_session_transaction
    async def insert_or_update(self, table, columns, values, transaction=None):
        """Insert/update one or more table rows."""
        transaction.insert_or_update(table, columns, values)
        return await transaction.commit()

    @_single_use_session_transaction
    async def replace(self, table, columns, values, transaction=None):
        """Replace one or more table rows."""
        transaction.replace(table, columns, values)
        return await transaction.commit()

    @_single_use_session_transaction
    async def delete(self, table, key_set, transaction=None):
        """Delete one or more table rows."""
        transaction.delete(table, key_set)
        return await transaction.commit()

    async def close(self):
        if not self._closed:
            self._closed = True
            await self._pool.clear()
            await self._client.transport.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            raise
        await self.close()
