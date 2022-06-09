#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" Transaction
"""
__author__ = 'Zagfai'
__date__ = '2022-06'

import ujson
import logging
import datetime

from google.protobuf.struct_pb2 import Struct
from google.cloud import spanner_v1

from .resultset import AsyncStreamedResultSet

logger = logging.getLogger(__name__)


class Transaction:
    """Implement read-write transaction semantics for a session.
    Also support read-only transaction, and single_use_transaction for some methods.
    """
    KeySet = spanner_v1.keyset.KeySet
    KeyRange = spanner_v1.keyset.KeyRange

    def __init__(self, db, read_only=False, **kwargs):
        self._db = db
        self._timeout = db._timeout
        self._transaction_read_only = read_only
        self._kwargs = kwargs

        self._session = None
        self._transaction = None  # TransactionSelector, None default as strong read-only for read
        self._finished = False

        self.mutations = []
        self.execute_sql_count = 0

    def _check_finished(self):
        if self._finished:
            raise ValueError("Transaction is already finished")

    def _check_in_transaction(self):
        if self._transaction_read_only:
            raise ValueError("Transaction is read-only")
        if not self._transaction:
            raise ValueError("Transaction do not initialized")

    async def read(self, table, columns, key_set, index="", limit=None,
                   partition_token=None, request_options=None, timeout=None):
        if key_set is None:
            key_set = spanner_v1.keyset.KeySet(all_=True)
        elif not isinstance(key_set, self.KeySet):
            key_set = spanner_v1.keyset.KeySet(key_set)

        resp = await self._db._client.streaming_read(
            request={
                'session': self._session.name,
                'transaction': self._transaction,
                'table': table,
                'columns': columns,
                'key_set': key_set._to_pb(),
                'index': index,
                'limit': limit,
                'partition_token': partition_token,
                'request_options': request_options,
            },
            timeout=(timeout or self._timeout)
        )

        result = [row async for row in AsyncStreamedResultSet(resp)]
        return result

    @staticmethod
    def _make_write_pb(table, columns, values):
        return spanner_v1.types.Mutation.Write(
                table=table, columns=columns,
                values=spanner_v1._helpers._make_list_value_pbs(values))

    @staticmethod
    def _support_dict_value(columns, values):
        for idx in range(len(values)):
            if isinstance(values[idx], tuple):
                values[idx] = list(values[idx])

            if isinstance(values[idx], list):
                for validx in range(len(values[idx])):
                    if isinstance(values[idx][validx], dict):
                        values[idx][validx] = ujson.dumps(values[idx][validx])
            elif isinstance(values[idx], dict):
                temp = []

                for key in columns:
                    if isinstance(values[idx][key], dict):  # for type dict in value change to JSON
                        values[idx][key] = ujson.dumps(values[idx][key])

                    temp.append(values[idx][key])

                values[idx] = temp

    @staticmethod
    def data_cast(params, param_types):
        """Auto detect some type of parameters.
        Type casting:
            int: spanner.param_types.INT64
            float: spanner.param_types.FLOAT64
            bool: spanner.param_types.BOOL
            str: spanner.param_types.STRING
            decimal.Decimal: spanner.param_types.NUMERIC

            bytes in base64: spanner.param_types.BYTES
            date: spanner.param_types.DATE
            datetime: spanner.param_types.TIMESTAMP

            list, tuple: spanner.param_types.Array  ** Only with same type elements
            dumps string of JSON: spanner.param_types.JSON
        """
        for i in params:
            if i not in param_types:
                if type(params[i]) == datetime.date:
                    param_types[i] = spanner_v1.param_types.DATE
                elif type(params[i]) == datetime.datetime:
                    param_types[i] = spanner_v1.param_types.TIMESTAMP
                elif isinstance(params[i], dict):
                    param_types[i] = spanner_v1.param_types.JSON
                    params[i] = ujson.dumps(params[i])

    def insert(self, table, columns, values):
        self._support_dict_value(columns, values)
        self.mutations.append(spanner_v1.types.Mutation(
            insert=self._make_write_pb(table, columns, values)))

    def update(self, table, columns, values):
        """Update one or more existing table rows."""
        self._support_dict_value(columns, values)
        self.mutations.append(spanner_v1.types.Mutation(
            update=self._make_write_pb(table, columns, values)))

    def insert_or_update(self, table, columns, values):
        """Insert/update one or more table rows."""
        self._support_dict_value(columns, values)
        self.mutations.append(spanner_v1.types.Mutation(
            insert_or_update=self._make_write_pb(table, columns, values)))

    def replace(self, table, columns, values):
        """Replace one or more table rows."""
        self._support_dict_value(columns, values)
        self.mutations.append(spanner_v1.types.Mutation(
            replace=self._make_write_pb(table, columns, values)))

    def delete(self, table, key_set):
        """Delete one or more table rows."""
        if not isinstance(key_set, spanner_v1.types.KeySet):
            key_set = spanner_v1.keyset.KeySet(key_set)
        delete = spanner_v1.types.Mutation.Delete(table=table, key_set=key_set._to_pb())
        self.mutations.append(spanner_v1.types.Mutation(delete=delete))

    async def query(
            self,
            sql,
            params=None,
            param_types=None,
            query_mode=None,
            query_options=None,
            request_options=None,
            partition=None,
            timeout=None,
            ):
        if params:
            params_pb = Struct(fields={key: spanner_v1._helpers._make_value_pb(value)
                                       for key, value in params.items()})
        else:
            params_pb = {}

        params = params or {}
        param_types = param_types or {}
        self.data_cast(params, param_types)

        seqno = self.execute_sql_count
        self.execute_sql_count += 1

        resp = await self._db._client.execute_streaming_sql(
            request={
                'session': self._session.name,
                'transaction': self._transaction,
                'sql': sql,
                'params': params_pb,
                'param_types': param_types,
                'query_mode': query_mode,
                'query_options': query_options,
                'seqno': seqno,
                'request_options': request_options,
            },
            timeout=(timeout or self._timeout)
        )

        result = [row async for row in AsyncStreamedResultSet(resp)]
        return result

    async def execute(
            self,
            dml,
            params=None,
            param_types=None,
            query_mode=None,
            query_options=None,
            request_options=None,
            timeout=None):

        seqno = self.execute_sql_count
        self.execute_sql_count += 1

        params = params or {}
        param_types = param_types or {}
        self.data_cast(params, param_types)

        resp = await self._db._client.execute_sql(
            request={
                'session': self._session.name,
                'transaction': self._transaction,
                'sql': dml,
                'params': Struct(fields={key: spanner_v1._helpers._make_value_pb(value)
                                         for key, value in params.items()}),
                'param_types': param_types,
                'query_mode': query_mode,
                'query_options': query_options,
                'seqno': seqno,
                'request_options': request_options,
            },
            timeout=(timeout or self._timeout)
        )

        return resp.stats.row_count_exact

    async def begin_transaction(self):
        if self._transaction:
            raise ValueError("Transaction is already started")

        txn_options = spanner_v1.types.TransactionOptions(
            read_write=spanner_v1.types.TransactionOptions.ReadWrite())

        resp = await self._db._client.begin_transaction(
                session=self._session.name,
                options=txn_options,
                timeout=self._timeout
            )
        logger.debug("Transaction began %s", resp)

        self._transaction = spanner_v1.types.TransactionSelector(id=resp.id)

    async def begin_snapshot(self, **kwargs):
        for param in ('read_timestamp', 'min_read_timestamp', 'max_staleness', 'exact_staleness'):
            if kwargs.get(param) is not None:
                key = param
                value = kwargs.get(param)
                break
        else:
            key = "strong"
            value = True

        txn_options = spanner_v1.types.TransactionOptions(
            read_only=spanner_v1.types.TransactionOptions.ReadOnly(**{key: value})
        )

        resp = await self._db._client.begin_transaction(
                session=self._session.name,
                options=txn_options,
                timeout=self._timeout
            )
        logger.debug("Snapshot began %s", resp)

        self._transaction = spanner_v1.types.TransactionSelector(id=resp.id)
        logger.debug(self._transaction)

    async def commit(self, mutations=None, return_commit_stats=True, request_options=None):
        self._check_finished()

        if mutations:
            self.mutations.extend(mutations)

        request = {
                'session': self._session.name,
                'return_commit_stats': return_commit_stats,
                'request_options': request_options,
                'mutations': self.mutations,
        }

        if self._transaction:
            request['transaction_id'] = self._transaction.id
        else:
            request['single_use_transaction'] = spanner_v1.types.TransactionOptions(
                    read_write=spanner_v1.types.TransactionOptions.ReadWrite())

        self._finished = True
        logger.debug(f"Committed mutations:{len(self.mutations)}, sql:{self.execute_sql_count}")
        resp = await self._db._client.commit(request=request, timeout=self._timeout)

        return resp

    async def rollback(self):
        """ Rollback the transaction, just for sql DMLs.
        Mutations do not need rollback because they have no status in Spanner."""
        self._check_finished()
        self._check_in_transaction()
        self._finished = True
        await self._db._client.rollback(
                session=self._session.name,
                transaction_id=self._transaction.id,
                timeout=self._timeout)

    async def __aenter__(self):
        self._session = await self._db._pool.get()
        if self._transaction_read_only:
            await self.begin_snapshot(**self._kwargs)
        else:
            await self.begin_transaction()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if not self._transaction_read_only and self._transaction and not self._finished:
            if exc_type:
                try:
                    await self.rollback()
                finally:
                    raise
            else:
                await self.commit()

        await self._db._pool.put(self._session)
