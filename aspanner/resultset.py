#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" Result set
"""
__author__ = 'Zagfai'
__date__ = '2022-06'


import grpc
from google.cloud import spanner_v1


class AsyncStreamedResultSet(spanner_v1.streamed.StreamedResultSet):
    """Inherited StreamedResultSet from standard library, make it support async result sets.
    """
    def __init__(self, iterx, source=None):
        super(AsyncStreamedResultSet, self).__init__(iterx, source)
        self._current_row = {}  # change row from list to dict

    async def _async_consume_next(self):
        """Consume the next partial result set from the stream.

        Parse the result set into new/existing rows in :attr:`_rows`
        """
        response = await self._response_iterator.read()
        if response == grpc.aio.EOF:
            return response
        response_pb = spanner_v1.PartialResultSet.pb(response)

        if self._metadata is None:  # first response
            metadata = self._metadata = response_pb.metadata

            source = self._source
            if source is not None and source._transaction_id is None:
                source._transaction_id = metadata.transaction.id

        if response_pb.HasField("stats"):  # last response
            self._stats = response.stats

        values = list(response_pb.values)
        if self._pending_chunk is not None:
            values[0] = self._merge_chunk(values[0])

        if response_pb.chunked_value:
            self._pending_chunk = values.pop()

        self._merge_values(values)

    async def __aiter__(self):
        while True:
            iter_rows, self._rows[:] = self._rows[:], ()
            while iter_rows:
                yield iter_rows.pop(0)

            res = await self._async_consume_next()
            if res == grpc.aio.EOF:
                return

    def _merge_values(self, values):
        """Merge values into rows.

        :type values: list of :class:`~google.protobuf.struct_pb2.Value`
        :param values: non-chunked values from partial result set.
        """
        fields = [field.name for field in self.fields]
        field_types = [field.type_ for field in self.fields]
        width = len(field_types)
        index = len(self._current_row)
        for value in values:
            self._current_row[fields[index]] = spanner_v1.streamed._parse_value_pb(
                    value, field_types[index])
            index += 1
            if index == width:
                self._rows.append(self._current_row)
                self._current_row = {}
                index = 0
