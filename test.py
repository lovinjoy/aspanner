#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" AsyncSpanner Test
"""
__author__ = 'Zagfai'

import time
import base64
import datetime
import logging
import asyncio
import google.cloud.spanner_v1 as spanner_v1

import aspanner


args = ('project id', 'instance name', 'database name')


async def test_session():
    db = aspanner.Aspanner(*args)
    print("Db instance: ", db)

    session_already_exist = await db.session_list()
    exist_session_set = {i.name for i in session_already_exist}
    print("Exist session count:", len(exist_session_set))

    session = await db.session_create()
    assert isinstance(session, spanner_v1.types.spanner.Session), "Session create error"
    assert 'name' in session, "Session create error"

    session_new_set = {i.name for i in await db.session_list()}
    assert session_new_set - exist_session_set == set([session.name])

    bfd = await db.session_get(session)
    print("Get before delete", bfd.name)
    assert isinstance(bfd, spanner_v1.types.spanner.Session)
    print("Delete Session: ", await db.session_delete(session))
    afd = await db.session_get(session)
    print("Get after delete", afd)
    assert afd is None

    false_name = session.name[:-3] + 'aBc'
    assert (await db.session_get(false_name)) is None

    session_list = await db.session_create_many(5)
    assert isinstance(session_list, list)
    assert isinstance(session_list[0], spanner_v1.types.spanner.Session)
    assert len(session_list) == 5

    session_new_set = {i.name for i in await db.session_list()}
    assert session_new_set - exist_session_set == set([session.name for session in session_list])
    print("Session count: ", len(session_new_set))

    for i in session_list:
        await db.session_delete(i)

    session_new_set = {i.name for i in await db.session_list()}
    assert session_new_set == exist_session_set

    await db.close()


async def test_pool():
    async with aspanner.Aspanner(*args) as db:
        print("Db instance: ", db)
        session = await db._pool.get()
        assert isinstance(session, spanner_v1.types.spanner.Session), "Session create error"
        assert 'name' in session, "Session create error"
        assert len(db._pool.working_sessions) == 1

        await db._pool.put(session)
        assert len(db._pool.working_sessions) == 0
        assert not db._pool.session_queue.empty()

        sessions = []
        for i in range(db._pool._limit_size):
            sessions.append(await db._pool.get())
        assert db._pool.session_queue.empty()

        try:
            await db._pool.get()
        except asyncio.exceptions.TimeoutError:
            pass
        else:
            assert False


async def test_transaction():
    async with aspanner.Aspanner(*args) as db:
        async with db.transaction() as txn:
            cols = ('id', 'data_int', 'data_float', 'data_str', 'data_bool',
                    'data_bytes', 'data_date', 'data_num', 'data_time',
                    'data_array', 'data_json')
            print(await txn.read("tb_test_types", cols, None))
            types = await txn.read("tb_test_types", cols, None)

            types[0]['data_int'] += 1

            cols = ('id', 'data_int',)
            txn.update("tb_test_types", cols, types)
            print(await txn.commit())
            try:
                await txn.rollback()
            except ValueError as e:
                assert e.args[0].startswith('Transaction is already finished')

        txn = db.transaction()
        txn._session = await txn._db._pool.get()
        cols = ('id', 'data_int', 'data_float', 'data_str', 'data_bool',
                'data_bytes', 'data_date', 'data_num', 'data_time',
                'data_array', 'data_json')
        print(await txn.read("tb_test_types", cols, None))
        await txn._db._pool.put(txn._session)
        del txn


async def test_snapshot():
    async with aspanner.Aspanner(*args) as db:
        async with db.snapshot() as snapshot:
            cols = ('id', 'data_int', 'data_float', 'data_str', 'data_bool',
                    'data_bytes', 'data_date', 'data_num', 'data_time',
                    'data_array', 'data_json')
            print(await snapshot.read("tb_test_types", cols, None))
            data = await snapshot.read("tb_test_types", cols, None)

            async with db.transaction() as txn:
                txn.update("tb_test_types", ('id', 'data_int',), [(0, data[0]['data_int'] + 1)])

            await asyncio.sleep(1)

            new_data = await snapshot.read("tb_test_types", cols, None)

            # new data is same as old one because read at the snapshot timestamp
            assert new_data == data


async def test_sql():
    async with aspanner.Aspanner(*args) as db:
        async with db.transaction() as txn:
            print(await txn.query("SELECT * FROM tb_test_types"))

            sql = "DELETE FROM tb_test_types WHERE id=@id"
            print(await txn.execute(sql, {'id': 1, 'data_int': 999}))

            item = {
                'id': 1,
                'data_int': 987,
                'data_float': 654.3,
                'data_str': '210',
                'data_bool': True,
                'data_bytes': base64.b64encode(b"54321"),
                'data_date': datetime.date.today(),
                'data_num': 210,
                'data_time': datetime.datetime.now(),
                'data_array': [0, 11, 222],
                'data_json': {"a": 1, "b": [1, 2, 3]}
            }
            values = ','.join(['@'+i for i in item.keys()])
            sql = f"INSERT INTO tb_test_types ({','.join(item.keys())}) VALUES ({values})"
            print(await txn.execute(sql, item))

            print(await txn.query("SELECT * FROM tb_test_types"))

            res = await txn.query("SELECT * FROM tb_test_types WHERE id=@id", {'id': 1})
            assert res[0]['data_json']['b'][2] == 3

            async with db.snapshot() as snapshot:
                print("Query", await snapshot.query("SELECT * FROM tb_test_types"))

            sql = "DELETE FROM tb_test_types WHERE id=@id"
            print(await txn.execute(sql, {'id': 1, 'data_int': 999}))

            print(await txn.query("SELECT * FROM tb_test_types"))

        async with db.snapshot() as snapshot:
            cols = ('id', 'data_int', 'data_float', 'data_str', 'data_bool',
                    'data_bytes', 'data_date', 'data_num', 'data_time',
                    'data_array', 'data_json')
            print(await snapshot.read("tb_test_types", cols, None))


async def test_mutations():
    try:
        db = aspanner.Aspanner(*args)

        cols = ('id', 'data_int', 'data_float', 'data_str', 'data_bool',
                'data_bytes', 'data_date', 'data_num', 'data_time',
                'data_array', 'data_json')

        async with db.transaction() as txn:
            txn.delete('tb_test_types', [(1,)])

        async with db.transaction() as txn:
            res = txn.insert(
                    "tb_test_types", cols,
                    [[1, 12345678, 1234.5678, "12345", True,
                      base64.b64encode(b"54321"), datetime.date.today(), 5678, datetime.datetime.now(),
                      [1, 2, 3, 4, 5], {"nums": [1, 3, 5, "'''"]}
                      ]])
            print(f"Insert return {res}")

        async with db.transaction() as txn:
            print(await txn.read("tb_test_types", cols, [(1,)]))
            print(txn.update("tb_test_types", ('id', 'data_num'), [(1, 101)]))
            print(await txn.read("tb_test_types", cols, [(1,)]))

        async with db.transaction() as txn:
            res = await txn.read("tb_test_types", cols, [(1,)])
            print("Select", res)
            print(res[0]['data_json'].get('aab'))
            print(res[0]['data_json']['nums'])

        async with db.transaction() as txn:
            ks = txn.KeySet(keys=[(1,), (2,)])
            print(await txn.read("tb_test_types", ('id', 'data_num', 'data_int'), key_set=ks))

            ks = txn.KeySet(ranges=[txn.KeyRange(start_closed=(0,), end_closed=(10,))])
            print(await txn.read("tb_test_types", ('id', 'data_num', 'data_int'), key_set=ks))

            ks = txn.KeySet(all_=True)
            print(await txn.read("tb_test_types", ('id', 'data_num', 'data_int'), key_set=ks))

        async with db.transaction() as txn:
            txn.delete('tb_test_types', [(1,)])

        logging.info("Finished main thread fun()")
    finally:
        await db.close()


async def test_single_use_speed_compare():
    async with aspanner.Aspanner(*args) as db:
        cols = ('id', 'data_int', 'data_float', 'data_str', 'data_bool',
                'data_bytes', 'data_date', 'data_num', 'data_time',
                'data_array', 'data_json')

        print(await db.read('tb_test_types', cols, None))

        t0 = time.time()
        for i in range(20):
            await db.read('tb_test_types', cols, None)
        print(time.time() - t0)

        t0 = time.time()
        for i in range(20):
            async with db.transaction() as txn:
                await txn.read('tb_test_types', cols, None)
        print(time.time() - t0)

        t0 = time.time()
        for i in range(20):
            async with db.snapshot() as snp:
                await snp.read('tb_test_types', cols, None)
        print(time.time() - t0)


async def test_single_use():
    # make sure idempotent
    async with aspanner.Aspanner(*args) as db:
        cols = ('id', 'data_int', 'data_float', 'data_str', 'data_bool',
                'data_bytes', 'data_date', 'data_num', 'data_time',
                'data_array', 'data_json')

        print(await db.read('tb_test_types', cols, [(1,)]))
        print(await db.delete('tb_test_types', [(1,)]))
        print(await db.read('tb_test_types', cols, [(1,)]))
        data = await db.read('tb_test_types', cols, [(1,)])
        assert not data
        print(await db.insert('tb_test_types', ('id', 'data_int'), [(1, 999)]))
        print(await db.read('tb_test_types', cols, [(1,)]))
        data = await db.read('tb_test_types', cols, [(1,)])
        assert len(data) == 1
        assert data[0]['id'] == 1 and data[0]['data_int'] == 999
        print(await db.delete('tb_test_types', [(1,)]))
        print(await db.read('tb_test_types', cols, [(1,)]))
        data = await db.read('tb_test_types', cols, [(1,)])
        assert not data


async def test_example():
    db = aspanner.Aspanner(*args)

    cols = ('id', 'data_int')
    print(await db.insert('tb_test_types', cols, [(9, 999)]))
    print(await db.read('tb_test_types', cols, [(9,)]))
    print(await db.delete('tb_test_types', [(9,)]))

    print(await db.close())


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    async def test():
        await test_session()
        await test_pool()
        await test_transaction()
        await test_snapshot()
        await test_sql()
        await test_mutations()
        # await test_single_use_speed_compare()
        await test_single_use()
        await test_example()

    asyncio.run(test())
