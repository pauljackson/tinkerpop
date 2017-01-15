import sys
import concurrent.futures
import uuid
from threading import Thread

import pytest
from six.moves import queue
from tornado import ioloop, gen

from gremlin_python.driver.client import Client
from gremlin_python.driver.request import RequestMessage
from gremlin_python.driver.connection import Connection
from gremlin_python.driver.protocol import GremlinServerWSProtocol
from gremlin_python.driver.remote.driver_remote_connection import (
    DriverRemoteConnection)
from gremlin_python.driver.tornado.transport import TornadoTransport
from gremlin_python.structure.graph import Graph


def test_connection():
    protocol = GremlinServerWSProtocol()
    executor = concurrent.futures.ThreadPoolExecutor(5)
    pool = queue.Queue()
    conn = Connection('ws://localhost:8182/gremlin', 'g', protocol,
                      lambda: TornadoTransport(), executor, pool, '', '')
    g = Graph().traversal()
    t = g.V()
    message = RequestMessage('traversal', 'bytecode', {'gremlin': t.bytecode})
    results_set = conn.write(message).result(1)
    future = results_set.all()
    results = future.result()
    assert len(results) == 6
    assert isinstance(results, list)
    conn.close()
    executor.shutdown()

def test_client():
    client = Client('ws://localhost:8182/gremlin', 'g')
    g = Graph().traversal()
    t = g.V()
    message = RequestMessage('traversal', 'bytecode', {'gremlin': t.bytecode})
    result_set = client.submit(message)
    assert len(result_set.all().result()) == 6
    client.close()

def test_iterate_result_set():
    client = Client('ws://localhost:8182/gremlin', 'g')
    g = Graph().traversal()
    t = g.V()
    message = RequestMessage('traversal', 'bytecode', {'gremlin': t.bytecode})
    result_set = client.submit(message)
    results = []
    for result in result_set:
        results += result
    assert len(results) == 6
    client.close()

def test_client_async():
    client = Client('ws://localhost:8182/gremlin', 'g')
    g = Graph().traversal()
    t = g.V()
    message = RequestMessage('traversal', 'bytecode', {'gremlin': t.bytecode})
    future = client.submitAsync(message)
    assert not future.done()
    result_set = future.result()
    assert len(result_set.all().result()) == 6
    client.close()

def test_connection_share():
    client = Client('ws://localhost:8182/gremlin', 'g', pool_size=1)
    g = Graph().traversal()
    t = g.V()
    message = RequestMessage('traversal', 'bytecode', {'gremlin': t.bytecode})
    future = client.submitAsync(message)
    future2 = client.submitAsync(message)

    result_set2 = future2.result()
    assert len(result_set2.all().result()) == 6

    # This future has to finish for the second to yield result - pool_size=1
    assert future.done()
    result_set = future.result()
    assert len(result_set.all().result()) == 6
    client.close()

def test_multi_conn_pool():
    client = Client('ws://localhost:8182/gremlin', 'g', pool_size=2)
    g = Graph().traversal()
    t = g.V()
    message = RequestMessage('traversal', 'bytecode', {'gremlin': t.bytecode})
    future = client.submitAsync(message)
    future2 = client.submitAsync(message)

    result_set2 = future2.result()
    assert len(result_set2.all().result()) == 6

    # with connection pool `future` may or may not be done here
    result_set = future.result()
    assert len(result_set.all().result()) == 6
    client.close()

def test_driver_remote_conn():
    conn = DriverRemoteConnection(
        'ws://localhost:8182/gremlin', 'g', pool_size=4)
    g = Graph().traversal().withRemote(conn)
    t = g.V()
    assert len(t.toList()) == 6
    conn.close()

def test_promise():
    conn = DriverRemoteConnection(
        'ws://localhost:8182/gremlin', 'g', pool_size=4)
    g = Graph().traversal().withRemote(conn)
    future = g.V().promise()
    t = future.result()
    assert len(t.toList()) == 6
    conn.close()

def test_clients_in_threads_with_promise():
    q = queue.Queue()
    child = Thread(target=_executor, args=(q, None))
    child2 = Thread(target=_executor, args=(q, None))
    child.start()
    child2.start()
    for x in range(2):
        success = q.get()
        assert success == 'success!'
    child.join()
    child2.join()

def test_client_in_threads_with_promise():
    q = queue.Queue()
    conn = DriverRemoteConnection(
        'ws://localhost:8182/gremlin', 'g', pool_size=4)
    child = Thread(target=_executor, args=(q, conn))
    child2 = Thread(target=_executor, args=(q, conn))
    child.start()
    child2.start()
    for x in range(2):
        success = q.get()
        assert success == 'success!'
    child.join()
    child2.join()
    conn.close()

def _executor(q, conn):
    if not conn:
        conn = DriverRemoteConnection(
            'ws://localhost:8182/gremlin', 'g', pool_size=4)
    try:
        g = Graph().traversal().withRemote(conn)
        future = g.V().promise()
        t = future.result()
        assert len(t.toList()) == 6
    except:
        q.put(sys.exc_info()[0])
    else:
        q.put('success!')
        conn.close()

def test_in_tornado_app():

    @gen.coroutine
    def go():
        conn = DriverRemoteConnection(
            'ws://localhost:8182/gremlin', 'g', pool_size=4)
        g = Graph().traversal().withRemote(conn)
        yield gen.sleep(0)
        assert len(g.V().toList()) == 6
        conn.close()

    io_loop = ioloop.IOLoop.current()
    io_loop.run_sync(go)


def test_side_effects():
    conn = DriverRemoteConnection(
        'ws://localhost:8182/gremlin', 'g', pool_size=4)
    g = Graph().traversal().withRemote(conn)
    t = g.V().aggregate('a').aggregate('b')
    t.toList()

    # The 'a' key should return some side effects
    results = t.side_effects.get('a')
    assert results

    # Close result is None
    results = t.side_effects.close()
    assert not results

    # Shouldn't get any new info from server
    # 'b' isn't in local cache
    results = t.side_effects.get('b')
    assert not results

    # But 'a' should still be cached locally
    results = t.side_effects.get('a')
    assert results

    # 'a' should have been added to local keys cache, but not 'b'
    results = t.side_effects.keys()
    assert len(results) == 1
    a, = results
    assert a == 'a'

    # Try to get 'b' directly from server, should throw error
    with pytest.raises(Exception):
        t.side_effects.get('b')
        connection.close()

def test_side_effect_keys():
    conn = DriverRemoteConnection(
        'ws://localhost:8182/gremlin', 'g', pool_size=4)
    g = Graph().traversal().withRemote(conn)
    t = g.V().aggregate('a')
    t.toList()
    result, = t.side_effects.keys()
    assert result == 'a'
