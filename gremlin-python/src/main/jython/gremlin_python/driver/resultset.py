"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""
from concurrent.futures import Future


class ResultSet:

    def __init__(self, stream, request_id):
        self._stream = stream
        self._request_id = request_id
        self._done = Future()
        self._aggregate_to = None

    @property
    def request_id(self):
        return self._request_id

    @property
    def stream(self):
        return self._stream

    def __iter__(self):
        return self

    def __next__(self):
        result = self.one()
        if not result:
            raise StopIteration
        return result

    def next(self):
        return self.__next__()

    @property
    def done(self):
        return self._done

    def one(self):
        if self.stream.empty() and self.done.done():
            return
        result = self.stream.get()
        return result

    def all(self):
        future = Future()

        def cb(f):
            try:
                f.result()
            except Exception as e:
                future.set_exception(e)
            else:
                results = []
                while not self.stream.empty():
                    results += self.stream.get_nowait()
                future.set_result(results)

        self.done.add_done_callback(cb)
        return future
