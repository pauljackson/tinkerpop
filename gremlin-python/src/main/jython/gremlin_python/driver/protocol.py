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
import abc
import collections
import json
import uuid

import six

from gremlin_python.driver import serializer


@six.add_metaclass(abc.ABCMeta)
class AbstractBaseProtocol:

    @abc.abstractmethod
    def connection_made(self, transport):
        self._transport = transport

    @abc.abstractmethod
    def data_received(self, message):
        pass

    @abc.abstractmethod
    def write(self, request_id, request_message):
        pass


class GremlinServerWSProtocol(AbstractBaseProtocol):

    def __init__(self, message_serializer=None):
        if message_serializer is None:
            message_serializer = serializer.GraphSON2MessageSerializer()
        self._message_serializer = message_serializer

    def connection_made(self, transport):
        super(GremlinServerWSProtocol, self).connection_made(transport)

    def write(self, request_id, request_message):
        message = self._message_serializer.serialize_message(
            request_id, request_message)
        self._transport.write(message)

    def data_received(self, data, results_dict):
        data = json.loads(data.decode('utf-8'))
        request_id = data['requestId']
        result_set = results_dict[request_id]
        status_code = data['status']['code']
        aggregate_to = data['result']['meta'].get('aggregateTo', 'list')
        result_set._aggregate_to = aggregate_to
        if status_code == 407:
            self._authenticate(request_id)
            data = self._transport.read()
            self.data_received(data, results_dict)
        elif status_code == 204:
            # result_set.stream.put_nowait([None])
            result_set.done.set_result(None)
            del results_dict[request_id]
        elif status_code in [200, 206]:
            results = []
            for msg in data["result"]["data"]:
                results.append(
                    self._message_serializer.deserialize_message(msg))
            result_set.stream.put_nowait(results)
            if status_code == 206:
                data = self._transport.read()
                self.data_received(data, results_dict)
            else:
                result_set.done.set_result(None)
                del results_dict[request_id]
        else:
            result_set.stream.put_nowait(GremlinServerError(
                "{0}: {1}".format(status_code, data["status"]["message"])))
            result_set.done.set_result(None)
            del results_dict[request_id]

    def _authenticate(self, request_id):
        auth = b''.join([b'\x00', self.username.encode('utf-8'),
                         b'\x00', self.password.encode('utf-8')])
        args = {'sasl': base64.b64encode(auth).decode(),
                'saslMechanism': 'PLAIN'}
        message = self._message_serializer.serialize_message(
            request_id, '', 'authentication', **args)
        self._transport.write(message)
