# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

import data_node_pb2 as data__node__pb2


class DataNodeStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.touch = channel.unary_unary(
                '/DataNode/touch',
                request_serializer=data__node__pb2.EmptyMsg.SerializeToString,
                response_deserializer=data__node__pb2.TouchDataResponse.FromString,
                )
        self.rm = channel.unary_unary(
                '/DataNode/rm',
                request_serializer=data__node__pb2.RmDataRequest.SerializeToString,
                response_deserializer=data__node__pb2.EmptyMsg.FromString,
                )
        self.write = channel.stream_unary(
                '/DataNode/write',
                request_serializer=data__node__pb2.WriteDataRequest.SerializeToString,
                response_deserializer=data__node__pb2.EmptyMsg.FromString,
                )
        self.read = channel.unary_stream(
                '/DataNode/read',
                request_serializer=data__node__pb2.ReadDataRequest.SerializeToString,
                response_deserializer=data__node__pb2.ReadDataResponse.FromString,
                )


class DataNodeServicer(object):
    """Missing associated documentation comment in .proto file."""

    def touch(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def rm(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def write(self, request_iterator, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def read(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_DataNodeServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'touch': grpc.unary_unary_rpc_method_handler(
                    servicer.touch,
                    request_deserializer=data__node__pb2.EmptyMsg.FromString,
                    response_serializer=data__node__pb2.TouchDataResponse.SerializeToString,
            ),
            'rm': grpc.unary_unary_rpc_method_handler(
                    servicer.rm,
                    request_deserializer=data__node__pb2.RmDataRequest.FromString,
                    response_serializer=data__node__pb2.EmptyMsg.SerializeToString,
            ),
            'write': grpc.stream_unary_rpc_method_handler(
                    servicer.write,
                    request_deserializer=data__node__pb2.WriteDataRequest.FromString,
                    response_serializer=data__node__pb2.EmptyMsg.SerializeToString,
            ),
            'read': grpc.unary_stream_rpc_method_handler(
                    servicer.read,
                    request_deserializer=data__node__pb2.ReadDataRequest.FromString,
                    response_serializer=data__node__pb2.ReadDataResponse.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'DataNode', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class DataNode(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def touch(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/DataNode/touch',
            data__node__pb2.EmptyMsg.SerializeToString,
            data__node__pb2.TouchDataResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def rm(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/DataNode/rm',
            data__node__pb2.RmDataRequest.SerializeToString,
            data__node__pb2.EmptyMsg.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def write(request_iterator,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.stream_unary(request_iterator, target, '/DataNode/write',
            data__node__pb2.WriteDataRequest.SerializeToString,
            data__node__pb2.EmptyMsg.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def read(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_stream(request, target, '/DataNode/read',
            data__node__pb2.ReadDataRequest.SerializeToString,
            data__node__pb2.ReadDataResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)
