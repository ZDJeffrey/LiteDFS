import sys
import os
import argparse

root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root)
import DFS_parameter

import threading
import shutil
from concurrent import futures
import grpc
import name_node_pb2 as n_pb2
import name_node_pb2_grpc as n_pb2_grpc
import data_node_pb2 as d_pb2
import data_node_pb2_grpc as d_pb2_grpc


class DataNode(d_pb2_grpc.DataNodeServicer):
    def __init__(self, ip, port, block_count):
        self.blocks = [True] * block_count
        self.ip_port = ip+':'+str(port)
        # 上线通知
        with grpc.insecure_channel(DFS_parameter.__name_node_ip_port__) as channel:
            stub = n_pb2_grpc.NameNodeStub(channel)
            stub.hello(n_pb2.HelloRequest(node=self.ip_port))
        if DFS_parameter.__verbose_message__:
            print(f'Data Node {self.ip_port} online')
        # 创建文件夹
        self.perfix = DFS_parameter.__data_node_storage__+self.ip_port.replace(':','_')+'/'
        if not os.path.exists(self.perfix):
            os.makedirs(self.perfix)
        
    def touch(self, request, context):
        block_id = next(i for i, block in enumerate(self.blocks) if block)
        self.blocks[block_id]=False
        if DFS_parameter.__verbose_message__:
            print(f'{self.ip_port}:>Create new block {block_id}')
        file = open(self.perfix+str(block_id),'w') # 创建块
        file.close()
        return d_pb2.TouchDataResponse(offset=block_id)
    
    def rm(self, request, context):
        block_ids = request.offsets
        for block_id in block_ids:
            if DFS_parameter.__verbose_message__:
                print(f'{self.ip_port}:>Delete block {block_id}')
            self.blocks[block_id]=True # 无需删除文件，后续会直接覆盖
        return d_pb2.EmptyMsg()
    
    def read(self, request, context):
        offset = request.offset
        with open(self.perfix+str(offset),'rb') as file:
            for chunk in iter(lambda: file.read(DFS_parameter.__chunk_size__), b''):
                yield d_pb2.ReadDataResponse(data=chunk)


    def write(self, request_iterator, context):
        offset = None
        for chunk in request_iterator:
            if offset is None:
                offset = chunk.offset
                file = open(self.perfix+str(offset),'wb')
            else:
                file.write(chunk.data)
        file.close()
        if DFS_parameter.__verbose_message__:
            print(f'{self.ip_port}:>Write block {offset}')
        return d_pb2.EmptyMsg()
    
    def exit(self):
        if os.path.exists(self.perfix):
            shutil.rmtree(self.perfix)
    
def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    data_node = DataNode(args.ip, args.port, args.block)
    d_pb2_grpc.add_DataNodeServicer_to_server(data_node ,server)
    server.add_insecure_port(data_node.ip_port)
    server.start()
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(0)
        data_node.exit()
        if DFS_parameter.__verbose_message__:
            print("Shutting down gracefully...")

parser = argparse.ArgumentParser(description='LiteDFS Data Node')
parser.add_argument('--ip', type=str, help='The ip address of data node', default='127.0.0.1')
parser.add_argument('--port', type=int, help='The port of data node to provide services', default=50052)
parser.add_argument('--block', type=int, help='The total number of block in the data node', default=1024)
args = parser.parse_args()
    
if __name__ == "__main__":
    serve()