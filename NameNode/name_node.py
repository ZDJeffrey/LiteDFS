import sys
import os

root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root)
import DFS_parameter

import random
import threading
from concurrent import futures
import grpc
from google.protobuf.timestamp_pb2 import Timestamp
import name_node_pb2 as n_pb2
import name_node_pb2_grpc as n_pb2_grpc

class FileNode:
    def __init__(self) -> None:
        self.ts = Timestamp()       # 时间戳
        self.ts.GetCurrentTime()
        self.node_ids = []          # 数据节点id
        self.offsets = []           # 数据节点中文件偏移量
        self.mtx = threading.Lock() # 读写锁
        self.r_count = 0            # 读数量
        self.w_lock = False         # 写锁


class NameNode(n_pb2_grpc.NameNodeServicer):
    def __init__(self) -> None:
        self.file_structure = {}    # 文件结构
        self.data_nodes = []        # 数据节点[ip:port]


    def search_dir(self, path_list):
        '''
        返回父级目录
        '''
        contents = self.file_structure
        for filename in path_list[:-1]:
            if filename+'/' in contents:
                contents = contents[filename+'/']
            else:
                return None
        if path_list[-1]+'/' in contents:
            return contents
        else:
            return None
            

    def search_file(self, path_list):
        '''
        返回父级目录
        '''
        contents = self.file_structure
        for filename in path_list[:-1]:
            if filename+'/' in contents:
                contents = contents[filename+'/']
            else:
                return None
        if path_list[-1] in contents:
            return contents
        else:
            return None
        
    
    def check_dir_for_lock(self, folder:dict)->bool:
        file_locked = [] # 用于回溯
        check = True # 检查结果
        for key, node in folder.items():
            if key[-1] == '/': # 文件夹
                if not self.check_dir_for_lock(node):
                    check = False
                    break
            else:
                with node.mtx: # 互斥
                    if node.w_lock or node.r_count > 0: # 无法删除
                        check = False
                        break
                    else: # 锁定用于删除
                        node.w_lock = True
                        file_locked.append(node)
        if not check: # 回溯
            for file in file_locked:
                with file.mtx:
                    file.w_lock = False
        return check
    

    def get_data_in_dir(self, folder:dict):
        data = {}
        for key, node in folder.items():
            if key[-1] == '/': # 文件夹
                sub_data = self.get_data_in_dir(node)
                for node_name, offset in sub_data.items():
                    if node_name in data:
                        data[node_name].extend(offset)
                    else:
                        data[node_name] = offset
            else:
                for node_id, offset in zip(node.node_ids,node.offsets):
                    node_name = self.data_nodes[node_id]
                    if node_name in data:
                        data[node_name].append(offset)
                    else:
                        data[node_name] = [offset]
        return data
    

    def touch(self, request, context):
        '''
        创建文件
        '''
        if len(self.data_nodes)==0:
            return n_pb2.TouchResponse(type=n_pb2.TouchResponseType.Value('TOUCH_FAIL'))
        # 解析路径
        path_list = request.path_list
        if len(path_list)==0:
            return n_pb2.TouchResponse(type=n_pb2.TouchResponseType.Value('TOUCH_INVALID'))
        elif len(path_list) == 1:
            folder = self.file_structure
        else: # 检查父文件夹是否存在
            folder = self.search_dir(path_list[:-1])
            if folder is None: # 父文件夹不存在
                return n_pb2.TouchResponse(type=n_pb2.TouchResponseType.Value('TOUCH_INVALID'))
            folder = folder[path_list[-2]+'/']
        # 检查文件是否存在
        if path_list[-1] in folder: 
            return n_pb2.TouchResponse(type=n_pb2.TouchResponseType.Value('TOUCH_EXIST'))
        # 创建文件
        folder[path_list[-1]] = FileNode()
        file = folder[path_list[-1]]
        with file.mtx:
            file.r_count = 0
            file.w_lock = True
            num = min(DFS_parameter.__max_copy_num__, len(self.data_nodes))
            file.node_ids = random.sample(range(len(self.data_nodes)), num) # 随机选择文件节点
        nodes = [ self.data_nodes[node_id] for node_id in file.node_ids ]
        if DFS_parameter.__verbose_message__:
            print(f'Cteate new file {path_list}, storage nodes:{nodes}')
        return n_pb2.TouchResponse(type=n_pb2.TouchResponseType.Value('TOUCH_SUCCESS'), ts=file.ts, nodes=nodes)


    def mkdir(self, request, context):
        path_list = request.path_list
        if len(path_list) == 0:
            return n_pb2.MkdirResponse(type=n_pb2.MkdirResponseType.Value('MKDIR_INVALID'))
        elif len(path_list) == 1:
            folder = self.file_structure
        else: # 检查父文件夹是否存在
            folder = self.search_dir(path_list[:-1])
            if folder is None: # 父文件夹不存在
                return n_pb2.MkdirResponse(type=n_pb2.MkdirResponseType.Value('MKDIR_INVALID'))
            folder = folder[path_list[-2]+'/']
        # 检查文件是否存在
        if path_list[-1]+'/' in folder: 
            return n_pb2.MkdirResponse(type=n_pb2.MkdirResponseType.Value('MKDIR_EXIST'))
        # 创建文件夹
        folder[path_list[-1]+'/'] = {}
        if DFS_parameter.__verbose_message__:
            print(f'Create new folder {path_list}')
        return n_pb2.MkdirResponse(type=n_pb2.MkdirResponseType.Value('MKDIR_SUCCESS'))


    def ls(self, request, context):
        path_list = request.path_list
        if len(path_list)==0: # 根文件夹
            folder = self.file_structure
        else:
            # 检查文件夹是否存在
            folder = self.search_dir(path_list)
            if folder is None: # 文件夹不存在
                return n_pb2.LsResponse(type=n_pb2.LsResponseType.Value('LS_INVALID'))
            folder = folder[path_list[-1]+'/']
        files = list(folder)
        return n_pb2.LsResponse(type=n_pb2.LsResponseType.Value('LS_SUCCESS'), files=files)
    

    def cd(self, request, context):
        path_list = request.path_list
        if len(path_list)==0: # 根文件夹
            return n_pb2.CdResponse(type=n_pb2.CdResponseType.Value('CD_SUCCESS'))
        folder = self.search_dir(path_list)
        if folder is None:
            return n_pb2.CdResponse(type=n_pb2.CdResponseType.Value('CD_INVALID'))
        return n_pb2.CdResponse(type=n_pb2.CdResponseType.Value('CD_SUCCESS'))


    def rm(self, request, context):
        path_list = request.path_list
        if len(path_list)==0:
            return n_pb2.RmResponse(type=n_pb2.RmResponseType.Value('RM_INVALID'))
        folder = self.search_file(path_list)
        if folder is None:
            return n_pb2.RmResponse(type=n_pb2.RmResponseType.Value('RM_INVALID'))
        file = folder[path_list[-1]]
        with file.mtx:
            if file.r_count > 0 or file.w_lock:
                return n_pb2.RmResponse(type=n_pb2.RmResponseType.Value('RM_LOCK'))
            file.w_lock = True
        node_offsets = []
        for node_id, offset in zip(file.node_ids, file.offsets):
            node_offsets.append(n_pb2.NodeOffsets(node=self.data_nodes[node_id], offsets=[offset]))
        del folder[path_list[-1]]
        if DFS_parameter.__verbose_message__:
            print(f'Delete file {path_list}')
        return n_pb2.RmResponse(type=n_pb2.RmResponseType.Value('RM_SUCCESS'), node_offsets=node_offsets)
    

    def rmdir(self, request, context):
        path_list = request.path_list
        if len(path_list)==0:
            return n_pb2.RmdirResponse(type=n_pb2.RmdirResponseType.Value('RMDIR_INVALID'))
        parent_folder = self.search_dir(path_list)
        if parent_folder is None:
            return n_pb2.RmdirResponse(type=n_pb2.RmdirResponseType.Value('RMDIR_INVALID'))
        folder = parent_folder[path_list[-1]+'/']
        if not self.check_dir_for_lock(folder):
            if DFS_parameter.__verbose_message__:
                print(f'Cannot delete folder {path_list}')
            return n_pb2.RmdirResponse(type=n_pb2.RmdirResponseType.Value('RMDIR_LOCK'))
        data = self.get_data_in_dir(folder)
        del parent_folder[path_list[-1]+'/']
        node_offsets = []
        for node, offsets in data.items():
            node_offsets.append(n_pb2.NodeOffsets(node=node, offsets=offsets))
        if DFS_parameter.__verbose_message__:
            print(f'delete folder {path_list}')
        return n_pb2.RmdirResponse(type=n_pb2.RmdirResponseType.Value('RMDIR_SUCCESS'), node_offsets=node_offsets)
        
        
    def open(self, request, context):
        path_list = request.path_list
        folder = self.search_file(path_list)
        if folder is None:
            return n_pb2.OpenResponse(type=n_pb2.OpenResponseType.Value('OPEN_INVALID'))
        file = folder[path_list[-1]]
        with file.mtx:
            if request.type == n_pb2.OpenRequestType.Value('OPEN_READ'):
                if file.w_lock == True:
                    return n_pb2.OpenResponse(type=n_pb2.OpenResponseType.Value('OPEN_LOCK'))
                file.r_count += 1
            elif request.type == n_pb2.OpenRequestType.Value('OPEN_WRITE'):
                if file.w_lock == True or file.r_count > 0:
                    return n_pb2.OpenResponse(type=n_pb2.OpenResponseType.Value('OPEN_LOCK'))
                file.w_lock = True
            index = random.choice(range(len(file.node_ids))) # 随机选取一个节点
            node = self.data_nodes[file.node_ids[index]] # 节点地址
            offset = file.offsets[index] # 节点文件偏移量
            if DFS_parameter.__verbose_message__:
                mode = 'read' if request.type == n_pb2.OpenRequestType.Value('OPEN_READ') else 'write'
                print(f'open file {path_list},mode:{mode},timestamp:{file.ts},read node:{node},offset:{offset}')
            return n_pb2.OpenResponse(type=n_pb2.OpenResponseType.Value('OPEN_SUCCESS'), ts=file.ts, node=node, offset=offset)
                

    def closeTouch(self, request, context):
        path_list = request.path_list
        offsets = request.offsets
        folder = self.search_file(path_list)
        if folder is None:
            return n_pb2.CloseResponse(type=n_pb2.CloseResponseType.Value('CLOSE_INVALID'))
        file = folder[path_list[-1]]
        with file.mtx:
            if file.w_lock == False:
                return n_pb2.CloseResponse(type=n_pb2.CloseResponseType.Value('CLOSE_FAIL'))
            file.w_lock=False
            file.offsets=offsets
            if DFS_parameter.__verbose_message__:
                print(f'New file {path_list} is created successfully, offsets:{offsets}')
            return n_pb2.CloseResponse(type=n_pb2.CloseResponseType.Value('CLOSE_SUCCESS'))


    def closeRead(self, request, context):
        path_list = request.path_list
        folder = self.search_file(path_list)
        if folder is None:
            return n_pb2.CloseResponse(type=n_pb2.CloseResponseType.Value('CLOSE_INVALID'))
        file = folder[path_list[-1]]
        with file.mtx:
            if file.w_lock==True or file.r_count <= 0:
                return n_pb2.CloseResponse(type=n_pb2.CloseResponseType.Value('CLOSE_FAIL'))
            file.r_count -= 1
            if DFS_parameter.__verbose_message__:
                print(f'file {path_list} read end, timestamp:{file.ts}, read lock:{file.r_count}, write lock:{file.w_lock}')
        return n_pb2.CloseResponse(type=n_pb2.CloseResponseType.Value('CLOSE_SUCCESS'))
    

    def beginWrite(self, request, context):
        path_list = request.path_list
        folder = self.search_file(path_list)
        if folder is None:
            return n_pb2.CloseResponse(type=n_pb2.CloseResponseType.Value('CLOSE_INVALID'))
        file = folder[path_list[-1]]
        with file.mtx:
            if file.w_lock==False or file.r_count > 0:
                return n_pb2.CloseResponse(type=n_pb2.CloseResponseType.Value('CLOSE_FAIL'))
        file.ts.GetCurrentTime()
        nodes = [ self.data_nodes[node_id] for node_id in file.node_ids ]
        offsets = file.offsets
        if DFS_parameter.__verbose_message__:
            print(f'file {path_list}begin to write to {nodes}, timestamp:{file.ts}, read lock:{file.r_count}, write lock:{file.w_lock}, offsets:{file.offsets}')
        return n_pb2.BeginWriteResponse(type=n_pb2.CloseResponseType.Value('CLOSE_SUCCESS'), ts=file.ts, nodes=nodes, offsets=offsets)
        

    def closeWrite(self, request, context):
        path_list = request.path_list
        folder = self.search_file(path_list)
        if folder is None:
            return n_pb2.CloseResponse(type=n_pb2.CloseResponseType.Value('CLOSE_INVALID'))
        file = folder[path_list[-1]]
        with file.mtx:
            if file.w_lock==False or file.r_count > 0:
                return n_pb2.CloseResponse(type=n_pb2.CloseResponseType.Value('CLOSE_FAIL'))
            file.w_lock=False
            if DFS_parameter.__verbose_message__:
                print(f'file {path_list} write end, timestamp:{file.ts}, read lock:{file.r_count}, write lock:{file.w_lock}')
        return n_pb2.CloseResponse(type=n_pb2.CloseResponseType.Value('CLOSE_SUCCESS'))
    
    def hello(self, request, context):
        node = request.node
        if node not in self.data_nodes:
            self.data_nodes.append(node) # 新增节点
            if DFS_parameter.__verbose_message__:
                print(f'{node} data node online')
        return n_pb2.HelloResponse()

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    n_pb2_grpc.add_NameNodeServicer_to_server(NameNode() ,server)
    server.add_insecure_port(DFS_parameter.__name_node_ip_port__)
    server.start()
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(0)
        if DFS_parameter.__verbose_message__:
            print("Shutting down gracefully...")

if __name__ == "__main__":
    serve()