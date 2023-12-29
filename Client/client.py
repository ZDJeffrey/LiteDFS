import sys
import os
import shutil
import uuid

root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root)
import DFS_parameter

import grpc
from google.protobuf.timestamp_pb2 import Timestamp
import name_node_pb2 as n_pb2
import name_node_pb2_grpc as n_pb2_grpc


class Client:
    def __init__(self, name:str) -> None:
        self.path = '/' # 命令行当前路径
        self.perfix = os.path.join(DFS_parameter.__client_cache__+name+'/') # 缓存文件夹
        if not os.path.exists(self.perfix): # 创建缓存文件夹
            os.makedirs(self.perfix)
        self.name = name # 客户名称
        self.ip_port = DFS_parameter.__name_node_ip_port__
        self.file_cache = {} # 文件缓存列表{filename: }
        self.channel = grpc.insecure_channel(self.ip_port)
        self.stub = n_pb2_grpc.NameNodeStub(self.channel)

    def path_split(self, path):
        if len(path)==0 or path[0] != '/':
            path = self.path + path
        try:
            path = os.path.normpath(path) # 规范化路径
            path = path.replace('\\', '/') # 统一使用'/'
            return [file for file in path.split('/') if file]
        except Exception as e:
            print('Invalid path')
            return None
        
    def search_dir_in_cache(self, path_list):
        '''
        返回父级目录
        '''
        contents = self.file_cache
        for filename in path_list[:-1]:
            if filename+'/' in contents:
                contents = contents[filename+'/']
            else:
                return None
        if path_list[-1]+'/' in contents:
            return contents
        else:
            return None
            
    def search_file_in_cache(self, path_list):
        '''
        返回父级目录
        '''
        contents = self.file_cache
        for filename in path_list[:-1]:
            if filename+'/' in contents:
                contents = contents[filename+'/']
            else:
                return None
        if path_list[-1] in contents:
            return contents
        else:
            return None
        
    def delete_dir_in_cache(self, parent_folder:dict, folder_name:str):
        if folder_name+'/' not in parent_folder:
            return
        folder = parent_folder[folder_name+'/']
        for folder_key in folder.keys():
            if folder_key[-1] == '/': # 文件夹
                self.delete_dir_in_cache(folder, folder_key[:-1])
            else: # 文件
                os.remove(self.perfix + folder[folder_key]['id'])

        
    def touch(self, path):
        path_list = self.path_split(path) # 解析路径
        if path_list is None:
            return
        # gRPC
        response = self.stub.touch(n_pb2.PathRequest(path_list=path_list))
        if response.type == n_pb2.TouchResponseType.Value('TOUCH_INVALID'):
            print('Invalid path')
        elif response.type == n_pb2.TouchResponseType.Value('TOUCH_EXIST'):
            print('File already exist')
        elif response.type == n_pb2.TouchResponseType.Value('TOUCH_SUCCESS'):
            # TODO: 存储节点创建新文件
            nodes = response.nodes
            offsets = []
            ts = response.ts
            response = self.stub.closeTouch(n_pb2.CloseTouchRequest(path_list=path_list, offsets=offsets))
            if response.type != n_pb2.CloseResponseType.Value('CLOSE_SUCCESS'):
                print('Touch Fault')
                return
            # 修改cache逻辑结构
            folder = self.file_cache
            for folder_name in path_list[:-1]:
                if folder_name+'/' not in folder:
                    folder[folder_name+'/'] = {}
                folder = folder[folder_name+'/']
            folder[path_list[-1]] = {}
            file = folder[path_list[-1]]
            file['id'] = str(uuid.uuid5(uuid.NAMESPACE_URL, '/'.join(path_list))) # 文件id
            file['ts'] = ts # 时间戳
            # 添加缓存文件
            file = open(self.perfix + file['id'], 'w')
            file.close()


    def mkdir(self, path):
        path_list = self.path_split(path) # 解析路径
        if path_list is None:
            return
        if len(path_list) == 0:
            print('Invalid path')
            return
        # gRPC
        response = self.stub.mkdir(n_pb2.PathRequest(path_list=path_list))
        if response.type == n_pb2.MkdirResponseType.Value('MKDIR_INVALID'):
            print('Invalid path')
        elif response.type == n_pb2.MkdirResponseType.Value('MKDIR_EXIST'):
            print('Folder already exist')
        elif response.type == n_pb2.MkdirResponseType.Value('MKDIR_SUCCESS'):
            print('Folder created')


    def ls(self, path):
        path_list = self.path_split(path) # 解析路径
        if path_list is None:
            return
        response = self.stub.ls(n_pb2.PathRequest(path_list=path_list))
        if response.type == n_pb2.LsResponseType.Value('LS_INVALID'):
            print('Invalid path')
        elif response.type == n_pb2.LsResponseType.Value('LS_SUCCESS'):
            files = response.files
            print(' '.join(files))


    def cd(self, path):
        path_list = self.path_split(path) # 解析路径
        if path_list is None:
            return
        response = self.stub.cd(n_pb2.PathRequest(path_list=path_list))
        # TODO: 检查目录是否存在
        if response.type == n_pb2.CdResponseType.Value('CD_SUCCESS'):
            if len(path_list) == 0:
                self.path = '/'
            else:
                self.path = '/'+'/'.join(path_list)+'/'
        else:
            print('Directory not exist')


    def rm(self, path):
        path_list = self.path_split(path) # 解析路径
        if path_list is None:
            return
        response = self.stub.rm(n_pb2.PathRequest(path_list=path_list))
        if response.type == n_pb2.RmResponseType.Value('RM_INVALID'):
            print('Invalid path')
        elif response.type == n_pb2.RmResponseType.Value('RM_LOCK'):
            print('File cannot be removed')
        elif response.type == n_pb2.RmResponseType.Value('RM_SUCCESS'):
            node_offsets = response.node_offsets
            # TODO: 删除文件
            
            folder = self.search_file_in_cache(path_list) # 存在本地缓存
            if folder is not None: # 删除缓存
                file = folder[path_list[-1]]
                file_path = self.perfix + file['id']
                os.remove(file_path)
                del folder[path_list[-1]]


    def rmdir(self, path):
        print(f"Removing directory {path}")
        path_list = self.path_split(path) # 解析路径
        if path_list is None: # 无效路径
            return
        if len(path_list) == 0: # 根目录
            print('Invalid path')
            return
        response = self.stub.rmdir(n_pb2.PathRequest(path_list=path_list))
        if response.type == n_pb2.RmdirResponseType.Value('RMDIR_INVALID'):
            print('Invalid path')
        elif response.type == n_pb2.RmdirResponseType.Value('RMDIR_LOCK'):
            print('Folder cannot be removed')
        elif response.type == n_pb2.RmdirResponseType.Value('RMDIR_SUCCESS'):
            node_offsets = response.node_offsets
            # TODO: 删除文件夹内所有文件

            # 删除缓存
            folder = self.search_dir_in_cache(path_list)
            if folder is not None:
                self.delete_dir_in_cache(folder, path_list[-1])
                del folder[path_list[-1]+'/']



    def open(self, path, mode):
        if mode not in ['read', 'write']:
            print('Invalid mode')
            return None
        path_list = self.path_split(path) # 解析路径
        if path_list is None:
            return None
        response = self.stub.open(n_pb2.OpenRequest(type=n_pb2.OpenRequestType.Value('OPEN_'+mode.upper()),path_list=path_list))
        if response.type == n_pb2.OpenResponseType.Value('OPEN_INVALID'):
            print('Invalid path')
        elif response.type == n_pb2.OpenResponseType.Value('OPEN_LOCK'):
            print('File is locked')
        elif response.type == n_pb2.OpenResponseType.Value('OPEN_SUCCESS'):
            folder = self.search_file_in_cache(path_list)
            if folder is not None: # 本地缓存
                file = folder[path_list[-1]]
                if file['ts'] == response.ts: # 缓存未过期
                    open_mode = 'r' if mode=='read' else 'r+'
                    # test
                    cache = open(self.perfix + file['id'], open_mode)
                    cache.close()
                    # return open(self.perfix + file['id'], open_mode)
            else: # 创建缓存
                # TODO: 根据节点获取文件
                node = response.node
                offset = response.offset
                file_content = '123'
                # 修改cache逻辑结构
                folder = self.file_cache
                for folder_name in path_list[:-1]:
                    if folder_name+'/' not in folder:
                        folder[folder_name+'/'] = {}
                    folder = folder[folder_name+'/']
                folder[path_list[-1]] = {}
                file = folder[path_list[-1]]
                file['id'] = str(uuid.uuid5(uuid.NAMESPACE_URL, '/'.join(path_list))) # 文件id
                file['ts'] = response.ts # 时间戳
                # 添加缓存文件
                cache = open(self.perfix + file['id'], 'w')
                cache.write(file_content) # 写入内容
                # Test
                cache.close()
                # return cache
        return None


    def close(self, path, mode):
        path_list = self.path_split(path) # 解析路径
        if path_list is None:
            return
        if mode in ['read', 'write']:
            response = getattr(self.stub, 'close'+mode.capitalize())(n_pb2.PathRequest(path_list=path_list))
            if response.type == n_pb2.CloseResponseType.Value('CLOSE_INVALID'):
                print('Invalid path')
            elif response.type == n_pb2.CloseResponseType.Value('CLOSE_FAIL'):
                print('Close fail')
            else:
                if mode=='write': # 更新缓存时间戳
                    folder = self.search_file_in_cache(path_list)
                    if folder is None:
                        print('Write fault')
                        return
                    folder[path_list[-1]]['ts']=response.ts
        else:
            print('Invalid mode')
            return
    

    def exit(self):
        self.channel.close()
        shutil.rmtree(self.perfix) # 删除缓存文件夹
        print('Bye')


def run(client: Client):
    while(True):
        cmd = input(f'{client.name}@{client.ip_port}{client.path}$ ').split()
        if len(cmd) == 0: # 空指令
            continue
        if len(cmd) == 1: # ls, exit
            cmd = cmd[0]
            if cmd == 'ls':
                client.ls('')
            elif cmd == 'exit':
                client.exit()
                break
        elif len(cmd) == 2: # ls, cd, rm, touch, mkdir, rmdir, close
            cmd, path = cmd
            if cmd in ['ls', 'cd', 'rm', 'touch', 'mkdir', 'rmdir']:
                getattr(client, cmd)(path)
            else:
                print('Invalid command')
        elif len(cmd) == 3: # open, close
            cmd, path, mode = cmd
            if cmd in ['open', 'close']:
                getattr(client, cmd)(path, mode)
            else:
                print('Invalid command')
        else:
            print('Invalid command')


if __name__ == "__main__":
    client = Client('jeffrey')
    run(client)