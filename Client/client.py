import sys
import os
import shutil
import uuid
import subprocess
import tempfile
import argparse

root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root)
import DFS_parameter

import grpc
from google.protobuf.timestamp_pb2 import Timestamp
import name_node_pb2 as n_pb2
import name_node_pb2_grpc as n_pb2_grpc
import data_node_pb2 as d_pb2
import data_node_pb2_grpc as d_pb2_grpc


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

    def upload_file(self, offset, file_path):
        yield d_pb2.WriteDataRequest(offset=offset)
        with open(file_path, 'rb') as file:
            for chunk in iter(lambda: file.read(DFS_parameter.__chunk_size__), b''):
                yield d_pb2.WriteDataRequest(data=chunk)

        
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
        elif response.type == n_pb2.TouchResponseType.Value('TOUCH_FAIL'):
            print('No Data Node')
        elif response.type == n_pb2.TouchResponseType.Value('TOUCH_SUCCESS'):
            offsets = []
            ts = response.ts
            # 存储节点创建新文件
            for node in response.nodes:
                with grpc.insecure_channel(node) as channel:
                    stub = d_pb2_grpc.DataNodeStub(channel)
                    response = stub.touch(d_pb2.EmptyMsg())
                    offsets.append(response.offset)
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
        # 检查目录是否存在
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
            # 删除文件
            for node_offset in node_offsets:
                node = node_offset.node
                offsets = node_offset.offsets
                with grpc.insecure_channel(node) as channel:
                    stub = d_pb2_grpc.DataNodeStub(channel)
                    stub.rm(d_pb2.RmDataRequest(offsets=offsets))
            folder = self.search_file_in_cache(path_list) # 存在本地缓存
            if folder is not None: # 删除缓存
                file = folder[path_list[-1]]
                file_path = self.perfix + file['id']
                os.remove(file_path)
                del folder[path_list[-1]]
                if DFS_parameter.__verbose_message__:
                    print('Remove cache')
            if DFS_parameter.__verbose_message__:
                print('Rm success')


    def rmdir(self, path):
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
            # 删除文件夹内所有文件
            for node_offset in node_offsets:
                node = node_offset.node
                offsets = node_offset.offsets
                with grpc.insecure_channel(node) as channel:
                    stub = d_pb2_grpc.DataNodeStub(channel)
                    stub.rm(d_pb2.RmDataRequest(offsets=offsets))
            # 删除缓存
            folder = self.search_dir_in_cache(path_list)
            if folder is not None:
                self.delete_dir_in_cache(folder, path_list[-1])
                del folder[path_list[-1]+'/']

            if DFS_parameter.__verbose_message__:
                print('Rmdir success')


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
                    if DFS_parameter.__verbose_message__:
                        print('Use cache')
                    return self.perfix + file['id']
                if DFS_parameter.__verbose_message__:
                    print('Cache expire')
            else: # 创建缓存
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
                # 根据节点获取文件写入缓存
                node = response.node
                offset = response.offset
                with grpc.insecure_channel(node) as channel:
                    stub = d_pb2_grpc.DataNodeStub(channel)
                    response_iterator = stub.read(d_pb2.ReadDataRequest(offset=offset))
                    with open(self.perfix + file['id'], 'wb') as cache:
                        for response in response_iterator:
                            cache.write(response.data)
                return self.perfix + file['id']
        return None


    def close(self, path, mode):
        path_list = self.path_split(path) # 解析路径
        if path_list is None:
            return
        if mode == 'read':
            response = self.stub.closeRead(n_pb2.PathRequest(path_list=path_list))
            if response.type == n_pb2.CloseResponseType.Value('CLOSE_INVALID'):
                print('Invalid path')
            elif response.type == n_pb2.CloseResponseType.Value('CLOSE_FAIL'):
                print('Read close fail')
            else:
                if DFS_parameter.__verbose_message__:
                    print('Read close success')
        elif mode == 'write':
            # 开始写入
            response = self.stub.beginWrite(n_pb2.PathRequest(path_list=path_list))
            if response.type == n_pb2.CloseResponseType.Value('CLOSE_INVALID'):
                print('Invalid path')
            elif response.type == n_pb2.CloseResponseType.Value('CLOSE_FAIL'):
                print('Write close fail')
            else:
                folder = self.search_file_in_cache(path_list)
                if folder is None:
                    print('Write fault')
                    return
                folder[path_list[-1]]['ts']=response.ts
                # 写入文件至存储节点
                nodes = response.nodes
                offsets = response.offsets
                for node, offset in zip(nodes, offsets):
                    with grpc.insecure_channel(node) as channel:
                        stub = d_pb2_grpc.DataNodeStub(channel)
                        upload = self.upload_file(offset, self.perfix + folder[path_list[-1]]['id'])
                        stub.write(upload)
                # 结束写入
                response = self.stub.closeWrite(n_pb2.PathRequest(path_list=path_list))
                if response.type == n_pb2.CloseResponseType.Value('CLOSE_INVALID'):
                    print('Invalid path')
                elif response.type == n_pb2.CloseResponseType.Value('CLOSE_FAIL'):
                    print('Write close fail')
                else:
                    if DFS_parameter.__verbose_message__:
                        print('Write close success')
        else:
            print('Invalid mode')
            return
    

    def exit(self):
        self.channel.close()
        shutil.rmtree(self.perfix) # 删除缓存文件夹
        print('Bye')

    
    def cat(self, path):
        file_addr = self.open(path, 'read')
        if file_addr is None:
            return
        with open(file_addr, 'r') as file:
            file_content = file.read()
        print(file_content)
        self.close(path, 'read')
    

    def notepad(self, path):
        file_addr = self.open(path, 'write')
        if file_addr is None:
            return
        # 打开记事本
        subprocess.run(['notepad.exe', file_addr])
        self.close(path, 'write')


    def vim(self, path, mode=None):
        if mode is None:
            file_addr = self.open(path, 'write')
            if file_addr is None:
                return
            # 打开vim
            subprocess.run(['vim.exe', file_addr])
            self.close(path, 'write')
        elif mode == '-M':
            file_addr = self.open(path, 'read')
            if file_addr is None:
                return
            # 打开vim
            subprocess.run(['vim.exe', '-M', file_addr])
            self.close(path, 'read')
        else:
            print('Invalid mode')
            return

    def help(self, cmd=None):
        if cmd is None:
            print("""
                General Help

                Usage: command [options] [arguments]

                Available Commands:
                ls             List contents of a directory.
                cd             Change the current working directory.
                rm             Remove file.
                touch          Create an empty file.
                mkdir          Create a new directory.
                rmdir          Remove an directory.
                cat            Display the contents of a file.
                notepad        Open a file in Notepad.
                vim            Open a file in the Vim text editor.
                help           Display help information for a specific command.

                Usage Examples:
                ls /path/to/directory
                cd /path/to/directory
                rm /path/to/myfile.txt
                touch myfile.txt
                mkdir new_directory
                rmdir /path/to/directory
                cat myfile.txt
                notepad myfile.txt
                vim myfile.txt
                help ls

                For detailed help on each command, use 'help' followed by the command name:
                help ls
                help cd
                """)
        elif cmd == 'ls':
            print("""
                ls Command

                Usage: ls [directory]
                Description: List the contents of the specified directory. If no directory is provided, list the contents of the current directory.
                Example: ls /path/to/directory
                """)
        elif cmd == 'cd':
            print("""
                cd Command

                Usage: cd <directory>
                Description: Change the current working directory to the specified directory.
                Example: cd /path/to/directory
                """)
        elif cmd == 'rm':
            print("""
                rm Command

                Usage: rm [options] file
                Description: Remove the specified file.
                Example: rm /path/to/myfile.txt
                """)
        elif cmd == 'touch':
            print("""
                touch Command

                Usage: touch <filename>
                Description: Create an empty file with the specified filename.
                Example: touch myfile.txt
                """)
        elif cmd == 'mkdir':
            print("""
                mkdir Command

                Usage: mkdir <directory>
                Description: Create a new directory with the specified name.
                Example: mkdir myfile.txt
                """)
        elif cmd == 'rmdir':
            print("""
                rmdir Command

                Usage: rmdir <directory>
                Description: Remove the specified directory.
                Example: rmdir directory
                """)
        elif cmd == 'cat':
            print("""
                cat Command

                Usage: cat <file>
                Description: Display the contents of the specified file.
                Example: cat myfile.txt
                """)
        elif cmd == 'notepad':
            print("""
                notepad Command

                Usage: notepad <file>
                Description: Open the specified file in the Notepad text editor.
                Example: notepad myfile.txt
                """)
        elif cmd == 'vim':
            print("""
                vim Command

                Usage: vim <file>
                Description: Open the specified file in the Vim text editor.
                Options:
                -M  Open file in read-only mode.
                Example: vim myfile.txt
                         vim myfile.txt -M
                """)
        elif cmd == 'help':
            print("""
                help Command

                Usage: help [command]
                Description: Display help information for the specified command. If no command is provided, show a list of available commands.
                Example: help ls
                """)
        else:
            print(f"Error: Unknown command '{cmd}'. Use 'help' for a list of available commands.")


def run():
    client = Client(args.username)
    while(True):
        try:
            cmd = input(f'{client.name}@{client.ip_port}{client.path}$ ').strip().split()
            if len(cmd) == 0: # 空指令
                continue
            if len(cmd) == 1: # ls, exit
                cmd = cmd[0]
                if cmd == 'ls':
                    client.ls('')
                elif cmd == 'exit':
                    client.exit()
                    break
                elif cmd == 'help':
                    client.help()
                else:
                    print('Invalid command')
            elif len(cmd) == 2: # ls, cd, rm, touch, mkdir, rmdir, cat, notepad, vim, help
                cmd, path = cmd
                if cmd in ['ls', 'cd', 'rm', 'touch', 'mkdir', 'rmdir', 'cat', 'notepad', 'vim', 'help']:
                    getattr(client, cmd)(path)
                else:
                    print('Invalid command')
            elif len(cmd) == 3: # TEST:open, close
                cmd, path, mode = cmd
                if cmd in ['open', 'close', 'vim']:
                    getattr(client, cmd)(path, mode)
                else:
                    print('Invalid command')
            else:
                print('Invalid command')
        except KeyboardInterrupt:
            client.exit()
            break


parser = argparse.ArgumentParser(description='LiteDFS Client')
parser.add_argument('--username', type=str, help='The user\'s name shown in CLI', default='user')
args = parser.parse_args()

if __name__ == "__main__":
    run()
    