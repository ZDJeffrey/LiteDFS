import os
import sys

__verbose_message__=False

__client_cache__=os.path.dirname(os.path.abspath(__file__))+'/Cache/' # 缓存模拟目录

__name_node_ip_port__='127.0.0.1:50051' # NameNode服务器

__max_copy_num__=3 # 最大副本数量
__data_node_storage__=os.path.dirname(os.path.abspath(__file__))+'/Storage/' # 缓存模拟目录

__chunk_size__ = 1024*1024 # 文件传输块大小


root = os.path.dirname(os.path.abspath(__file__))
sys.path.append(root+'/NameNode/proto/')
sys.path.append(root+'/DataNode/proto/')
