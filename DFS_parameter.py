import os
import sys

__name_node_ip_port__='127.0.0.1:50051'
__client_cache__=os.path.dirname(os.path.abspath(__file__))+'/Cache/'

root = os.path.dirname(os.path.abspath(__file__))
sys.path.append(root+'/NameNode/proto/')
sys.path.append(root+'/DataNode/proto/')
