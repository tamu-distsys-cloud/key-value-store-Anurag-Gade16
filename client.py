### CLIENT CODE - ANURAG GADE

import random
import threading
from typing import Any, List
import time

from labrpc.labrpc import ClientEnd
from server import GetArgs, GetReply, PutAppendArgs, PutAppendReply

def nrand() -> int:
    return random.getrandbits(62)

class Clerk:
    def __init__(self, servers: List[ClientEnd], cfg):
        self.servers = servers
        self.cfg = cfg
        
        self.client_id = nrand()
        self.seq_num = 0
        self.mu = threading.Lock()

    def get(self, key: str) -> str:
        with self.mu:
            self.seq_num += 1
            seq = self.seq_num
        
        args = GetArgs(key)
        args.client_id = self.client_id
        args.seq_num = seq
        
        # sharding check
        if hasattr(self.cfg, 'nreplicas') and self.cfg.nreplicas > 1:
            num_shards = len(self.servers)
            shard = int(key) % num_shards
            server_list = [(shard + i) % num_shards for i in range(self.cfg.nreplicas)]
            
            while True:
                for srv_id in server_list:
                    if srv_id < len(self.servers):
                        try:
                            reply = self.servers[srv_id].call("KVServer.Get", args)
                            if reply and hasattr(reply, 'value'):
                                return reply.value
                        except:
                            pass
                time.sleep(0.1)
        else:
            while True:
                for i in range(len(self.servers)):
                    try:
                        reply = self.servers[i].call("KVServer.Get", args)
                        if reply and hasattr(reply, 'value'):
                            return reply.value
                    except:
                        pass
                time.sleep(0.1)

    def put_append(self, key: str, value: str, op: str) -> str:
        with self.mu:
            self.seq_num += 1
            seq = self.seq_num
        
        args = PutAppendArgs(key, value)
        args.client_id = self.client_id
        args.seq_num = seq
        
        if hasattr(self.cfg, 'nreplicas') and self.cfg.nreplicas > 1:
            num_shards = len(self.servers)
            shard = int(key) % num_shards
            server_list = [(shard + i) % num_shards for i in range(self.cfg.nreplicas)]
            while True:
                for srv_id in server_list:
                    if srv_id < len(self.servers):
                        try:
                            reply = self.servers[srv_id].call(f"KVServer.{op}", args)
                            if reply and hasattr(reply, 'value'):
                                return reply.value if reply.value is not None else ""
                        except:
                            pass
                time.sleep(0.1)
        else:
            while True:
                for i in range(len(self.servers)):
                    try:
                        reply = self.servers[i].call(f"KVServer.{op}", args)
                        if reply and hasattr(reply, 'value'):
                            return reply.value if reply.value is not None else ""
                    except:
                        pass
                time.sleep(0.1)

    def put(self, key: str, value: str):
        self.put_append(key, value, "Put")


    def append(self, key: str, value: str) -> str:
        return self.put_append(key, value, "Append")