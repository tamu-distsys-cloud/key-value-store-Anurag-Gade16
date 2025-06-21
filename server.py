### SERVER CODE - ANURAG GADE


import logging
import threading
from typing import Tuple, Any

debugging = False

def debug(format, *args):
    if debugging:
        logging.info(format % args)

class PutAppendArgs:
    def __init__(self, key, value):
        self.key = key
        self.value = value
        self.client_id = None
        self.seq_num = None

class PutAppendReply:
    def __init__(self, value):
        self.value = value

class GetArgs:
    def __init__(self, key):
        self.key = key
        self.client_id = None
        self.seq_num = None

class GetReply:
    def __init__(self, value):
        self.value = value

class KVServer:
    def __init__(self, cfg):
        self.mu = threading.Lock()
        self.cfg = cfg
        self.data = {}
        self.client_results = {}
        self.server_id = None
        self.nreplicas = getattr(cfg, 'nreplicas', 1)

    def Get(self, args: GetArgs):
        with self.mu:

            # here finding the server
            if self.server_id is None:
                for i, server in enumerate(self.cfg.kvservers):
                    if server is self:
                        self.server_id = i
                        break
                if self.server_id is None:
                    return GetReply(self.data.get(args.key, ""))

            # responsblity check
            if self.nreplicas > 1:
                if hasattr(self.cfg, 'running_servers') and self.server_id not in self.cfg.running_servers:
                    return GetReply("")
                
                num_servers = len(self.cfg.kvservers)
                key_shard = int(args.key) % num_servers
                is_responsible = any((key_shard + i) % num_servers == self.server_id for i in range(self.nreplicas))
                if not is_responsible:
                    return GetReply("")
            
            # duplicate request check
            if hasattr(args, 'client_id') and args.client_id is not None:
                if args.client_id in self.client_results:
                    cached_seq, cached_result = self.client_results[args.client_id]
                    if args.seq_num == cached_seq:
                        return GetReply(cached_result)
            
            value = self.data.get(args.key, "")
            if hasattr(args, 'client_id') and args.client_id is not None:
                self.client_results[args.client_id] = (args.seq_num, value)
            
            return GetReply(value)

    def Put(self, args: PutAppendArgs):
        with self.mu:
            if self.server_id is None:
                for i, server in enumerate(self.cfg.kvservers):
                    if server is self:
                        self.server_id = i
                        break
                if self.server_id is None:
                    self.data[args.key] = args.value
                    return PutAppendReply(None)
            
            if self.nreplicas > 1:
                if hasattr(self.cfg, 'running_servers') and self.server_id not in self.cfg.running_servers:
                    return PutAppendReply(None)
                
                total_servers = len(self.cfg.kvservers)
                shard = int(args.key) % total_servers
                is_responsible = any((shard + i) % total_servers == self.server_id for i in range(self.nreplicas))
                if not is_responsible:
                    return PutAppendReply(None)
                
                # forwarding to primary
                primary_id = shard
                if self.server_id != primary_id:
                    try:
                        if hasattr(self.cfg, 'kvservers') and primary_id < len(self.cfg.kvservers):
                            primary_server = self.cfg.kvservers[primary_id]
                            forwarded_result = primary_server.Put(args)
                            if forwarded_result is not None:
                                return forwarded_result
                    except:
                        pass
            
            if hasattr(args, 'client_id') and args.client_id is not None:
                if args.client_id in self.client_results:
                    cached_seq, cached_result = self.client_results[args.client_id]
                    if args.seq_num == cached_seq:
                        return PutAppendReply(cached_result)
            
            self.data[args.key] = args.value
            
            if hasattr(args, 'client_id') and args.client_id is not None:
                self.client_results[args.client_id] = (args.seq_num, None)
            
            return PutAppendReply(None)

    def Append(self, args: PutAppendArgs):
        with self.mu:
            if self.server_id is None:
                for i, server in enumerate(self.cfg.kvservers):
                    if server is self:
                        self.server_id = i
                        break
                if self.server_id is None:
                    old_value = self.data.get(args.key, "")
                    self.data[args.key] = old_value + args.value
                    return PutAppendReply(old_value)
            
            if self.nreplicas > 1:
                if hasattr(self.cfg, 'running_servers') and self.server_id not in self.cfg.running_servers:
                    return PutAppendReply("")
                
                total_servers = len(self.cfg.kvservers)
                shard = int(args.key) % total_servers
                is_responsible = any((shard + i) % total_servers == self.server_id for i in range(self.nreplicas))
                if not is_responsible:
                    return PutAppendReply("")
                
                primary_id = shard
                if self.server_id != primary_id:
                    try:
                        if hasattr(self.cfg, 'kvservers') and primary_id < len(self.cfg.kvservers):
                            primary_server = self.cfg.kvservers[primary_id]
                            forwarded_result = primary_server.Append(args)
                            if forwarded_result is not None:
                                return forwarded_result
                    except:
                        pass
            
            if hasattr(args, 'client_id') and args.client_id is not None:
                if args.client_id in self.client_results:
                    cached_seq, cached_result = self.client_results[args.client_id]
                    if args.seq_num == cached_seq:
                        return PutAppendReply(cached_result)
            
            old_value = self.data.get(args.key, "")
            self.data[args.key] = old_value + args.value
            
            if hasattr(args, 'client_id') and args.client_id is not None:
                self.client_results[args.client_id] = (args.seq_num, old_value)
            
            return PutAppendReply(old_value)
        
# Referred to Perplexity for responsibility checks for keys, and to refine duplicate checking in server.py. Referred perplexity for help to implement sharding checks in client.py