#!/usr/bin/env python3
import os
import argparse
import json
import paramiko
import sys
import matplotlib.pyplot as plt

class RemoteServer:
    '''
        A ssh server that functions as the file reader iterator or linux command executor
    '''
    def __init__(self, addr, path, prkey='~/.ssh/id_rsa'):    
        addrs = addr.split('@')
        self.user = addrs[0]
        self.host = addrs[1]
        self.path = path
        self.prkey = prkey
        
    def __iter__(self):
        cmd = 'cat ' + self.path
        _, self.iterout, stderr = self.server.exec_command(cmd)
        err = stderr.readlines()
        if len(err):
            raise Exception('get stderr when loading file in remote server', err)
        return self
        
    def __next__(self):
        line = self.iterout.readline()
        if line:
            return line
        else:
            self.server.close()
            raise StopIteration
        
    def start_server(self):
        self.server = paramiko.SSHClient()
        self.server.load_system_host_keys()
        self.server.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        privatekeyfile = os.path.expanduser(self.prkey)
        mykey = paramiko.RSAKey.from_private_key_file(privatekeyfile)
        self.server.connect(self.host, 22, self.user, pkey=mykey, timeout=5)
        
    def execute_cmd(self, cmd):
        _, stdout, stderr = self.server.exec_command(cmd)
        err = stderr.readlines()
        if len(err):
            raise Exception('get stderr when checking directory in remote server', err)
        return stdout.readlines()

 ################################################### UTILS ################################################
# For each round of experiment, which has the same log file location in the cluster peers, 
# we need to count the average time a leader to take over and the cumulative duration when the cluster has no master.
def gen_elect_result(peers, subdir):
    npeers = len(peers)
    # start remote servers to read log files
    rss = [RemoteServer(peers[i], os.path.join(subdir, '{}'.format(i+1), 'election.log')) for i in range(npeers)]
    for i in range(npeers):
        rss[i].start_server()
        rss[i] = iter(rss[i]) 
    
    # initialize log queues, timestamps and counters
    leader, stepdownts = None, 0
    startts, stopts, ttr, takeovercnt, noleader = 0, 0, 0, 0, 0
    warning, mocknetstat, msgstat = [], "", ""
    try:
        globaljs = [load_legal_row(rs, 0) for rs in rss]
    except StopIteration:
        raise Exception('get empty log file in peers')

    done = npeers
    while done > 0:
        js, idx = find_mints_log(npeers, globaljs)
        # jot down the state changes of the raft cluster
        if js['level'] == 'warn':
            warning.append(json.dumps(js))
        elif js['msg'] == 'raft instance start service':
            if startts == 0:
                startts = js['ts']
                stepdownts = startts
        elif js['msg'] == 'raft instance stop service':
            stopts = js['ts']
            msgstat += json.dumps(js) + '\n'
            if leader == idx:
                stepdownts = stopts
                leader = None
        elif js['msg'] == 'become follower':
            if leader == idx:
                stepdownts = js['ts']
                leader = None                         
        elif js['msg'] == 'become leader':
            if leader is None:
                takeovercnt += 1
                noleader += js['ts'] - stepdownts
                leader = idx
            else:
                msg = 'illegal for peer {0} to take over while leader {1} still in lease'.format(idx, leader)
                raise Exception(msg)
            if ttr == 0 and stopts > 0:
                ttr = js['ts'] - stopts
        elif js['msg'] == 'mock net terminates with packets statistics':
            mocknetstat += json.dumps(js) + '\n'
        
        try:
            globaljs[idx] = load_legal_row(rss[idx], globaljs[idx]['ts'])            
        except StopIteration: 
            done -= 1
            rss[idx] = None
            globaljs[idx]['ts'] = sys.maxsize
    
    # noleader += stopts - stepdownts 
    
    return noleader / (stopts - startts), takeovercnt, noleader / takeovercnt, ttr, \
            warning, mocknetstat, msgstat

# Generate the statistics results in all sub directories
def gen_group_elect_results(peers, dir):
    # get all results subdir under the given directory
    rs = RemoteServer(peers[0], dir)
    rs.start_server()
    subdirs = rs.execute_cmd('ls ' + rs.path)
    subdirs = [subdir.strip('\n') for subdir in subdirs]
    
    # generate reports for all sub directories
    noleadrates, takeovercnts, avgtakeovers, ttrs = [], [], [], []
    mocknetstats, msgstats = [], []
    for subdir in subdirs:
        path = os.path.join(dir, subdir)
        noleadrate, takeovercnt, avgtakeover, ttr, warning, mocknetstat, msgstat = gen_elect_result(peers, path)
        noleadrates.append(noleadrate)
        takeovercnts.append(takeovercnt)
        avgtakeovers.append(avgtakeover)
        ttrs.append(ttr)
        mocknetstats.append(mocknetstat)
        msgstats.append(msgstat)
        if len(warning) > 0:
            print(path)
            for warn in warning:
                print('warning: {0}'.format(warn))
                
    return noleadrates, ttrs, mocknetstats, msgstats
    
# Boxplot the no leader ratios and the time to repair after downtime of a group of experiments
def gen_graph(noleadrates, ttrs, mocknetstats, msgstats):
    fig, axes = plt.subplots(nrows=1, ncols=2, figsize=(9,4))
    bplot1 = axes[0].boxplot()
    bplot2 = axes[1].boxplot()
    
################################################### FUNCTIONS ################################################
# Read the next log file with 'member' tag from given remote server, 
# and ensure the timestamp satisfies a restriction of linear growth
def load_legal_row(rs, globalts):
    if rs is None: 
        return
    js = json.loads(next(rs))
    while 'member' not in js:
        js = json.loads(next(rs))
    if js['ts'] < globalts:
        raise ValueError('timestamp ({0}) fallback in the log row: {1}'.format(globalts, js))
    return js

def find_mints_log(npeers, globaljs):
    # find the log with the latest timestamp
    mints, idx = sys.maxsize, 0
    for i in range(npeers):
        if globaljs[i]['ts'] < mints:
            mints = globaljs[i]['ts']
            idx = i
    js = globaljs[idx]
    
    return js, idx

if __name__ == "__main__":
    # parse arguments, all of which should be passed in by users 
    description = 'generate reports for single or several election experiments'
    parser = argparse.ArgumentParser(description)
    parser.add_argument('dirs', help='the result directory to generate reports')
    parser.add_argument('peers', help='the comma-separated and ordered ssh addresses of raft instances')
    args = parser.parse_args()
    dirs = args.dirs.split(',')
    peers = args.peers.split(',')
    
    for dir in dirs:
        noleadrates, ttrs, mocknetstats, msgstats = gen_group_elect_results(peers, dir)
        print(noleadrates)
        print(ttrs)
        print(mocknetstats)
        print(msgstats)