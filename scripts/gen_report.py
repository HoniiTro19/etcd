#!/usr/bin/env python3
import os
import re
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
        _, self.stdout, stderr = self.server.exec_command(cmd)
        # TODO stderr readlines blocks
        # err = stderr.readlines()
        # if len(err):
        #     raise Exception('get stderr when loading file in remote server', err)
        # return self
        
    def __next__(self):
        line = self.stdout.readline()
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
        self.server.connect(self.host, 22, self.user, pkey=mykey, timeout=100000)
        
    def execute_cmd(self, cmd):
        _, stdout, stderr = self.server.exec_command(cmd)
        # TODO stderr readlines blocks
        # err = stderr.readlines()
        # if len(err):
        #     raise Exception('get stderr when checking directory in remote server', err)
        return stdout.readlines()

    def stop_server(self):
        self.server.close()

#################################################### UTILS ################################################
# For each round of experiment, which has the same log file location in the cluster peers, 
# we need to count the average time a leader to take over and the cumulative duration when the cluster has no master.
def gen_elect_result(peers, subdir, leadkill):
    npeers = len(peers)
    # start remote servers to read log files
    rss = [RemoteServer(peers[i], os.path.join(subdir, '{}'.format(i+1), 'election.log')) for i in range(npeers)]
    for i in range(npeers):
        rss[i].start_server()
        rss[i] = iter(rss[i]) 

    '''
        initialize log queues, timestamps and counters
        leader: whether leader is on service
        stepdownts: the lastest timestamp of leader steps down (shutdown or lose connection to majority)
        startts: the timestamp of election experiment starts (first raft instance start service)
        stopts: the timestamp of election experiment stops (last raft instance stop service)
        ttr: time to repair the cluster service when N raft instances are killed (N is set to 1 in this version)
        takeovercnt: the times of new leader take over
        noleader: the length of timestamp window without a leader on service
        tryelect: the times to start elections
    '''
    leader, stepdownts, startts, stopts = None, 0, 0, 0
    ttr, takeovercnt, noleader, tryelect = 0, 0, 0, 0
    warning, mocknetstat, msgstat = set(), "", ""
    try:
        globaljs = [load_legal_row(rs, 0, warning) for rs in rss]
    except StopIteration:
        msg = 'get empty log file in peers'
        print(msg)
        return 0, 0, 0, 0, 0, warning, mocknetstat, msgstat

    done = npeers
    while done > 0:
        js, idx = find_mints_log(npeers, globaljs)
        # jot down the state changes of the raft cluster
        if js['level'] == 'warn':
            warning.add(js['msg'])
        elif js['msg'] == 'mock net terminates with packets statistics':
            mocknetstat += json.dumps(js) + '\n'
        elif js['msg'] == 'raft instance start service':
            if startts == 0:
                startts = js['ts']
                stepdownts = js['ts']
        elif js['msg'] == 'raft instance stop service':
            stopts = js['ts']
            msgstat += json.dumps(js) + '\n'
            if leader == idx:
                stepdownts = js['ts']
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
                warning.add(msg)
                leader = idx

            # A leader is killed when stopts > 0
            if leadkill and ttr == 0 and stopts > 0:
                ttr = js['ts'] - stopts
                print('ttr', js['ts'], stopts, ttr)
                stepdownts = js['ts']
                stopts = js['ts']
                break
        elif js['msg'] == 'start a new election':
            tryelect += 1

        try:
            globaljs[idx] = load_legal_row(rss[idx], globaljs[idx]['ts'], warning)
        except StopIteration:
            done -= 1
            rss[idx].stop_server()
            rss[idx] = None
            globaljs[idx]['ts'] = sys.maxsize

    for rs in rss:
        if rs is not None:
            rs.stop_server()

    noleader += stopts - stepdownts
    duration = stopts - startts

    return noleader, duration, takeovercnt, ttr, tryelect, warning, mocknetstat, msgstat

# Generate the statistics results in all sub directories
def gen_group_elect_results(peers, dir, leadkill):
    # get all results subdir under the given directory
    rs = RemoteServer(peers[0], dir)
    rs.start_server()
    subdirs = rs.execute_cmd('ls ' + rs.path)
    subdirs = [subdir.strip('\n') for subdir in subdirs]
    rs.stop_server()

    # generate reports for all sub directories
    noleadrates, durations, takeovercnts, avgtakeovers, ttrs, tryelects \
        = [], [], [], [], [], []
    mocknetstats, msgstats = [], []
    for subdir in subdirs:
        path = os.path.join(dir, subdir)
        noleader, duration, takeovercnt, ttr, tryelect, warning, mocknetstat, msgstat \
            = gen_elect_result(peers, path, leadkill)
        if leadkill and ttr == 0:
            continue
        noleadrates.append(noleader / duration)
        durations.append(duration)
        takeovercnts.append(takeovercnt)
        avgtakeovers.append(noleader / takeovercnt if takeovercnt > 0 else 0)
        ttrs.append(ttr)
        tryelects.append(tryelect)
        mocknetstats.append(mocknetstat)
        msgstats.append(msgstat)
        if len(warning) > 0:
            print(path)
            for warn in warning:
                print('Warn: {0}'.format(warn))
    print(dir)
    print("noleadrates", noleadrates)
    print("durations", durations)
    print("takeovercnts", takeovercnts)
    print("avgtakeovers", avgtakeovers)
    print("ttrs", ttrs)
    print("tryelects", tryelects)
    return noleadrates, ttrs
    
# Boxplot the no leader ratios and the time to repair after downtime of a group of experiments
def gen_graph(title, tags, ylabels, data):
    print("get valid samples of each experiment {0}".format([len(items) for items in data[0]]))
    fig, axes = plt.subplots(nrows=1, ncols=len(data))
    print(data)
    print(ylabels)
    print(tags)
    for i in range(len(data)):
        bplot = axes[i].boxplot(data[i], notch=True, vert=True, patch_artist=True)
        axes[i].yaxis.grid(True)
        axes[i].set_xlabel(title)
        axes[i].set_ylabel(ylabels[i])
        whiskers = [item.get_ydata()[1] for item in bplot['whiskers']]
        axes[i].set_ylim(min(whiskers)*0.8, max(whiskers)*1.1)

    fig.tight_layout()
    plt.subplots_adjust(wspace=0.5)
    plt.setp(axes, xticks=[i+1 for i in range(len(tags))], xticklabels=tags)
    plt.savefig("{0}.png".format(title))

################################################### UTILS ################################################
# Read the next log file with 'member' tag from given remote server, 
# and ensure the timestamp satisfies a restriction of linear growth
def load_legal_row(rs, globalts, warning):
    if rs is None: 
        return
    js = json.loads(next(rs))
    if js['level'] == 'warn':
        warning.add(js['msg'])
    while 'member' not in js:
        js = json.loads(next(rs))
    if js['ts'] < globalts:
        msg = 'timestamp ({0}) fallback in the log row: {1}'.format(globalts, js)
        warning.add(msg)

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
    parser.add_argument('title', help='description of this group of experiments')
    parser.add_argument('dirs', help='result directory to generate reports')
    parser.add_argument('peers', help='comma-separated and ordered ssh addresses of raft instances')
    parser.add_argument('leadkill', type=bool, help='experiments to test the time to repair')
    args = parser.parse_args()
    dirs = args.dirs.split(',')
    peers = args.peers.split(',')

    # generate results
    if len(dirs) > 1:
        noleadrates, ttrs, tags = [], [], []
        pattern = r"\d+\.?\d*"
        for dir in dirs:
            noleadrate, ttr = gen_group_elect_results(peers, dir, args.leadkill)
            noleadrates.append(noleadrate)
            ttrs.append(ttr)
            res = re.findall(pattern, dir)
            assert len(res) == 1
            tags.append(res[0])
        gen_graph(args.title, tags, \
                  ['no leader ratio (%)', 'time to repair (s)'], \
                  [noleadrates, ttrs])
    else:
        noleadrate, ttr = gen_group_elect_results(peers, dirs[0], args.leadkill)
        print('no leader ratios', noleadrate)
        print('time to repairs', ttr)
