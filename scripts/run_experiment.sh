#!/usr/bin/env bash

if [ $# -ne 5 ] ; then
    echo "Usage: $(basename $0) CODE_DIR LOG_DIR PORT FUNC USE_MOCK_NET" >&2
    exit 2
fi

# start_raft(description, duration, latency, mocknet, msgloss, msgdelay, leadkill)
function start_raft() {    
    for i in ${!sshaddrs[@]} ; do
        echo "start raft instance on server ${i}..."
        echo "./bin/contrib/election --id ${i} --desc $1 --duration $2ns --latency $3 --mocknet=$4 --msgloss $5 --msgdelay $6 --leadkill=$7 --basedir ${LOG_DIR} --cluster ${cluster}"
        ssh -n -o ConnectTimeout=10 ${sshaddrs[i]} "
        cd ${CODE_DIR} && ./bin/contrib/election --id ${i} --desc $1 --duration $2ns --latency $3 --mocknet=$4 --msgloss $5 --msgdelay $6 --leadkill=$7 --basedir ${LOG_DIR} --cluster ${cluster}
        " &
    done
    wait
}

# maximum round trip message latency over the network
latency=1000
msgloss=(0 5 10 15 20 30 40 50)
msgdelay=(1 100 200 300 400)

start_tc_loss() {
    echo "not supported"
}

start_tc_delay() {
    echo "not supported"
}

start_msgloss_mrrt() {
    if [ ${1} == false ]
        then start_tc_loss
    fi
    for loss in ${msgloss[@]}; do
        for i in {1..9}; do
            start_raft "msgloss${loss}mrrt" 60 ${latency} ${1} ${loss} 0 true
        done
    done
}

start_msgdelay_mrrt() {
    if [ ${1} == false ]
        then start_tc_delay
    fi
    for delay in ${msgdelay[@]}; do
        for i in {1..10}; do
            start_raft "msgdelay${loss}mrrt", 60, ${latency}, ${1}, 0, ${delay}, true
        done
    done
}

start_msgloss_noleader() {
    if [ ${1} == false ]
        then start_tc_loss
    fi
    for loss in ${msgloss[@]}; do
        for i in {1..10}; do
            start_raft "msgloss${loss}noleader", 600, ${latency}, ${1}, ${loss}, 0, false
        done
    done
}

CODE_DIR=$1; shift
LOG_DIR=$1; shift
PORT=$1; shift

port=12379
host=(`cat hostname.txt`)
npeers=`expr ${#host[@]} / 3`
for ((i=0; i<${npeers}; i++)); do
    clusters[${host[i*3]}]="http://${host[i*3+2]}:${port}"
    sshaddrs[${host[i*3]}]="${host[i*3+1]}@${host[i*3+2]}"
    port=$[${port}+10000]
done
cluster=$(IFS=,; echo "${clusters[*]}")

if declare -f ${1} > /dev/null
then 
    "$@"
else 
    echo "Error: ${1} is not a known function name, Options: [start_msgloss_mrrt|start_msgdelay_mrrt|start_msgloss_noleader]" >&2
    exit 1
fi
