#!/usr/bin/env bash

if [ $# -ne 8 ] ; then
    echo "Usage: $(basename $0) CODE_DIR LOG_DIR PORT LATENCY TIMES DEV FUNC USE_MOCK_NET" >&2
    exit 2
fi

# start_raft(description, duration, latency, mocknet, msgloss, msgdelay, leadkill)
function start_raft() {    
    for i in ${!sshaddrs[@]}; do
        echo "start raft instance on server ${i}"
        echo "./bin/contrib/election --id ${i} --desc $1 --duration $2ns --latency ${LATENCY} --mocknet=$3 --msgloss $4 --msgdelay $5 --leadkill=$6 --basedir ${LOG_DIR} --cluster ${CLUSTER}"
        ssh -n -o ConnectTimeout=10 ${sshaddrs[i]} "
        cd ${CODE_DIR} && ./bin/contrib/election --id ${i} --desc $1 --duration $2ns --latency ${LATENCY} --mocknet=$3 --msgloss $4 --msgdelay $5 --leadkill=$6 --basedir ${LOG_DIR} --cluster ${CLUSTER}
        " &
        if [ $? -ne 0 ]; then
            echo "ERROR: fail to start raft instance on server ${i}"
        fi
    done
    wait
}

start_tc_loss() {
    for i in ${!sshaddrs[@]}; do
        echo "create tc queueing discipline on server ${i}"
        ssh -n -o ConnectTimeout=10 ${sshaddrs[i]} "
        sudo tc qdisc add dev ${DEV} root handle 1: prio priomap 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 &&
        sudo tc qdisc add dev ${DEV} parent 1:2 handle 20: netem loss $1 &&
        sudo tc filter add dev ${DEV} parent 1:0 protocol ip u32 match ip dport ${PORT} 0xffff flowid 1:2
        "
        if [ $? -ne 0 ]; then
            echo "ERROR: fail to create tc queueing discipline on server ${i}"
        fi
    done
}

start_tc_delay() {
    for i in ${!sshaddrs[@]}; do
        echo "create tc queueing discipline on server ${i}"
        ssh -n -o ConnectTimeout=10 ${sshaddrs[i]} "
        sudo tc qdisc add dev ${DEV} root handle 1: prio priomap 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 &&
        sudo tc qdisc add dev ${DEV} parent 1:2 handle 20: netem delay $1ms &&
        sudo tc filter add dev ${DEV} parent 1:0 protocol ip u32 match ip dport ${PORT} 0xffff flowid 1:2
        "
        if [ $? -ne 0 ]; then
            echo "ERROR: fail to create tc queueing discipline on server ${i}"
        fi
    done
}

clean_up_tc() {
    for i in ${!sshaddrs[@]}; do
        echo "clean up tc queueing discipline on server ${i}"
        ssh -n -o ConnectTimeout=10 ${sshaddrs[i]} "
        sudo tc qdisc del dev ${DEV} root
        "
        if [ $? -ne 0 ]; then
            echo "ERROR: fail to clean up tc queueing discipline on server ${i}"
        fi
    done
}

start_msgloss_mrrt() {
    for loss in ${msgloss[@]}; do
        if [ $1 == false ] 
            then start_tc_loss ${loss}
        fi
        for i in {1..${TIMES}}; do
            start_raft "msgloss${loss}mrrt" 180 $1 ${loss} 0 true
        done
        if [ $1 == false ] 
            then clean_up_tc
        fi
    done
}

start_msgdelay_mrrt() {
    for delay in ${msgdelay[@]}; do
        if [ $1 == false ]
            then start_tc_delay ${delay}
        fi
        for i in {1..${TIMES}}; do
            start_raft "msgdelay${loss}mrrt" 180 $1 0 ${delay} true
        done
        if [ $1 == false ] 
            then clean_up_tc
        fi
    done
}

start_msgloss_noleader() {
    for loss in ${msgloss[@]}; do
        if [ $1 == false ]
            then start_tc_loss loss
        fi
        for i in {1..${TIMES}}; do
            start_raft "msgloss${loss}noleader" 1800 $1 ${loss} 0 false
        done
        if [ $1 == false ] 
            then clean_up_tc
        fi
    done
}

CODE_DIR=$1; shift
LOG_DIR=$1; shift
PORT=$1; shift
LATENCY=$1; shift
TIMES=$1; shift
DEV=$1; shift

msgloss=(0 5 10 15 20 30 40 50)
msgdelay=(1 100 200 300 400)

host=(`cat hostname.txt`)
npeers=`expr ${#host[@]} / 3`
for ((i=0; i<${npeers}; i++)); do
    clusters[${host[i*3]}]="http://${host[i*3+2]}:${PORT}"
    sshaddrs[${host[i*3]}]="${host[i*3+1]}@${host[i*3+2]}"
done
CLUSTER=$(IFS=,; echo "${clusters[*]}")

if declare -f ${1} > /dev/null
then 
    "$@"
else 
    echo "Error: ${1} is not a known function name, Options: [start_msgloss_mrrt|start_msgdelay_mrrt|start_msgloss_noleader]" >&2
    exit 1
fi
