#!/usr/bin/env bash

if [ $# -ne 11 ] ; then
    echo "Usage: $(basename $0) CODE_DIR LOG_DIR PORT LATENCY TIMES DEV PARALLEL FUNC ROUND USE_MOCK_NET DURATION" >&2
    exit 2
fi

# start_raft(description, duration, mocknet, msgloss, msgdelay, leadkill, round)
start_raft() {
    for i in ${!sshaddrs[@]}; do
        echo "start raft instance on server ${i}"
        echo "./bin/contrib/election --id ${i} --desc $1 --duration $2ns --latency ${LATENCY} --mocknet=$3 --msgloss $4 --msgdelay $5 --leadkill=$6 --basedir ${LOG_DIR} --cluster ${CLUSTER} --round $7"
        ssh -n -o ConnectTimeout=${TIME_OUT} ${sshaddrs[i]} "
        cd ${CODE_DIR} && ./bin/contrib/election --id ${i} --desc $1 --duration $2ns --latency ${LATENCY} --mocknet=$3 --msgloss $4 --msgdelay $5 --leadkill=$6 --basedir ${LOG_DIR} --cluster ${CLUSTER} --round $7
        " &
        if [ $? -ne 0 ]; then
            echo "ERROR: fail to start raft instance on server ${i}"
        fi
    done
}

# start_tc_filter(port)
start_tc_filter() {
    echo "create tc filter on port $1"
    for i in ${!sshaddrs[@]}; do
        ssh -n -o ConnectTimeout=${TIME_OUT} ${sshaddrs[i]} "
        sudo tc filter add dev ${DEV} protocol ip parent 1:0 prio 4 u32 match ip sport $1 0xffff flowid 1:4 &&
        sudo tc filter add dev ${DEV} protocol ip parent 1:0 prio 4 u32 match ip dport $1 0xffff flowid 1:4
        "
        if [ $? -ne 0 ]; then
            echo "ERROR: fail to create tc filter on server ${i} port $1"
        fi
    done
}

# start_tc_loss(msgloss)
start_tc_loss() {
     for i in ${!sshaddrs[@]}; do
         echo "create tc queueing discipline on server ${i}"
         ssh -n -o ConnectTimeout=${TIME_OUT} ${sshaddrs[i]} "
         sudo tc qdisc add dev ${DEV} root handle 1: prio bands 4 &&
         sudo tc qdisc add dev ${DEV} parent 1:4 handle 40: netem loss $1
         "
         if [ $? -ne 0 ]; then
             echo "ERROR: fail to create tc queueing discipline on server ${i}"
         fi
     done
}

# start_tc_delay(msgdelay)
start_tc_delay() {
    for i in ${!sshaddrs[@]}; do
        echo "create tc queueing discipline on server ${i}"
        ssh -n -o ConnectTimeout=${TIME_OUT} ${sshaddrs[i]} "
        sudo tc qdisc add dev ${DEV} root handle 1: prio bands 4 &&
        sudo tc qdisc add dev ${DEV} parent 1:4 handle 40: netem delay $1ms
        "
        if [ $? -ne 0 ]; then
            echo "ERROR: fail to create tc queueing discipline on server ${i}"
        fi
    done
}

# clean_up_tc()
clean_up_tc() {
    for i in ${!sshaddrs[@]}; do
        echo "clean up tc queueing discipline on server ${i}"
        ssh -n -o ConnectTimeout=${TIME_OUT} ${sshaddrs[i]} "
        sudo tc qdisc del dev ${DEV} root
        "
        if [ $? -ne 0 ]; then
            echo "ERROR: fail to clean up tc queueing discipline on server ${i}"
        fi
    done
}

# start_msgloss_mttr(round, usemocknet, duration)
start_msgloss_mttr() {
    for loss in ${msgloss[@]}; do
        round=$1
        if [ $2 == false ]; then
            start_tc_loss ${loss}
            for p in $(seq 1 ${PARALLEL}); do
                start_tc_filter $((${PORT}+${p}))
            done
        fi
        for i in $(seq 1 ${TIMES}); do
            for p in $(seq 1 ${PARALLEL}); do
                flush_parameters $((${PORT}+${p}))
                start_raft "msgloss${loss}mttr" $3 $2 ${loss} 0 true ${round}
                round=$((${round}+1))
                sleep 1s
            done
            wait
        done
        if [ $2 == false ]; then
            clean_up_tc
        fi
    done
}

# start_msgdelay_mttr(round, usemocknet)
start_msgdelay_mttr() {
    for delay in ${msgdelay[@]}; do
        round=$1
        if [ $2 == false ]; then
            start_tc_delay ${delay}
            for p in $(seq 1 ${PARALLEL}); do
                start_tc_filter $((${PORT}+${p}))
            done
        fi
        for i in $(seq 1 ${TIMES}); do
            for p in $(seq 1 ${PARALLEL}); do
                flush_parameters $((${PORT}+${p}))
                start_raft "msgdelay${delay}mttr" $3 $2 0 ${delay} true ${round}
                round=$((${round}+1))
            done
            wait
        done
        if [ $2 == false ]; then
            clean_up_tc
        fi
    done
}

# start_msgloss_noleader(round, usemocknet)
start_msgloss_noleader() {
    for loss in ${msglossnoleader[@]}; do
        round=$1
        if [ $2 == false ]; then
            start_tc_loss ${loss}
            for p in $(seq 1 ${PARALLEL}); do
                start_tc_filter $((${PORT}+${p}))
            done
        fi
        for i in $(seq 1 ${TIMES}); do
            for p in $(seq 1 ${PARALLEL}); do
                flush_parameters $((${PORT}+${p}))
                start_raft "msgloss${loss}noleader" $3 $2 ${loss} 0 false ${round}
                round=$((${round}+1))
            done
            wait
        done
        if [ $2 == false ]; then
            clean_up_tc
        fi
    done
}

flush_parameters() {
  for ((i=0; i<${npeers}; i++)); do
      clusters[${host[i*3]}]="http://${host[i*3+2]}:$1"
      sshaddrs[${host[i*3]}]="${host[i*3+1]}@${host[i*3+2]}"
  done
  CLUSTER=$(IFS=,; echo "${clusters[*]}")
}

TIME_OUT=20000
CODE_DIR=$1; shift
LOG_DIR=$1; shift
PORT=$1; shift
# maximum one-way network latency
LATENCY=$1; shift
TIMES=$1; shift
DEV=$1; shift
PARALLEL=$1; shift

msgloss=(40 50)
msgdelay=(1 100 200 300 400)
msglossnoleader=(50 60 65 70)

host=(`cat hostname.txt`)
npeers=`expr ${#host[@]} / 3`
flush_parameters ${PORT}

if declare -f $1 > /dev/null
then 
    "$@"
else 
    echo "Error: $1 is not a known function name, Options: [start_msgloss_mttr|start_msgdelay_mttr|start_msgloss_noleader]" >&2
    exit 1
fi
