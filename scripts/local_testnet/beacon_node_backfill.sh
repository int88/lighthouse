#!/usr/bin/env bash

#
# Starts a beacon node based upon a genesis state created by `./setup.sh`.
#

set -Eeuo pipefail

source ./vars.env

SUBSCRIBE_ALL_SUBNETS=
DEBUG_LEVEL=${DEBUG_LEVEL:-info}

# Get options
while getopts "d:sh" flag; do
  case "${flag}" in
    d) DEBUG_LEVEL=${OPTARG};;
    s) SUBSCRIBE_ALL_SUBNETS="--subscribe-all-subnets";;
    h)
       echo "Start a beacon node"
       echo
       echo "usage: $0 <Options> <DATADIR> <NETWORK-PORT> <HTTP-PORT>"
       echo
       echo "Options:"
       echo "   -s: pass --subscribe-all-subnets to 'lighthouse bn ...', default is not passed"
       echo "   -d: DEBUG_LEVEL, default info"
       echo "   -h: this help"
       echo
       echo "Positional arguments:"
       echo "  DATADIR       Value for --datadir parameter"
       echo "  NETWORK-PORT  Value for --enr-udp-port, --enr-tcp-port and --port"
       echo "  HTTP-PORT     Value for --http-port"
       exit
       ;;
  esac
done

# Get positional arguments
data_dir=${@:$OPTIND+0:1}
network_port=${@:$OPTIND+1:1}
http_port=${@:$OPTIND+2:1}

exec lighthouse \
	--debug-level $DEBUG_LEVEL \
	bn \
	$SUBSCRIBE_ALL_SUBNETS \
	--datadir $HOME/.lighthouse/local-testnet/backfill \
	--testnet-dir $TESTNET_DIR \
	--enable-private-discovery \
	--staking \
	--enr-address 127.0.0.1 \
	--enr-udp-port 9010 \
	--enr-tcp-port 9010 \
	--port $network_port \
	--http-port 8010 \
	--disable-packet-filter \
	--target-peers $((BN_COUNT - 1)) \
	--validator-monitor-auto \
	--checkpoint-sync-url http://127.0.0.1:8001
        
