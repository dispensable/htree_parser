#!/bin/bash

set -e -o pipefail

DB_DIR=/var/lib/beansdb
BKT_PATH=
DB_HOST=
RSYNC_PORT=7904
LOCAL_BEANSDB_DIR=
DRY_RUN=
DB_CFG_DIR=
LOCAL_DB_CFG_DIR=
USERNAME=$(whoami)
BWLIMIT=10000

function ensure_dep {
    local deps=(ncat yq rsync less)
    local apt_updated=
    for i in "${deps[@]}"; do
        if ! which $i; then
            if [[ -z $apt_updated ]]; then
                apt-get update -y
                apt_updated=1
            fi
            apt-get install -yqq $i
        fi
    done
}

function usage {
    echo "Migrate single db bucket data to container for annalysis"
    echo "-p beansdb db dir, default: /var/lib/beansdb/"
    echo "-h hostname of db"
    echo "-b bucket path, eg 9/1 means /var/lib/beansdb/9/1 bucket, required"
    echo "-d local beansdb dir, required"
    echo "-D dry-run"
    echo "-c remote db conf dir"
    echo "-C local beansdb conf dir"
    echo "-u username"
    echo "-l dbwidth limit, default: 10000 (10MB/s)"
}

function check_args {
    if [[ -z ${BKT_PATH} ]]; then
        echo "-b args required"
        exit 1
    fi

    if [[ -z ${DB_HOST} ]]; then
        echo "-h args required"
        exit 1
    fi

    if [[ -z ${LOCAL_BEANSDB_DIR} ]]; then
        echo "-d args required"
        exit 1
    fi

    if [[ ! -d $LOCAL_BEANSDB_DIR ]]; then
        echo "local beansdb dir not exists"
        exit 1
    fi

    if [[ -z "${DB_CFG_DIR}" ]]; then
        echo "-c args required"
        exit 1
    fi

    if [[ -z $LOCAL_DB_CFG_DIR ]]; then
        echo "-C args required"
        exit 1
    fi
}


while getopts "p:h:b:d:Dc:C:u:l:" opt; do
  case $opt in
    p) DB_DIR=$OPTARG;;
    b) BKT_PATH=$OPTARG;;
    h) DB_HOST=$OPTARG;;
    d) LOCAL_BEANSDB_DIR=$OPTARG;;
    D) DRY_RUN=1;;
    c) DB_CFG_DIR=$OPTARG;;
    C) LOCAL_DB_CFG_DIR=$OPTARG;;
    u) USERNAME=$OPTARG;;
    l) BWLIMIT=$OPTARG;;
    *) usage;;
  esac
done
shift $((OPTIND -1))

check_args
ensure_dep

# rsync remote db path to local
rsync_cmd="rsync -azhP"
if [[ -n $DRY_RUN ]]; then
    rsync_cmd="${rsync_cmd} --bwlimit=${BWLIMIT} --dry-run"
fi

mkdir -p $LOCAL_BEANSDB_DIR/$BKT_PATH/

echo "----------------- start rsync ---------------------"
echo "===> syncing beansdb cfg ..."
$rsync_cmd ${USERNAME}@$DB_HOST:$DB_CFG_DIR/ $LOCAL_DB_CFG_DIR/
echo "===> syncing beansdb btk data ..."
$rsync_cmd ${USERNAME}@$DB_HOST:$DB_DIR/$BKT_PATH/ $LOCAL_BEANSDB_DIR/$BKT_PATH/
echo "========= sync end ========="
echo ""

# change route.yaml to a single btk

# start beansdb locally
