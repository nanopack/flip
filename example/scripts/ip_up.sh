#!/bin/bash

## this has been written to run on SmartOS

ip2dec () {
  local a b c d ip=$@
  IFS=. read -r a b c d <<< "$ip"
  printf '%d\n' "$((a * 256 ** 3 + b * 256 ** 2 + c * 256 + d))"
}

if [ `uname -s` = 'SunOS' ]; then  
  ip=`ip2dec $1`
  interface=`echo  "$2" | json interface`
  
  ## we delay the up so that we don't get ips flagged as
  ## DUPLICATE
  sleep 2

  if ! ipadm show-addr $interface/ip$ip &> /dev/null; then
    exec ipadm create-addr -t -T static -a $1 $interface/ip$ip
  fi

else
  echo "This script only works on SunOS currently."
  exit 1
fi