#!/bin/bash
set -o pipefail -o noclobber -o errexit -o nounset

# cd to the root of the project
cd "$(dirname "$(dirname "${0}")")"

controlling_host="${controlling_host:-subutai}"
ipmi_host="${run_host_ipmi:-10.47.142.35}"
controlled_host="${controlled_host:-tinker-3}"
controlled_host_fqdn=tinker-3.cs.iit.edu

ipmi_args="-I lanplus -H "${ipmi_host}" -U root -f "/home-remote/sgrayson/passfile""

if [ -z "${1:-}" ]
then
	echo "Must supply subcommand"
	exit 1
fi

case "${1}" in
	'restart')
		ssh -t "${controlling_host}" ipmitool ${ipmi_args} power cycle
		;;
	'console')
		ssh -t "${controlling_host}" ipmitool ${ipmi_args} sol activate -e '@'
		;;
	'wait_for_linux')
		while ! ping -w 3 -c 1 "${controlled_host_fqdn}" > /dev/null
		do
			echo Waiting
			sleep 3s
		done
		;;
	'run_nautilus')
		unbuffer ./scripts/drive_grub.py
		;;
	*)
		echo "Do not recognize subcommand '${1}'"
esac
