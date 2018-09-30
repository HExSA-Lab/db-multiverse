#!/bin/bash
set -o pipefail -o noclobber -o errexit -o nounset

# cd to the root of the project
cd "$(dirname "$(dirname "${0}")")"

if [ -z "${1:-}" ]
then
	echo "Must supply one or more subcommands"
	exit 1
fi

controlling_host="${controlling_host:-subutai}"
controlled_host="${controlled_host:-tinker-2}"

if [ "${controlled_host}" = tinker-2 ]
then
	ipmi_host="${run_host_ipmi:-10.47.142.34}"
	controlled_host_fqdn=tinker-2.cs.iit.edu
elif [ "${controlled_host}" = tinker-3 ]
then
	ipmi_host="${run_host_ipmi:-10.47.142.35}"
	controlled_host_fqdn=tinker-3.cs.iit.edu
else
	echo "Host information not found"
	exit 1
fi


ipmi_args="-I lanplus -H "${ipmi_host}" -U root -f "/home-remote/sgrayson/passfile""

case "${1}" in
	'restart')
		ssh -t "${controlling_host}" ipmitool ${ipmi_args} power cycle
		;;
	'console')
		ssh -t "${controlling_host}" ipmitool ${ipmi_args} sol activate -e '@'
		;;
	'wait')
		sleep 5s
		while ! ping -w 3 -c 1 "${controlled_host_fqdn}" > /dev/null
		do
			echo Waiting
			sleep 3s
		done
		;;
	'ipmi')
		ssh -t "${controlling_host}" ipmitool ${3}
		;;
	*)
		echo "Do not recognize subcommand '${1}'"
esac

if [ "$#" -gt 1 ]
then
	shift
	"./${0}" ${@}
fi
