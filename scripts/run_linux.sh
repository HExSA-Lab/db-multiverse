#!/bin/bash
###############################################################################
# Sane environemnt
###############################################################################
set -o pipefail -o noclobber -o errexit -o nounset
#set -o xtrace

# cd to the root of the project
cd "$(dirname "$(dirname "${0}")")"

###############################################################################
# Parse args
###############################################################################
# pass arguments like
#     $ version=12 arg_abc=123 ./run_linux.sh
# this way, I don't have to arg-parse,
# default arguments are easy to implement
# and it is more explicit

# the host on which to to build the binary
build_host="${build_host:-tinker-2}"

# the path to copy our code on the build_host
build_path="${build_path:-db}"

# the host on which to run and meter our binary
run_host="${run_host:-tinker-2}"

# the name of the target to make (on the build_host), copy, and run (on the run_host)
run_target="${run_target:-main}"

# whether or not to delete the old log file
delete=${delete:-}

# required args
if [ -z "${version:-}" ]
then
	echo must pass a version, like
	echo $ version=12 ${0}
	exit 1
fi

###############################################################################
# Set up log
###############################################################################

# clear out log
log=data/linux_${version}.log
if [ -e "${log}" ]
then
	if [ -n "${delete}" ]
	then
		rm "${log}"
	else
		echo "${log} already exists!"
		echo "pass 'delete=1 ${0}' or pass a different version string"
		exit 1
	fi
fi

# Add version information to log
mkdir -p "$(dirname "${log}")"
commit=$(git log -1 --pretty=format:"%h")
echo "Git commit ${commit}" >> "${log}"
git status >> "${log}"

###############################################################################
# build
###############################################################################

# sync with build_host
make clean
rsync ./ "${build_host}:${build_path}/" \
      --archive  \
      --compress  \
      --delete     \
      --exclude=".git/*" \
      --exclude="nautilus/*" \

# build on build host
ssh "${build_host}" make V=1 -C "${build_path}" "${run_target}" | tee -a "${log}"

###############################################################################
# transfer
###############################################################################

# move from build host to run host
remote_tmp_path=/tmp
local_tmp_path=/tmp
ssh "${run_host}" rm -f "${remote_tmp_path}/${run_target}"
if [ "${build_host}" = "${run_host}" ]
then
	ssh "${build_host}" mv "${build_path}/${run_target}" "${remote_tmp_path}/${run_target}"
else
	# scp build_host -> local
	scp "${build_host}:${build_path}/${run_target}" "${local_tmp_path}/${run_target}"
	# scp local -> run_host
	scp "${local_tmp_path}/${run_target}" "${run_host}:${remote_tmp_path}/${run_target}"
fi

###############################################################################
# Run (unbuffered)
###############################################################################

echo running
perf_counters="-e cycles -e L1-icache-load-misses -e dTLB-load-misses"
unbuffer ssh "${run_host}" "unbuffer perf stat ${perf_counters} "${remote_tmp_path}/${run_target}"" \
	| unbuffer -p tee -a "${log}"

echo "run log in ${log}"

./scripts/split_files.py "${log}"

# I use unbuffer so that our code's output is sent to the operator immediately.
# If it our code gets stuck, you reliably know the last thing our code printed.
# I run with perf so that we can compare my perf counters to the perf-tool (verification)

# send a notification
./scripts/notify.sh "${0} done"
