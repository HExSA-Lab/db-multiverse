#!/bin/bash
###############################################################################
# Sane environemnt
###############################################################################
set -o pipefail -o noclobber -o errexit -o nounset
set -o xtrace

# cd to the root of the project
cd "$(dirname "$(dirname "${0}")")"

###############################################################################
# Parse args
###############################################################################
# pass arguments like
#     $ version=12 arg_abc=123 ./run_nautk.sh

# required args
if [ -z "${version:-}" ]
then
	echo must pass a version, like
	echo $ version=12 ${0}
	exit 1
fi

# the host on which to to build the binary
build_host="${build_host:-tinker-3}"

# the path to copy our code on the build_host
build_path="${build_path:-nautilus}"

# the host on which to run and meter our binary
run_host="${run_host:-tinker-3}"

###############################################################################
# Set up log
###############################################################################

# clear out log
log=data/nautk_${version}.log
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
# Insert our code into Nautilus
###############################################################################
for subdir in src include
do
	rm -rf "nautilus/${subdir}/app"

	# copy directory structure
	find "${subdir}" -mindepth 1 -type d -exec mkdir nautilus/{} \;
	# directories are copied instead of linked, so that
	# we can add a Makefile to nautilus/src/app it ending up in our proj/src/app
	# Additionally, it avoids nautilus outputing object-files in our source directories.

	# link files
	find "${subdir}" -mindepth 1 -type f -exec link {} nautilus/{} \;
done

for subdir in $(find src/app -type d)
do
	object_files=$(find "${subdir}" -maxdepth 1 -name '*.c' -printf '%P ' | sed 's/\.c/\.o/g')
	subsubdirs=$(find "${subdir}" -mindepth 1 -maxdepth 1 -type d -printf '%P/ ')
	rm -f "nautilus/${subdir}/Makefile"
	echo "obj-y += ${subsubdirs}" > "nautilus/${subdir}/Makefile"
	echo "obj-y += ${object_files}" >> "nautilus/${subdir}/Makefile"
done

###############################################################################
# build
###############################################################################

# sync with build_host
#make -C nautilus clean
#ssh "${build_host}" make -C "${build_path}" clean
rsync nautilus/ "${build_host}:${build_path}/" \
      --archive  \
      --compress  \
	  --checksum   \
      `#--delete`   \
      --copy-links   \
	  --verbose
# --copy-linkes makes rsync copy the _content_ of the links, rather than just the symlink

# build on build host
ssh "${build_host}" make V=1 -C "${build_path}" nautilus.bin | tee -a "${log}"

if [ "${build_host}" = "${run_host}" ]
then
	ssh "${run_host}" -- mv "${build_path}/nautilus.bin" "/boot/nautilus.bin"
else
	scp "${build_host}:${build_path}/nautilus.bin" /tmp/nautilus.bin
	scp /tmp/nautilus.bin "${run_host}:/boot/nautilus.bin"
fi

# capture FQDN hostname for later
run_host_hostname=$(ssh "${run_host}" hostname)

###############################################################################
# run on remote host
###############################################################################

./scripts/ipmi_helper.sh restart
./scripts/ipmi_helper.sh console | unbuffer -p tee -a "${log}"

#./scripts/ipmi_helper.sh run_nautilus | unbuffer -p tee -a "${log}"

./scripts/notify.sh 'Nautilus done'

./scripts/ipmi_helper.sh restart
./scripts/ipmi_helper.sh wait_for_linux

./scripts/notify.sh 'Linux back up'
