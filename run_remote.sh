#!/bin/bash
set -e

build_host=qemu
build_path=db/
host=tinker-1
path=/projects/db/
run_name="${HOME}/data/data/linux_run_r3.out"

if [ -n "${1}" ]
then
	run_name="${1}"
fi
touch "${run_name}"
echo Writing to "${run_name}"

make distclean
rsync ./ "${build_host}:${build_path}/" \
	  --archive \
	  --compress \
	  --delete \
	  --exclude=".git/*"

ssh "${build_host}" -- make -C "${build_path}" main

scp "${build_host}:${build_path}/main" .
scp main "${host}:${path}/"

ssh "${host}" -- "${path}/main" | tee "${run_name}"
ln -sf $(realpath "${run_name}") ~/data/data/linux_run.out
