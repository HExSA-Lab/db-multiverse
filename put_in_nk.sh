#!/bin/bash

nautilus="${HOME}/nk"
src_dest="${nautilus}/src/test"
hdr_dest="${nautilus}/include/test"

srcs=($(
    find . -name '*.c' | \
    grep -v "./advanced_timing.c"
))

hdrs=($(
    find . -name '*.h' | \
    grep -v "./include/libperf.h"
))

mkdir "${src_dest}"
mkdir "${hdr_dest}"

for file in "${srcs[@]}"
do
    name="$(basename ${file})"
	ln -s "${PWD}/${file}" "${src_dest}/${name}"
done

for file in "${hdrs[@]}"
do
    name="$(basename ${file})"
	ln -s "${PWD}/${file}" "${hdr_dest}/${name}"
done
