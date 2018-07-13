#!/bin/bash

nautilus="${HOME}/nk"
src_dest="${nautilus}/src/app"
hdr_dest="${nautilus}/include/app"

ln -s "$(realpath nk-Makefile)" "${src_dest}/Makefile"

srcs=($(find . -name '*.c'))
hdrs=($(find . -name '*.h'))

rm -rf "${src_dest}"
rm -rf "${hdr_dest}"

mkdir "${src_dest}" "${nautilus}/src/database"
mkdir "${hdr_dest}" "${nautilus}/include/database"

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
