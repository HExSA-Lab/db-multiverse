#!/bin/bash
###############################################################################
# Sane environemnt
###############################################################################
set -o pipefail -o noclobber -o errexit -o nounset
set -o xtrace

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
