#!/bin/sh

# https://pypi.org/project/ntfy/
# If you would like to be notified when scripts complete, use pip
# to install ntfy and configure a default notification target.
# For example, I receive a telegram notification on my phone, even
# if I am away from my laptop.
if which ntfy
then
	ntfy send "$@"
fi
