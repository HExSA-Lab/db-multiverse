#!/usr/bin/env python3
import re
from pathlib import Path
import os

if len(sys.argv) != 2:
    raise RuntimeError('Must pass a file')
file_path = Path(sys.argv[1])

file_pattern = re.compile(rb'file: (?P<name>.*?) \{\n(?P<content>.*?)\}', re.DOTALL)

with file_path.open('b') as file:
    file_string = file.read()
    for match in file_pattern.finditer(file_string):
        sub_file_path = match.group('name')
        sub_file_path = sub_file_path.replace(b'$parent', file_path.stem.encode())
        sub_file_path = file_path.parent / sub_file_path
        print(sub_file_path)
        with sub_file_path.open('wb') as sub_file:
            sub_file.write(match.group('content'))
