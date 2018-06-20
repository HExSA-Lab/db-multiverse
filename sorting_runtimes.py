#!/usr/bin/env python3.6
import matplotlib.pyplot as plt
import yaml
import numpy as np

with open('README.md') as f:
    lines = list(f)

start = lines.index('```\n') + 1
end   = lines[start:].index('```\n') + start
data = yaml.load(''.join(lines[start:end]))

colors = 'rgbcmyk'
base_x = 0
labels = []

plt.figure()

for algorithm_n, (algorithm, algorithm_data) in enumerate(data.items()):
    for ver, version_data in enumerate(algorithm_data):
        plt.bar(
            x=base_x + ver,
            height=version_data['mean'],
            yerr=version_data['std'],
            color=colors[algorithm_n]
        )
        labels.append(f'{algorithm[:-4]} v{ver}')
    base_x += ver + 1

plt.title('performance of various sorting algorithms')
plt.ylabel('sort time (sec)')
plt.xticks(np.array(range(base_x)) - 0.1, labels, rotation=60)
plt.subplots_adjust(bottom=0.2)
fig = plt.gcf()
plt.show()
fig.savefig('sorting_runtimes.png')
