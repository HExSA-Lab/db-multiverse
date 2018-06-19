#!/usr/bin/env python3.6
import matplotlib.pyplot as plt
import yaml

with open('README.md') as f:
    lines = list(f)

start = lines.index('```\n') + 1
end   = lines[start:].index('```\n') + start
data = yaml.load(''.join(lines[start:end]))

plt.figure()
last_i = 0
labels = []
for algorithm, algorithm_data in data.items():
    for i, version_data in enumerate(algorithm_data):
        plt.bar(
            x=i + last_i,
            height=version_data['mean'],
            yerr=version_data['std'],
        )
        print(labels)
        labels.append(f'{algorithm[:-4]} v{i}')
    last_i += i + 1

plt.xticks(list(range(last_i)), labels)
plt.show()
