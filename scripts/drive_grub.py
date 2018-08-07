#!/usr/bin/env python3
import pexpect
import sys
import os
import time

# propagate env vars for configuration
child = pexpect.spawn('./scripts/ipmi_helper.sh console', env=os.environ, )
child.logfile = sys.stdout.buffer

child.expect('SOL Session operational', timeout=15)

try:
    # server firmware is done initing and trying to boot
    child.expect('Booting', timeout=120)

    # Grub lists Nautilus option
    child.expect('Nautilus', timeout=120)

    # grub wants our input
    child.expect('automatically', timeout=5)

    # send down-arrow
    for _ in range(7):
        csi = '\x1b['
        down = f'{csi}B'
        child.send(down)
        print('down')
        time.sleep(1)

    # press enter
    # sometimes the first \n doesn't take
    for _ in range(3):
        child.send('\r\n')
        print('enter')
        time.sleep(1)

    child.expect(['root-shell', 'UNHANDLED EXCEPTION'], timeout=None)
except KeyboardInterrupt:
    print('keyboard interrupt')
finally:
    # exits the SOL session
    child.send('\n')
    child.send('@.')
    print('quit')

child.wait()
