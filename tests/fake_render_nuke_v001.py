#!/usr/bin/python

import sys
import os
import time

OUTPUT_DIR="/projects/nuke/output/test_v001"
FRAMES=range(int(os.environ['ACCSYN_ITEM'].split("-")[0]), int(os.environ['ACCSYN_ITEM'].split("-")[1])+1)

print("Fake rendering frames %s > %s, stand by.."%(FRAMES, OUTPUT_DIR))


if not os.path.exists(OUTPUT_DIR):
	os.makedirs(OUTPUT_DIR)

prefix = os.path.basename(OUTPUT_DIR)

for frame in FRAMES:
	p_output = os.path.join(OUTPUT_DIR, '%s.%04d.jpg'%(prefix, frame))
	print('Writing %s took 0.01 seconds'%(p_output))
	with open(p_output, "w") as f:
		f.write('Humbug')
	time.sleep(1)

print('Total render time: 0.06 seconds'