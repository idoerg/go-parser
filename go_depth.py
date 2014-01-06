#!/usr/bin/env python
import GO.go_utils as gu
import getopt
import sys

opts, args = getopt.getopt(sys.argv[1:],'f:', ['filename='])

gocon, goc = gu.open_go(user="idoerg", passwd="mingus", db="MyGO")
infile = None
for o, a in opts:
    if o in ('-f','--filename='):
        infile = a
if infile:
    for inline in file(infile):
        go_acc = inline.strip()
        go_level = gu.go_level(go_acc,goc)
        print "%s\t%.1f" % (go_acc, go_level)
    
else:
    go_acc = args[0]
    go_level = gu.go_level(go_acc,goc)
    print "%s\t%.1f" % (go_acc, go_level)
gocon.close()
        








