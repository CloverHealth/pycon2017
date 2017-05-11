#!/bin/bash
set -e

gprof2dot -f pstats $1 | dot -Tpng > $1.png
open $1.png
