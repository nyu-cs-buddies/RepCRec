#!/bin/bash
PROGRAM=${1:-./build/repcrec}
shift
INDIR=${1:-./inputs}
shift
OUTDIR=${1:-./outputs}
echo "program=<$PROGRAM> indir=<$INDIR> outdir=<$OUTDIR>"

INS="`seq 1 23`"
INPRE="test"
OUTPRE="out"

for f in ${INS}; do
	echo "${PROGRAM} ${INDIR}/${INPRE}${f} > ${OUTDIR}/${OUTPRE}${f}"
	${PROGRAM} ${INDIR}/${INPRE}${f} > ${OUTDIR}/${OUTPRE}${f} &
done