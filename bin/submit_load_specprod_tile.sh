#!/bin/bash
#
# Submit load_specprod_tile jobs in a chained sequence.
#
function usage() {
    local execName=$(basename $0)
    (
        echo "${execName} [-s SHELL] [-t] [-v]"
        echo ""
        echo "Submit load_specprod_tile jobs in sequence."
        echo ""
        echo " -s SHELL = Scripts have extension SHELL, default 'sh'."
        echo "       -t = Test mode.  Do not make any changes.  Implies -v."
        echo "       -v = Verbose mode. Print lots of extra information."
    ) >&2
}
#
# Command-line options.
#
shell=sh
test=/usr/bin/false
verbose=/usr/bin/false
while getopts s:tv argname; do
    case ${argname} in
        s) shell=${OPTARG} ;;
        t) test=/usr/bin/true; verbose=/usr/bin/true ;;
        v) verbose=/usr/bin/true
        *) usage; exit 1 ;;
    esac
done
#
# Find the first tile and submit it.
#
first_tile=$(grep -- --overwrite load_specprod_tile_*.${shell} | cut -d: -f1)
${verbose} && echo "sbatch --parsable ${first_tile}"
if ${test}; then
    job_id=123456
else
    job_id=$(sbatch --parsable ${first_tile})
fi
# last tile
last_tile=$(grep -- --primary load_specprod_tile_*.${shell} | cut -d: -f1)
for tile in load_specprod_tile_*.${shell}; do
    if [[ "${tile}" == "${first_tile}" ]]; then
        ${verbose} && echo "DEBUG: First tile, ${first_tile}, already submitted."
    elif [[ "${tile}" == "${last_tile}" ]]; then
        ${verbose} && echo "Last tile, ${last_tile}, will be skipped."
    else
        ${verbose} && echo "sbatch --parsable --afterok:${job_id} ${tile}"
        if ${test}; then
            job_id=$(( job_id + 1 ))
        else
            job_id=$(sbatch --parsable --afterok:${job_id} ${tile})
        fi
    fi
done
${verbose} && echo "sbatch --parsable --afterok:${job_id} ${last_tile}"
if ${test}; then
    :
else
    sbatch --afterok:${job_id} ${last_tile}
fi
