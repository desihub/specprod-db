#!/bin/bash
#
# Submit load_specprod_tile jobs in a chained sequence.
#
function usage() {
    local execName=$(basename $0)
    (
        echo "${execName} [-h] [-j N] [-s SHELL] [-t] [-v]"
        echo ""
        echo "Submit load_specprod_tile jobs in sequence."
        echo ""
        echo "       -h = Print help message and exit."
        echo "     -j N = Only submit N jobs."
        echo " -s SHELL = Scripts have extension SHELL, default 'sh'."
        echo "       -t = Test mode.  Do not make any changes.  Implies -v."
        echo "       -v = Verbose mode. Print lots of extra information."
    ) >&2
}
#
# Command-line options.
#
n_jobs=0
shell=sh
test=/usr/bin/false
verbose=/usr/bin/false
while getopts hj:s:tv argname; do
    case ${argname} in
        h) usage; exit 0 ;;
        j) n_jobs=${OPTARG} ;;
        s) shell=${OPTARG} ;;
        t) test=/usr/bin/true; verbose=/usr/bin/true ;;
        v) verbose=/usr/bin/true ;;
        *) usage; exit 1 ;;
    esac
done
shift $((OPTIND - 1))
#
# Find the first tile and submit it.
#
first_tile=$(grep -- --overwrite load_specprod_tile_*.${shell} | cut -d: -f1)
if [[ -n "${first_tile}" ]]; then
    ${verbose} && echo "DEBUG: sbatch --parsable ${first_tile}"
    if ${test}; then
        job_id=1
    else
        job_id=$(sbatch --parsable ${first_tile})
    fi
    n_submitted=1
else
    echo "INFO: No first tile found, assuming it has already been loaded."
    job_id=0
    n_submitted=0
fi
#
# Find the last tile.
#
last_tile=$(grep -- --primary load_specprod_tile_*.${shell} | cut -d: -f1)
#
# Loop over tiles.
#
for tile in load_specprod_tile_*.${shell}; do
    if [[ "${tile}" == "${first_tile}" ]]; then
        ${verbose} && echo "DEBUG: First tile, ${first_tile}, already submitted."
    elif [[ "${tile}" == "${last_tile}" ]]; then
        ${verbose} && echo "DEBUG: Last tile, ${last_tile}, will be skipped."
    else
        if [[ ${job_id} == 0 ]]; then
            dependency=''
        else
            dependency="--dependency=afterok:${job_id}"
        fi
        ${verbose} && echo "DEBUG: sbatch --parsable ${dependency} ${tile}"
        if ${test}; then
            job_id=$(( job_id + 1 ))
        else
            job_id=$(sbatch --parsable ${dependency} ${tile})
        fi
        n_submitted=$(( n_submitted + 1 ))
        if (( n_submitted == n_jobs )); then
            echo "INFO: ${n_jobs} have been submitted, exiting."
            exit 0
        fi
    fi
done
${verbose} && echo "DEBUG: sbatch --dependency=afterok:${job_id} ${last_tile}"
if ${test}; then
    :
else
    sbatch --dependency=afterok:${job_id} ${last_tile}
fi
