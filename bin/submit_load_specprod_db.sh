#!/bin/bash
#
# Submit load_specprod_db jobs in sequence.
#
function usage() {
    local execName=$(basename $0)
    (
        echo "${execName} [-h] [-s SHELL] [-t] [-v] SPECPROD"
        echo ""
        echo "Submit load_specprod_db jobs in sequence."
        echo ""
        echo "       -h = Print help message and exit."
        echo " -s SHELL = Scripts have extension SHELL, default 'sh'."
        echo "       -t = Test mode.  Do not make any changes.  Implies -v."
        echo "       -v = Verbose mode. Print lots of extra information."
        echo " SPECPROD = The specprod jobs to submit, e.g. 'iron'."
    ) >&2
}
#
# Command-line options.
#
shell=sh
test=/usr/bin/false
verbose=/usr/bin/false
while getopts hs:tv argname; do
    case ${argname} in
        h) usage; exit 0 ;;
        s) shell=${OPTARG} ;;
        t) test=/usr/bin/true; verbose=/usr/bin/true ;;
        v) verbose=/usr/bin/true ;;
        *) usage; exit 1 ;;
    esac
done
shift $((OPTIND - 1))
#
# Get the specprod value.
#
if [[ $# > 0 ]]; then
    specprod=$1
else
    echo "ERROR: SPECPROD must be set!" >&2
    exit 1
fi
#
# Loop over jobs.
#
job_id=0
for stage in exposures photometry targetphot target redshift fiberassign; do
    if [[ "${stage}" == "exposures" ]]; then
        dependency=''
    else
        dependency="--dependency=afterok:${job_id}"
    fi
    ${verbose} && echo "DEBUG: sbatch --parsable ${dependency} load_specprod_db_${specprod}_${stage}.${shell}"
    if ${test}; then
        job_id=$(( job_id + 1 ))
    else
        job_id=$(sbatch --parsable ${dependency} load_specprod_db_${specprod}_${stage}.${shell})
    fi
done
