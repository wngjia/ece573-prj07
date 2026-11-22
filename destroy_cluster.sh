#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 0 ]]; then
  echo "Usage: $0" >&2
  exit 2
fi

cd "$(dirname "$0")"
kind delete cluster --name ece573-prj07
rm -f secrets/private.pem secrets/public.pem secrets/admin-password secrets/job-id-secret secrets/job-receipt-secret

echo "Project 7 cluster, database data, and local credentials removed."
