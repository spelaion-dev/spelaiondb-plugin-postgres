#!/usr/bin/env bash
# Generates the test certificates (they are not real secrets; they are in .gitignore). The
# same ones as in SpelaionDB's repository, without what only its SSH tests use.
#   keys/ca.crt           test certificate authority
#   keys/server.crt/.key  the server's TLS certificate, valid for "localhost" and for
#                         "postgres", but not for 127.0.0.1: that way verify-full is tested
#                         against verify-ca
#   keys/client.crt/.key  client certificate for the user "spelaion_cert", who logs in
#                         without a password (SpelaionDB's ADR-022)
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p keys
# Git Bash on Windows rewrites arguments that start with "/" (like -subj "/CN=…").
export MSYS_NO_PATHCONV=1

if [ ! -f keys/ca.crt ]; then
  openssl req -x509 -new -nodes -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 \
    -keyout keys/ca.key -out keys/ca.crt -days 3650 -subj "/CN=SpelaionDB test CA" \
    -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" 2>/dev/null
fi

if [ ! -f keys/server.crt ]; then
  openssl req -new -nodes -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 \
    -keyout keys/server.key -out keys/server.csr -subj "/CN=localhost" 2>/dev/null
  printf 'subjectAltName=DNS:localhost,DNS:postgres\nbasicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature\nextendedKeyUsage=serverAuth\n' > keys/server.ext
  openssl x509 -req -in keys/server.csr -CA keys/ca.crt -CAkey keys/ca.key -CAcreateserial \
    -out keys/server.crt -days 3650 -extfile keys/server.ext 2>/dev/null
  rm -f keys/server.csr keys/server.ext keys/ca.srl
fi

if [ ! -f keys/client.crt ]; then
  openssl req -new -nodes -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 \
    -keyout keys/client.key -out keys/client.csr -subj "/CN=spelaion_cert" 2>/dev/null
  printf 'basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature\nextendedKeyUsage=clientAuth\n' > keys/client.ext
  openssl x509 -req -in keys/client.csr -CA keys/ca.crt -CAkey keys/ca.key -CAcreateserial \
    -out keys/client.crt -days 3650 -extfile keys/client.ext 2>/dev/null
  rm -f keys/client.csr keys/client.ext keys/ca.srl
fi

echo "Certificates in $(pwd)/keys"
