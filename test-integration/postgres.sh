#!/bin/bash

# Tweak PATH for Travis
export PATH=$PATH:$HOME/gopath/bin

OPTIONS="-config=test-integration/dbconfig.yaml -env postgres"

set -ex

sql-migrate status $OPTIONS
sql-migrate up $OPTIONS
sql-migrate down $OPTIONS
sql-migrate redo $OPTIONS
sql-migrate status $OPTIONS
