#!/bin/sh
set -e

# The demo keeps its data in a Delta table in /data.
export GOALERT_STORAGE_OBJECTS=file:///data

if ! test -f /init-data
then
  echo Creating the demo storage...
  mkdir -p /data
  /bin/goalert create-storage
  /bin/goalert add-user --admin --user admin --pass admin123
  /bin/goalert add-user --user user --pass user1234
  touch /init-data
fi

exec /bin/goalert --log-requests
