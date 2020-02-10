#!/usr/bin/env bash
#doc: Show all the available comment commands
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
echo "Available commands:"
DOCTAG="#"
DOCTAG="${DOCTAG}doc"
for command in $(ls -1 $DIR/*.sh); do
    printf " * **%s** %s\n" "/$(basename $command | sed 's/.sh//g')" "$(cat $command | grep $DOCTAG | sed "s/$DOCTAG//g")"
done
