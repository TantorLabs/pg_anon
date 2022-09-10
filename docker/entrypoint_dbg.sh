#!/bin/bash
set -e

echo '[ ! -z "$TERM" -a -r /etc/motd ] && cat /etc/motd' >> /etc/bash.bashrc

trap : TERM INT; sleep infinity & wait
