#!/bin/bash
# Start cron and tail logs
cron
touch /var/log/producer.log
tail -f /var/log/producer.log

# chmod +x api/entrypoint.sh run in terminal