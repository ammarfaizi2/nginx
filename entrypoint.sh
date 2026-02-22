#!/bin/sh
cron
exec nginx -g "daemon off;"
