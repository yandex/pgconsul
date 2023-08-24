SHELL=/bin/sh
PATH=/bin:/sbin:/usr/bin:/usr/sbin:/usr/local/bin:/usr/local/sbin
MAILTO=mail-root@yandex-team.ru

*/1 * * * *   root /etc/cron.yandex/wd_pgconsul >/dev/null 2>&1
