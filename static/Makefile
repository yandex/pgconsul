install:
	install -d $(DESTDIR)/etc
	install -d $(DESTDIR)/etc/init.d
	install -d $(DESTDIR)/etc/sudoers.d
	install -d $(DESTDIR)/etc/logrotate.d
	install -d $(DESTDIR)/etc/cron.d
	install -d $(DESTDIR)/etc/cron.yandex
	install -m755 pgconsul.init.d $(DESTDIR)/etc/init.d/pgconsul
	install -m400 pgconsul.sudoers.d $(DESTDIR)/etc/sudoers.d/pgconsul
	install -m644 pgconsul.logrotate $(DESTDIR)/etc/logrotate.d/pgconsul
	install -m644 wd-pgconsul.cron.d $(DESTDIR)/etc/cron.d/wd-pgconsul
