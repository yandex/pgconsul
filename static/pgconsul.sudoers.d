postgres ALL, ALL = NOPASSWD: /bin/systemctl restart postgresql@1[0-9]-data.service
postgres ALL, ALL = NOPASSWD: /bin/systemctl start odyssey.service
postgres ALL, ALL = NOPASSWD: /bin/systemctl stop odyssey.service
