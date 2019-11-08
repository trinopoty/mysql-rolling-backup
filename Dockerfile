FROM mysql:5.7.28

EXPOSE 3306

ADD binlog.cnf /etc/mysql/conf.d/

