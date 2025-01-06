### SimpleDB 연결
- 기존 환경, 테스트에서 의존성, 테스트케이스를 변경하지 않고 만족

### MySQL 환경
```shell
chmod 644 dockerProjects/mysql-1/volumes/etc/mysql/conf.d/my.cnf 2>/dev/null
echo "[mysqld]
# general_log = ON
# general_log_file = /etc/mysql/conf.d/general.log" > dockerProjects/mysql-1/volumes/etc/mysql/conf.d/my.cnf
chmod 444 dockerProjects/mysql-1/volumes/etc/mysql/conf.d/my.cnf
```
```shell
docker run \
    --name mysql-1 \
    -p 3306:3306 \
    -v /${PWD}/dockerProjects/mysql-1/volumes/var/lib/mysql:/var/lib/mysql \
    -v /${PWD}/dockerProjects/mysql-1/volumes/etc/mysql/conf.d:/etc/mysql/conf.d \
    -e TZ=Asia/Seoul \
    -e MYSQL_ROOT_PASSWORD=lldj123414 \
    -d \
    mysql:8.4.1
```