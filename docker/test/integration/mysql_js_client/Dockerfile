# docker build -t clickhouse/mysql-js-client .
# MySQL JavaScript client docker container

FROM node:16.14.2

WORKDIR /usr/app

RUN npm install mysql --registry=http://mirrors.cloud.tencent.com/npm/

COPY ./test.js ./test.js
