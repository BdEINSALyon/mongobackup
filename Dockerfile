FROM python:3

RUN apt-get update -y && apt-get dist-upgrade -y && apt-get autoremove --purge -y && apt-get autoclean -y

RUN wget https://fastdl.mongodb.org/linux/mongodb-linux-x86_64-debian92-4.0.1.tgz &&\
    tar xf mongodb-linux-x86_64-debian92-4.0.1.tgz &&\
    rm -rf mongodb-linux-x86_64-debian92-4.0.1.tgz &&\
    mv mongodb-linux-x86_64-debian92-4.0.1 mongo

WORKDIR /backup

COPY requirements.txt .
RUN pip install -r requirements.txt

ENV MONGODUMP_COMMAND /mongo/bin/mongodump
ENV DATABASE_URL mongodb://mongo@localhost/mongo
ENV FTP_URL ftp://backup:password@backup.network/backups/mydb
VOLUME /tmp

COPY backup.py .
CMD python /backup/backup.py
