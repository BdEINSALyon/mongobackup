import logging

import dj_mongo_database_url
import configargparse
from datetime import datetime
from sultan.api import Sultan
from urllib import parse as urlparse
import ftputil

root = logging.getLogger()
root.setLevel(logging.INFO)

p = configargparse.ArgumentParser()
p.add('--db', required=True, help='database url (e.g. mongodb://mongo@localhost/db)',
      env_var='DATABASE_URL')
p.add('--ftp', required=True, help='FTP url (e.g. ftp://backup:password@backup.network/backups/mydb)',
      env_var='FTP_URL')
p.add('--mongodump', required=False, help='mongodump path command',
      env_var='MONGODUMP_COMMAND', default='mongodump')
p.add('--max', required=False, help='maximum count of backups',
      env_var='MAX_FILES', default=5)
p.add('--name', required=False, help='backup name',
      env_var='BACKUP_NAME', default='manual_backup')
options = p.parse_args()

s = Sultan()


def backup_front_name(database):
    db = dj_mongo_database_url.parse(database)
    return db['HOST'] + '-' + db['NAME'] + '-' + options.name


def backup_name(database):
    now = datetime.now()
    now = now.replace(microsecond=0)
    return backup_front_name(database) + '-' + now.isoformat() + ".gz"


def backup(database, ftp, mongodump):
    logging.info('Starting backup')
    name = backup_name(database)
    path = '/tmp/' + name
    logging.info('Backup database to {}'.format(path))
    s.bash('-c', '"'+' '.join([mongodump, '--uri', database, '--archive', path, '--gzip']) + '"').run()


    ftp = urlparse.urlparse(ftp, 'ftp')
    logging.info('Sending backup {} to {}'.format(path, ftp.hostname))

    host = ftp.hostname
    if ftp.port:
        host = host + ':' + ftp.port

    # noinspection is due to an error into FTPHost code
    # noinspection PyDeprecation
    with ftputil.FTPHost(host, ftp.username, ftp.password) as host:
        host.makedirs(ftp.path)
        host.upload_if_newer(path, ftp.path + name + '.gz')
        host.chdir(ftp.path)
        files = [file for file in host.listdir(ftp.path) if file.startswith(backup_front_name(database))]
        files.sort()
        while len(files) > options.max:
            old_file = files.pop(0)
            host.remove(ftp.path + old_file)
            logging.info("Deleted old file {}".format(old_file))

    s.rm(path).run()
    logging.info('Ended')


if __name__ == '__main__':
    backup(options.db, options.ftp, options.mongodump)
