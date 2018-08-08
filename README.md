# MongoDBBackup

Backup script written in Python to dump a MongoDB database and upload it to a FTP

## Usage

```
usage: backup.py [-h] --db DB --ftp FTP [--mongodump MONGODUMP] [--max MAX]
                 [--name NAME]

If an arg is specified in more than one place, then commandline values
override environment variables which override defaults.

optional arguments:
  -h, --help       show this help message and exit
  --db DB          database url (e.g. mongodb://mongo@localhost/db) [env
                   var: DATABASE_URL]
  --ftp FTP        FTP url (e.g.
                   ftp://backup:password@backup.network/backups/mydb) [env
                   var: FTP_URL]
  --mongodump MONGODUMP  mongodump path command [env var: MONGODUMP_COMMAND]
  --max MAX        maximum count of backups [env var: MAX_FILES]
  --name NAME      backup name [env var: BACKUP_NAME]
```

A Docker image `bdeinsalyon/mongobackup` is also available

## ToDo

* Implement Duplicity (http://duplicity.nongnu.org/) as backup storage

## License

This script is given under MIT License and GNU AGPLv3 sublicense.
```
© 2017, Philippe VIENNE
© 2018, François LALLEVÉ
```
