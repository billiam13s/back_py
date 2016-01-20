#!/usr/bin/python
from datetime import date, datetime, timedelta
import socket
import argparse
import json
from subprocess import Popen, PIPE
import os
from shutil import copy2, copytree, rmtree
import tarfile
import tempfile
import re

class Backup:
    __temp = tempfile.gettempdir() # system temp directory
    __monthly = 'monthly'
    __weekly = 'weekly'
    __daily = 'daily'
    __num_file_monthly = 2
    __num_file_weekly = 6
    __num_file_daily = 14
    # compression types
    # bz2: smaller file size, longer run time
    # gz: larger file size, faster run tume
    __compress_types = ['gz', 'bz2']

    def __init__(self, config, datetime=datetime.today(), hostname=socket.gethostname()):
        self.compress_type = self.__compress_types[0] # default compression

        self.datetime = datetime #datetime.today()
        self.hostname = hostname #socket.gethostname()
        self.name = config['name']
        self.dest = config['dest']

        self.mysql = {}
        if config.has_key('mysql'):
            self.mysql['database'] = config['mysql']['database']
            self.mysql['user'] = config['mysql']['user']
            self.mysql['password'] = config['mysql']['password']

        self.files = []
        if config.has_key('files'):
            self.files = config['files']

    def __get_temp_dir(self, full_path=True):
        if full_path:
            return os.path.join(self.__temp, self.name)
        else:
            return self.name

    def __backup_type(self):
        result = self.__weekly

        if 1 == self.datetime.day: #datetime.day:
            result = self.__monthly
        elif 6 != self.datetime.weekday(): #datetime.weekday():
            result = self.__daily

        return result

    def __backup_filename(self):
        extension = 'tar.{compression}'.format(compression=self.compress_type)

        return '{hostname}-{name}-{date}-{backup_type}.{extension}'.format(hostname=self.hostname, date=self.datetime.date(), backup_type=self.__backup_type(), extension=extension, name=self.name)

    def __mysql(self):
        if self.mysql and self.mysql.has_key('database') and self.mysql.has_key('user') and self.mysql.has_key('password'):
            print 'backing up {database} database'.format(database=self.mysql['database'])

            filename = '{database}-{date}.sql'.format(database=self.mysql['database'], date=self.datetime.date())
            sqlArgs = ['mysqldump', '-u', self.mysql['user'], '-p'+self.mysql['password'], self.mysql['database']]

            stdout, stderr = Popen(sqlArgs, stdout=PIPE, stderr=PIPE).communicate()

            f = open(os.path.join(self.__get_temp_dir(), filename), 'w')
            if stderr:
                f.write(stderr)
                print '{0}'.format(stderr)
            else:
                f.write(stdout)
            f.close()


    def __files(self):
        if self.files and type(self.files) is list:
            print 'backing up {name} files'.format(name=self.name)

            for item in self.files:
                if os.path.exists(item):
                    print 'copied {item}'.format(item=item)

                    if os.path.isfile(item):
                        copy2(item, self.__get_temp_dir())

                    elif os.path.isdir(item):
                        dst = os.path.join(self.__get_temp_dir(), os.path.basename(item))
                        copytree(item, dst)

    def __archive(self):
        print 'Archive {name} backup...'.format(name=self.name)

        base_dir = os.getcwd()
        os.chdir(self.__temp)

        try:
            tarMode = 'w:{compression}'.format(compression=self.compress_type)
            backup_file = os.path.join(self.dest, self.__backup_filename())
            tar = tarfile.open(backup_file, tarMode)
            tar.add(self.__get_temp_dir(False))
            tar.close()

        except Exception as e:
            print e

        os.chdir(base_dir)

    def __house_cleaning(self):
        print 'Cleaning up {name} old archives'.format(name=self.name)

        prefix_name = re.escape('{hostname}-{name}'.format(hostname=self.hostname, name=self.name))

        pattern_date_format = '\d{4}\-\d{2}\-\d{2}'
        pattern_file_extensions = '\.tar\.(bz2|gz)'

        pattern_valid_filename = '^{0}\-{1}\-({3}|{4}|{5}){2}$'.format(prefix_name, pattern_date_format, pattern_file_extensions, self.__monthly, self.__weekly, self.__daily)
        pattern_monthly = '^{0}\-{1}\-{3}{2}$'.format(prefix_name, pattern_date_format, pattern_file_extensions, self.__monthly)
        pattern_weekly = '^{0}\-{1}\-{3}{2}$'.format(prefix_name, pattern_date_format, pattern_file_extensions, self.__weekly)
        pattern_daily = '^{0}\-{1}\-{3}{2}$'.format(prefix_name, pattern_date_format, pattern_file_extensions, self.__daily)

        backup_files_monthly = []
        backup_files_weekly = []
        backup_files_daily = []
        to_be_remove = []
        # check directory items. If its a file put into backup_files
        for item in os.listdir(self.dest):
            full_path = os.path.join(self.dest, item)

            if os.path.isfile(full_path):
                if re.match(pattern_valid_filename, item):
                    if re.match(pattern_monthly, item):
                        backup_files_monthly.append(item)
                    elif re.match(pattern_weekly, item):
                        backup_files_weekly.append(item)
                    elif re.match(pattern_daily, item):
                        backup_files_daily.append(item)

        # sort filename newest to older and add the older filename
        if len(backup_files_monthly) > self.__num_file_monthly:
            backup_files_monthly.sort(reverse=True)
            for i in range(self.__num_file_monthly, len(backup_files_monthly)):
                to_be_remove.append(backup_files_monthly[i])

        if len(backup_files_weekly) > self.__num_file_weekly:
            backup_files_weekly.sort(reverse=True)
            for i in range(self.__num_file_weekly, len(backup_files_weekly)):
                to_be_remove.append(backup_files_weekly[i])

        if len(backup_files_daily) > self.__num_file_daily:
            backup_files_daily.sort(reverse=True)
            for i in range(self.__num_file_daily, len(backup_files_daily)):
                to_be_remove.append(backup_files_daily[i])

        if not to_be_remove:
            print "No archive to clean up"
        else:
            for old_file in to_be_remove:
                print "removing {file}".format(file=old_file)
                full_path = os.path.join(self.dest, old_file)
                os.remove(full_path)

    def process(self):
        print '{datetime}: Begin {name} {type} backup process...'.format(name=self.name, type=self.__backup_type(), datetime=self.datetime)

        os.makedirs(self.__get_temp_dir())

        try:
            if self.mysql:
                sqlfile = self.__mysql()

            if self.files:
                files = self.__files()

            self.__archive()

            self.__house_cleaning()

        except Exception as e:
            print e

        rmtree(self.__get_temp_dir())

        print '{name} {type} backup is completed'.format(name=self.name, type=self.__backup_type())

    @staticmethod
    def read_json(json_file):
        def __check_key(config, key, strict=True, optional=False):
            result = True

            if not config.has_key(key) and not optional:
                print '{key} key is required'.format(key=key)
                result = False
            if strict and not optional and config.has_key(key) and not config[key]:
                print 'value is required for {key}'.format(key=key)
                result = False

            if result and config.has_key(key): return config[key]


        try:
            with open(json_file) as f:
                fails = 0
                config = json.load(f)

                if __check_key(config, 'name') is None: fails += 1
                if __check_key(config, 'dest') is None: fails += 1

                if not os.path.exists(config['dest']):
                    os.makedirs(config['dest'])
                elif not os.access(config['dest'], os.W_OK):
                    print '{dest} permission denied'.format(dest=config['dest'])
                    fails += 1

                mysql_conf = __check_key(config, 'mysql', optional=True)
                if not mysql_conf is None:
                    if __check_key(mysql_conf, 'database') is None or __check_key(mysql_conf, 'user') is None or __check_key(mysql_conf, 'password', strict=False) is None:
                        fails += 1

                files = __check_key(config, 'files', optional=True)
                if not files is None and not type(files) is list: fails += 1

                if not fails: return config

        except Exception as e:
            print "{file}: {error}".format(file=json_file, error=e)


def main():
    parser = argparse.ArgumentParser(description='Process backup.')
    parser.add_argument('config_files', metavar='config', type=str, nargs="+", help='config files')

    args = parser.parse_args()

    backups = []
    for config_json in args.config_files:
        config = Backup.read_json(config_json)
        if not config is None: backups.append(Backup(config))

    for backup in backups:
        backup.process();

if __name__ == "__main__":
    main()
