#!/usr/bin/env python

import sys, argparse, cmd, shlex, json, requests, time, os, subprocess, \
    itertools

class HdfsBrowser(cmd.Cmd):
    def __init__(self, hdfs_namenode):
        """
        """
        if issubclass(HdfsBrowser, object):
            # New-style class
            super(HdfsBrowser, self).__init__()
        else:
            # Old-style class
            cmd.Cmd.__init__(self)
        self.namenode = hdfs_namenode
        self.intro = "HDFS browser for %s" % (self.namenode)

        self.cwd = "/"

        self.update_prompt()

        self.parsers = {}

        ls_parser = argparse.ArgumentParser(
            prog="ls", description="lists the contents of a directory",
            add_help=False)
        ls_parser.add_argument(
            "directory", help="the name of the directory to list",
            nargs="?")
        ls_parser.add_argument(
            "-l", help="use a long listing format", default=False,
            action="store_true")

        self.parsers["ls"] = ls_parser

        stat_parser = argparse.ArgumentParser(
            prog="stat", description="displays information about the "
            "specified file")
        stat_parser.add_argument(
            "file", help="the name of the file to query")

        self.parsers["stat"] = stat_parser

        cd_parser = argparse.ArgumentParser(
            prog="cd", description="change the working directory",
            add_help=False)
        cd_parser.add_argument(
            "directory", help="the name of the directory to list",
            nargs="?", default="/")

        self.parsers["cd"] = cd_parser

    def update_prompt(self):
        self.prompt = "HDFS %s:%s > " % (
            self.namenode.split(':')[0], self.cwd)

    def parse_args(self, s, cmd_name):
        try:
            args = self.parsers[cmd_name].parse_args(shlex.split(s))
            return args
        except SystemExit:
            return None

    def stat_file(self, filename):
        return self.webhdfs_request(
            path=filename, op="GETFILESTATUS", auto_redirect=True)

    def do_stat(self, s):
        args = self.parse_args(s, "stat")

        if args is None:
            return

        filename = os.path.join(self.cwd, args.file)

        (status_code, stat_info) = self.stat_file(filename)

        if self.handle_error(status_code, 200, stat_info):
            return

        for key, value in stat_info["FileStatus"].items():
            print "%s: %s" % (key, value)

    def complete_stat(self, text, line, begin_index, end_index):
        return self.path_completion(text, line, begin_index, end_index)

    def do_cd(self, s):
        args = self.parse_args(s, "cd")

        if args is None:
            return

        if args.directory.startswith(".."):
            full_path = os.path.dirname(self.cwd)
        else:
            full_path = os.path.join(self.cwd, args.directory)

        (status_code, stat_info) = self.stat_file(full_path)

        if status_code == 200:
            self.cwd = full_path
            self.update_prompt()
        else:
            print "cd: %s: No such file or directory" % (full_path)

    def complete_cd(self, text, line, begin_index, end_index):
        return self.path_completion(text, line, begin_index, end_index)

    def ls_directory(self, directory):
        (status_code, dir_listing) = self.webhdfs_request(
            path=directory, op="LISTSTATUS", auto_redirect=True)

        if self.handle_error(status_code, 200, dir_listing):
            return None

        return dir_listing["FileStatuses"]["FileStatus"]

    def do_ls(self, s):
        args = self.parse_args(s, "ls")

        if args is None:
            return

        if args.directory is None:
            args.directory = self.cwd
        else:
            args.directory = os.path.join(self.cwd, args.directory)

        file_statuses = self.ls_directory(args.directory)

        if file_statuses is None:
            return

        if args.l:
            for file_info in file_statuses:
                file_info["permission"] = self.printable_permissions(file_info)
                file_info["accessTime"] = self.printable_access_time(file_info)

                print ("%(permission)s  %(replication) 3d %(owner)s %(group)s "
                       "%(length) 13s %(accessTime)s %(pathSuffix)s") % (
                    file_info)
        else:
            print self.columnar_list(
                map(lambda x: x["pathSuffix"], file_statuses))

    def complete_ls(self, text, line, begin_index, end_index):
        return self.path_completion(text, line, begin_index, end_index)

    def path_completion(self, incomplete_path, line, begin_index, end_index):
        if incomplete_path == "" and begin_index == end_index:
            line_chunks = line.strip().split(' ')
            incomplete_path = line_chunks[len(line_chunks) - 1]

        dirname = os.path.join(
            self.cwd, os.path.dirname(incomplete_path))

        file_statuses = self.ls_directory(dirname)

        if file_statuses == None:
            return []
        else:
            matches = list(itertools.ifilter(
                    lambda y: y.startswith(incomplete_path),
                    map(lambda x: x["pathSuffix"], file_statuses)))
            return matches

    def columnar_list(self, str_list):
        str_list.sort()

        column_cmd = subprocess.Popen(
            "column", stdout=subprocess.PIPE, stdin=subprocess.PIPE, shell=True)
        stdout_data, stderr_data = column_cmd.communicate(
            input='\n'.join(str_list) + '\n')

        return stdout_data.strip()

    def printable_permissions(self, file_info):
        if file_info["type"] == "DIRECTORY":
            perm_string = "d"
        else:
            perm_string = "-"

        for digit in map(int, str(file_info["permission"])):
            bits = []
            flags = ['r', 'w', 'x']

            while digit > 0:
                bits.append(digit % 2)
                digit /= 2

            while len(bits) < len(flags):
                bits.append(0)

            for i, flag in enumerate(flags):
                if bits[i] == 1:
                    perm_string += flags[i]
                else:
                    perm_string += '-'

        return perm_string

    def printable_access_time(self, file_info):
        access_time = time.localtime(int(file_info["accessTime"]) / 1000.0)
        current_time = time.localtime()

        month, day, time_of_day, year = time.strftime(
            "%b %d %H:%M %Y", access_time).split(' ')

        if access_time.tm_year != current_time.tm_year:
            return "%s % 2s % 5s" % (month, day, year)
        else:
            return "%s % 2s % 5s" % (month, day, time_of_day)

    def help_ls(self):
        self.parsers["ls"].print_help()

    def do_exit(self, s):
        """Quits the shell
        """
        sys.exit(0)

    def do_quit(self, s):
        """Quits the shell
        """
        self.do_exit(s)

    def handle_error(self, received_status, expected_status, response_json):
        if received_status != expected_status:
            print ("Exception %(exception)s in %(javaClassName)s: %(message)s"
                   % response_json["RemoteException"])

        return received_status != expected_status

    def webhdfs_request(
        self, path, op, method="get", auto_redirect=False, expect_json=True,
        **kwargs):

        if path[0] == '/':
            path = path[1:]

        if len(path) > 0 and path[-1] == '/':
            path = path[:-1]

        webhdfs_url = "http://%s/webhdfs/v1/%s" % (
            self.namenode, path)

        request_params = kwargs
        request_params["op"] = op

        webhdfs_response = requests.request(
            method, webhdfs_url, params=request_params,
            allow_redirects=auto_redirect, config={"trust_env" : False})

        if webhdfs_response.status_code == 307 and not auto_redirect:
            webhdfs_response = requests.request(
                method, webhdfs_response.headers['location'])

        response = webhdfs_response.text

        if expect_json:
            try:
                response = json.loads(response)
            except:
                response = None

        return (webhdfs_response.status_code, response)

def main():
    parser = argparse.ArgumentParser(
        description="runs a bash-like prompt for HDFS")
    parser.add_argument(
        "hdfs_namenode", help="host:port on which HDFS namenode is running "
        "WebHDFS")

    args = parser.parse_args()

    shell = HdfsBrowser(**vars(args))
    shell.cmdloop()

if __name__ == "__main__":
    sys.exit(main())
