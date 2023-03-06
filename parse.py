#!/usr/bin/env python3
"""
Parses MySQL slow log files. Output is in CSV.

# Time: 180705 12:34:56
# User@Host: root[root] @ localhost []
# Query_time: 13.123456  Lock_time: 0.01234 Rows_sent: 0  Rows_examined: 123456
SET timestamp=1530661285;
select * from mytable;

New format
# Time: 230305 20:05:30
# User@Host: root[root] @  localhost []
# Thread_id: 7658002  Schema: dbname  Last_errno: 0  Killed: 0
# Query_time: 13.747660  Lock_time: 0.000036  Rows_sent: 20  Rows_examined: 3701090  Rows_affected: 0  Rows_read: 3701090
# Bytes_sent: 420782
SET timestamp=1530661285;
select * from mytable;

"""


import fileinput
import re
import csv
import sys

pat = """^# Time: (?P<time>\d{2}\d{2}\d{2} \d{2}:\d{2}:\d{2}\s?)
# User@Host: (?P<User_Host>.*?)\s?
# Thread_id: (?P<Thread_id>.*?)\s+Schema: (?P<Schema>.*?)\s+Last_errno: (?P<Error_N>\d+)\s+Killed: (?P<Killed>\d+)\s?
# Query_time: (?P<Query_time>\d+\.\d+)\s+Lock_time: (?P<Lock_time>\d+\.\d+)\s+Rows_sent: (?P<Rows_sent>\d+)\s+Rows_examined: (?P<Rows_examined>\d+)\s+Rows_affected: (?P<Rows_affected>\d+)\s+Rows_read: (?P<Rows_read>\d+)\s?
# Bytes_sent: (?P<Bytes_sent>\d+)\s?
(?P<Query>[^#]+)#?$"""

fields = ['Time', 'User_Host', 'Query_time', 'Schema', 'Error_N', 'Lock_time', 'Rows_sent',
        'Rows_examined', 'Rows_affected', 'Rows_read', 'Bytes_sent', 'Query']

lines = ''
for line in fileinput.input():
    lines = lines + line

writer = csv.writer(sys.stdout)
headers_printed = False
for match in re.finditer(pat, lines, re.MULTILINE | re.DOTALL):
    if not headers_printed:
        writer.writerow(fields)
        headers_printed = True
    writer.writerow(match.groups())
