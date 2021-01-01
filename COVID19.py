#!/usr/bin/env python3

import covid19_data
import string
import sqlite3
from datetime import datetime

rows_to_select = -1
import os
delete_existing_database = True

print('Sqlite3 version {0}'.format(sqlite3.version))

if os.path.exists("covid19.db"):
	print("covid19.db exists")
	if delete_existing_database:
		print("deleting covid19.db")
		os.remove("covid19.db")


conn = sqlite3.connect(r"covid19.db")

cursor = conn.cursor()
cursor.execute("SELECT name FROM sqlite_master WHERE type = 'table';")
rows = cursor.fetchall()

cursor.execute('''CREATE TABLE IF NOT EXISTS covid19 (
					  id integer PRIMARY KEY,
			          location text,
			          updated text,
			          confirmed integer,
					  deaths integer,
					  recovered integer,
					  active integer)''')

rows = cursor.execute('''SELECT * FROM covid19''')
current_rows = cursor.fetchall()

headers = ['Updated', 'Confirmed', 'Deaths', 'Recovered', 'Active']
name_maps = {
	'UNITEDSTATESVIRGINISLANDS':'United States Virgin Islands',
	'GRANDPRINCESS' : 'Grand Princess',
	'WESTVIRGINIA' : 'West Virginia',
	'AMERICANSAMOA' : 'American Somoa',
	'VIRGINISLANDS' : 'Virgin Islands',
	'SOUTHDAKOTA' : 'South Dakota',
	'SOUTHCAROLINA' : 'South Carolina',
	'RHODEISLAND' : 'Rhode Island',
	'PUERTORICO' : 'Puerto Rico',
	'NEWMEXICO' : 'New Mexico',
	'NEWJERSEY' : 'New Jersey',
	'NEWYORK' : 'New York',
	'NEWHAMPSHIRE' : 'New Hampshire',
	'NORTHCAROLINA' : 'North Carolina',
	'NORTHDAKOTA' : 'North Dakota',
	'NORTHERNMARIANAISLANDS' : 'Northern Mariana Islands',
	'DISTRICTOFCOLUMBIA' : 'District of Columbia'
}

def has_lower_case_letters(s):
	for c in s:
		if c.islower():
			return True
	return False
	
def mapped_name(key):
	result = key
	if key in name_maps.keys():
		result = name_maps[key]
		
	if not has_lower_case_letters(result):
		result = result.capitalize()
	return result


def print_header():
	print ( '{0:6}'.format(''), '{0:32}'.format(''), end='' )
	for h in headers:
		print ('{0:>32}'.format(h), end = '')
	print()
	return

def date_time_format(d):
	d = datetime.strptime(d,'%Y-%m-%d %H:%M:%S.%f')
	result = '{0:02}/{1:02}/{2:4} @ {3:02}:{4:02}'.format(d.month, d.day, d.year, d.hour, d.minute)
	return result

def print_data_set ( t ):
	print_header()
	for row in t:
		print('{0:>6}'.format(row[0]), '{0:<32}{1:>32}{2:32,}{3:32,}{4:32,}{5:32,}'.format(mapped_name(row[1]), date_time_format(row[2]), row[3], row[4], row[5], row[6]))
	return


data_set = covid19_data.JHU.dataByName("GEORGIA")
d = data_set.data
found_usa = False
for k in d.keys():
	
	if k == 'ALASKA':
		found_usa = True
		
	if found_usa:
		try:
			value_list = list(d[k].keys());
			if len(value_list) == 4:
				found = [item for item in current_rows if item[1] == k]
				sql = f'''INSERT OR REPLACE INTO covid19
						  ( id, location, updated, confirmed, deaths, recovered, active )
						  VALUES (
									(SELECT id FROM covid19 WHERE location = '{k}'),
									'{k}',
									'{str(datetime.now())}',
									{d[k][list(d[k].keys())[0]] or 0},
									{d[k][list(d[k].keys())[1]] or 0},
									{d[k][list(d[k].keys())[2]] or 0},
									{d[k][list(d[k].keys())[3]] or 0}
								 ); '''
#				print(sql)
				cursor.execute(sql)
		except (Exception) as error:
			print('ERROR: ', error, '\n', sql)

conn.commit()

cursor.execute(f'''SELECT * FROM covid19 ORDER BY deaths DESC LIMIT {rows_to_select or 1000000} ''')
set = cursor.fetchall()
print_data_set(set)

print('------------------')

cursor.execute(f'''SELECT * FROM covid19 WHERE location = 'KANSAS' ORDER BY Deaths DESC LIMIT {rows_to_select or 1000000} ''')
set = cursor.fetchall()
print_data_set(set)

conn.close()
