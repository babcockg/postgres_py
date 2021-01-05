#!/usr/bin/env python3

import psycopg2

from psycopg2 import Error
from termcolor import colored, cprint
import datetime

mapped_column_names = {
	"product_id": "id",
	"product_name": "name",
	"supplier_id": "supplier",
	"category_id": "cat",
	"quantity_per_unit": "unit",
	"unit_price": "unit price",
	"units_in_stock": "stock",
	"units_on_order": "ordered",
	"reorder_level": "reorder",
	"discontinued": "disc",
	"category_name": "category",
	"company_name": "name",
	"order_id" : "id",
	"customer_id" : "cust",
	"employee_id" : "emp",
	"order_date" : "order",
	"required_date" : "required",
	"shipped_date" : "shipped",
	"shipped_via" : "via",
	"ship_name" : "name",
	"ship_address" : "address",
	"ship_region" : "region",
	"ship_city" : "city",
	"ship_postal_code" : "postal code",
	"ship_country" : "country"
}


def get_mapped_column_name(name):
	result = mapped_column_names.get(name, name)
	return result

def type_name(x):
	return psycopg2.extensions.string_types[x].name.lower()
	
def get_column_info(curs):
	#
	# https://www.psycopg.org/docs/cursor.html
	#
	try:
		colnames = [desc[0] for desc in cursor.description]
		coltypes = [desc[1] for desc in cursor.description]
		
		coltypes = list(map(type_name, coltypes))
		curs.scroll(0, mode='absolute')
		
		# lens is a list of integers that is the maximum string length
		# of column names or column data
		lens = [0] * len(colnames)
		
		# seed with the string length of the mapped column names
		for colidx in range(0, len(lens)):
			lens[colidx] = len(get_mapped_column_name(colnames[colidx]))
			
		# overwrite column length if the length from a data row is longer
		rows = curs.fetchall()
		for row in rows:
			for colidx, value in enumerate(row):
				data_width = len(str(row[colidx] or ''))
				if data_width > lens[colidx]:
					lens[colidx] = data_width
	except (Exception, Error) as error:
		print(error)
	finally:
		return [lens, colnames, coltypes]
	
	
def formatted_output_line(value, width, extra_padding, is_column_header, col_type):
	try:
		result = f'{{0:<{width}}}'
		
		if col_type == 'date':
			value = str(value)
			result = f'{{0:{width}}}'
			
		if col_type == 'float':
			if is_column_header:
				result = f'{{0:>{width}}}'
			else:
				result = f'{{0:>{width}.2f}}'
				
		result = result.format(value or '')
		result += ' ' * (extra_padding - 1) + '|'
		return result
	except (Exception, Error) as error:
		print("Error creating formatted output line", error)

# row_limit = 'ALL' or an integer > 0
row_limit = 'ALL'
connection = None

try:
	# Connect to an existing database
	connection = psycopg2.connect(user="galen",
		password="",
		host="127.0.0.1",
		port="5432",
		database="Northwind")
	
	# Create a cursor to perform database operations
	cursor = connection.cursor()
	
	# Print PostgreSQL details
	print("PostgreSQL server information")
	info = connection.get_dsn_parameters()
	for key in info.keys():
		print('{0:>24} : {1:<}'.format(key, info[key]))
		
	# Executing a SQL query
	cursor.execute("SELECT version();")
	
	# Fetch result
	record = cursor.fetchone()
	print("You are connected to - ", record, "\n")
	
	print (f"Query results limited to {row_limit} rows.")
	
	# TODO: Determine safety of query parameter substitution
	cursor.execute(f"""SELECT      ords.order_id
								, ords.customer_id
								, concat(emp.last_name, ', ',  emp.first_name) as "employee"
								, ords.order_date
								, ords.required_date
								, ords.shipped_date
								, sh.company_name
								, ords.freight
								, ords.ship_name
								, ords.ship_address
								, ords.ship_city
								, ords.ship_region
								, ords.ship_postal_code
								, ords.ship_country
							FROM orders ords
							JOIN shippers sh ON sh.shipper_id = ords.ship_via
							JOIN employees emp ON emp.employee_id = ords.employee_id
							ORDER BY employee
							LIMIT {row_limit}""")
	rows = cursor.fetchall()
	
	column_info = get_column_info(cursor)
	lens = column_info[0]
	colnames = column_info[1]
	coltypes = column_info[2]
	extra_padding = 4
	
	for colidx, colname in enumerate(colnames):
		col_value = formatted_output_line(get_mapped_column_name(colname), lens[colidx], extra_padding, True, coltypes[colidx])
		cprint(col_value, 'green',  end='')
	print()
	
	for row in rows:
		for colidx, column in enumerate(row):
			alignment_direction = "<"
			col_value = formatted_output_line(column, lens[colidx], extra_padding, False, coltypes[colidx])
			cprint(col_value, 'yellow', end='')
		print()
		
except (Exception, Error) as error:
	print("Error while connecting to PostgreSQL", error)
finally:
	if (connection):
		cursor.close()
		connection.close()
		print("PostgreSQL connection is closed")
		