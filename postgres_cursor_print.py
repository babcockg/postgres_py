#!/usr/bin/env python3

import psycopg2
import datetime

from psycopg2 import Error
from termcolor import colored, cprint

class postgres_cursor_print:
	def __init__ ( self, cursor ):
		self.cursor = cursor
		self.rows = None
		self.query = None
		self.col_widths = None
		self.col_name_mappings = {}
		self.header_color = 'green'
		self.row_color = 'yellow'
		self.column_padding = 0
		
	def info(self):
		info = connection.get_dsn_parameters()
		for key in info.keys():
			print('{0:>24} : {1:<}'.format(key, info[key]))
		return
	
	def map_column_name(self, from_name, to_name):
		self.col_name_mappings[from_name] = to_name
		return

	def map_column_name_dict(self, name_dict):
		self.col_name_mappings.update(name_dict)
		return
	
	def get_mapped_column_name(self, col_name):
		if col_name in self.col_name_mappings.keys():
			return self.col_name_mappings[col_name]
		return col_name
	
	def exec_query ( self, query ):
		self.query = query
		self.cursor.execute(query)
		rows = self.cursor.fetchall()
		self.rows = rows
		return
	
	def get_column_names ( self ):
		colnames = [desc[0] for desc in self.cursor.description]
		return colnames
	
	def calc_columns_widths( self ):
		col_names = self.get_column_names()
		self.col_widths = [0] * len(col_names)
		
		for idx, col_name in enumerate(col_names):
			col_name = self.get_mapped_column_name(col_name)
			self.col_widths[idx] = len(col_name)
		
		for row in self.rows:
			for idx, col_value in enumerate(row):
				if len(str(col_value or '')) > self.col_widths[idx]:
					self.col_widths[idx] = len(str(col_value) or '')
		
		for idx, wid in enumerate(self.col_widths):
			wid += self.column_padding

		return
	
	def formatted_column_name(self, idx ):
		return f"{idx}"
	
	def output_rows ( self , header_color = None, row_color = None ):
		if not header_color == None:
			self.header_color = header_color
		if not row_color == None:
			self.row_color = row_color
			
		self.calc_columns_widths()
		
		print(f"\nThere are {len(self.rows)} rows to output.")
		
		for idx, col_name in enumerate(self.get_column_names()):
			fline = f"{{0:{self.col_widths[idx]}}}{' ' * self.column_padding}"
			cprint(fline.format(self.get_mapped_column_name(col_name)), self.header_color, end = ' ')
		print()
		
		for idx,row in enumerate(self.rows):
			for cidx,col in enumerate(row):
				alignment_char = '<'
				if col == None:
					col = ''

				if type(col) == float:
					col = "{0:.2f}".format(col)
					alignment_char = '>'

				if type(col) == int:
					alignment_char = '>'
					
				if type(col) == datetime.date:
					col = col.strftime('%m/%d/%y')
					
				fline = f"{{0:{alignment_char}{self.col_widths[cidx]}}}{' ' * self.column_padding}"
				cprint ( fline.format(str(col)), self.row_color, end = ' ')
			print()
		return

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #


connection = psycopg2.connect(	user="postgres",
								password="",
								host="127.0.0.1",
								port="5432",
								database="Northwind")

cursor = connection.cursor()
pcp = postgres_cursor_print(cursor)
pcp.info()
limit_amount = 'all'
pcp.exec_query(f"""SELECT ord.order_id as "id"
					, concat(emp.last_name, ', ', emp.first_name) as "employee"
					, concat(ord.customer_id, ', ', cst.company_name) as "customer"
					-- , cst.company_name as "customer"
					, ord.ship_name as "ship to"

					, ord.order_date as "ordered"
					, ord.required_date as "required"
					, ord.shipped_date as "shipped"
					, shp.company_name as "shipper"
					, ord.freight as "shipping cost"
					, cst.address as "ship address"
					, ord.ship_address as "ship address"
					, ord.ship_city as "city"
					, ord.ship_region as "region"
					, ord.ship_postal_code as "postal code"
					, ord.ship_country as "country"

				  FROM orders ord
				  JOIN employees emp ON emp.employee_id = ord.employee_id
				  JOIN shippers shp ON shp.shipper_id = ord.ship_via
				  JOIN customers cst ON cst.customer_id = ord.customer_id
				  ORDER BY employee
				  LIMIT {limit_amount}
				  ;""")
pcp.column_padding = 0
pcp.output_rows()
				