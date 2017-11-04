import sys
sys.path.insert(0, '/Library/Frameworks/Python.framework/Versions/2.7/lib/python2.7/site-packages')

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import requests
sys.path.pop(0)
sys.path.append('/Library/Frameworks/Python.framework/Versions/2.7/lib/python2.7/site-packages')
from bs4 import BeautifulSoup
import time
from unidecode import unidecode
import datetime
import random



link_column = 1
job_type_column = 2
assignee_column = 3
completion_column = 4
writerow_column = 5
available_job_column = 6
author_column = 7
author_affiliation_column = 8
editor_column = 9
editor_department_column = 10
url_column = 11
timedelta_column = 12


new_author_column = 4
new_author_affiliation_column = 5
new_linkedin_author_affiliations_column = 6
new_editor_column = 7
new_department_column = 8
new_editor_affiliation_column = 9
new_url_column = 10
new_timedelta_column = 11
new_dates_column = 12

MACHINES = 32


class DataMover:


	def __init__(self):
		self.main_sheet, self.other_sheets_array = getCredentials()
		self.credential_time = datetime.datetime.now()

	def moveData(self):
		print "working on moving data"
		writerow = 2
		info_tuple_set = set()
		for sheet in self.other_sheets_array:
			if (datetime.datetime.now() - self.credential_time).seconds > (60*50):
				self.main_sheet, self.other_sheets_array = getCredentials()

			for row in xrange(2, sheet.row_count):
				try:
					if not sheet.cell(row, author_column).value:
						break
					author = sheet.cell(row, author_column).value
					split_author = author.split(' ')
					if len(split_author) > 1:
						new_author = split_author[0] + ' ' + split_author[-1]
					else:
						new_author = author
					affiliation = sheet.cell(row, author_affiliation_column).value
					split_affiliation = affiliation.split(',')
					new_affiliation = split_affiliation[0] + ',' + split_affiliation[1]
					editor = sheet.cell(row, editor_column).value
					split_editor = editor.split(' ')
					if len(split_editor) > 1:
						new_editor = split_editor[0] + ' ' + split_editor[-1]
					else:
						new_editor = editor
					department = sheet.cell(row, editor_department_column).value
					new_department = department.replace('.', '').lower()
					url = sheet.cell(row, url_column).value
					timedelta_and_dates = sheet.cell(row, timedelta_column).value
					timedelta = timedelta_and_dates.split(',')[0]
					dates = timedelta_and_dates.split(',')[1] + ',' + timedelta_and_dates.split(',') [2]
					info_tuple = (new_author, affiliation, new_editor, new_department, url, timedelta)
					if info_tuple not in info_tuple_set:
						info_tuple_set.add(info_tuple)
						self.main_sheet.update_cell(writerow, new_author_column, new_author)
						self.main_sheet.update_cell(writerow, new_author_affiliation_column, new_affiliation)
						self.main_sheet.update_cell(writerow, new_editor_column, new_editor)
						self.main_sheet.update_cell(writerow, new_department_column, new_department)
						self.main_sheet.update_cell(writerow, new_url_column, url)
						self.main_sheet.update_cell(writerow, new_timedelta_column, timedelta)
						self.main_sheet.update_cell(writerow, new_dates_column, dates)
						writerow += 1
				except:
					if not sheet.cell(row, author_column).value:
						break
					author = sheet.cell(row, author_column).value
					split_author = author.split(' ')
					if len(split_author) > 1:
						new_author = split_author[0] + ' ' + split_author[-1]
					else:
						new_author = author
					affiliation = sheet.cell(row, author_affiliation_column).value
					split_affiliation = affiliation.split(',')
					new_affiliation = split_affiliation[0] + ',' + split_affiliation[1]
					editor = sheet.cell(row, editor_column).value
					split_editor = editor.split(' ')
					if len(split_editor) > 1:
						new_editor = split_editor[0] + ' ' + split_editor[-1]
					else:
						new_editor = editor
					department = sheet.cell(row, editor_department_column).value
					new_department = department.replace('.', '').lower()
					url = sheet.cell(row, url_column).value
					timedelta_and_dates = sheet.cell(row, timedelta_column).value
					timedelta = timedelta_and_dates.split(',')[0]
					dates = timedelta_and_dates.split(',')[1] + ',' + timedelta_and_dates.split(',') [2]
					info_tuple = (new_author, affiliation, new_editor, new_department, url, timedelta)
					if info_tuple not in info_tuple_set:
						info_tuple_set.add(info_tuple)
						self.main_sheet.update_cell(writerow, new_author_column, new_author)
						self.main_sheet.update_cell(writerow, new_author_affiliation_column, new_affiliation)
						self.main_sheet.update_cell(writerow, new_editor_column, new_editor)
						self.main_sheet.update_cell(writerow, new_department_column, new_department)
						self.main_sheet.update_cell(writerow, new_url_column, url)
						self.main_sheet.update_cell(writerow, new_timedelta_column, timedelta)
						self.main_sheet.update_cell(writerow, new_dates_column, dates)
						writerow += 1


def getCredentials():
	'''
	Obtains credentials to allow machines edit access to the spreadsheets
	'''
	print "getCredentials"
	scope = ['https://spreadsheets.google.com/feeds']
	credentials = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
	client = gspread.authorize(credentials)
	main_sheet = client.open('UAP_web_scraping').sheet1
	other_sheets_array = [client.open('UAP_web_scraping').get_worksheet(sheet_index) for sheet_index in xrange(MACHINES+1)][1:]
	return main_sheet, other_sheets_array


def main():
	'''
	Manager Function
	'''
	data_cleaner = DataMover()
	data_cleaner.moveData()



if __name__ == '__main__':
	main()
