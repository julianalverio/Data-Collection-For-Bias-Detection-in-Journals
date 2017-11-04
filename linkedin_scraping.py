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
import datetime
from selenium import webdriver
import selenium
import signal
import multiprocessing

email_address = 'email_address'
password = 'password'


author_column = 4
author_affiliation_column = 6
editor_column = 7
editor_affiliation_column = 9


def scrapeLinkedIn(driver, input_name='Ravi Bapna'):
	print 'Scraping:', input_name
	driver.get('https://www.google.com/')
	time.sleep(random.uniform(3.1, 8.1))
	search_bar_element = driver.find_element_by_id('lst-ib')
	search_bar_element.send_keys(input_name + ' LinkedIn')
	search_bar_element.submit()
	time.sleep(random.uniform(3.1, 8.1))

	linkedin_element = driver.find_element_by_partial_link_text('| LinkedIn')
	linkedin_element.click()
	time.sleep(random.uniform(3.1, 8.1))

	# education_element = driver.find_element_by_class_name('pv-entity__degree-info')
	# print education_element.text
	affiliations_list = []
	try:
		education_elements = driver.find_elements_by_xpath("//div[@class='pv-entity__degree-info']")
	except:
		print "ERROR"
		education_elements = ['']
	for education_element in education_elements:
		info = education_element.text
		split_info = info.split('\n')
		if len(split_info) == 1:
			affiliation = split_info[0]
		if len(split_info) >= 3:
			affiliation = split_info[0] + ',' + split_info[2]
		if len(split_info) >= 5:
			affiliation = split_info[0] + ',' + split_info[2] + ',' + split_info[4]
		try:
			affiliations_list.append(unidecode(affiliation))
		except:
			pass
		print affiliation
	affiliations_list = str(affiliations_list)[1: -1]
	print (affiliations_list)
	return affiliations_list


def getCredentials():
	'''
	Obtains credentials to allow machines edit access to the spreadsheets
	'''
	print "getCredentials"
	scope = ['https://spreadsheets.google.com/feeds']
	credentials = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
	client = gspread.authorize(credentials)
	main_sheet = client.open('UAP_web_scraping').sheet1
	return main_sheet


def main():
	chrome_path = "/Users/julianalverio/Desktop/chromedriver"
	driver = webdriver.Chrome(chrome_path)
	start_url = 'https://www.linkedin.com/uas/login'
	driver.get(start_url)
	time.sleep(random.uniform(3.1, 8.1))
	email_element = driver.find_element_by_id("session_key-login")
	email_element.send_keys(email_address)
	password_element = driver.find_element_by_id("session_password-login")
	password_element.send_keys(password)
	password_element.submit()


	credential_time = datetime.datetime.now()
	main_sheet = getCredentials()

	affiliations_hash = {}
	for row in xrange(2, main_sheet.row_count):
		if not main_sheet.cell(row, author_column).value:
			break
		author = main_sheet.cell(row, author_column).value
		if author not in affiliations_hash:
			try:
				author_affiliations = scrapeLinkedIn(driver, author)
			except:
				continue
			affiliations_hash[author] = author_affiliations
		else:
			author_affiliations = affiliations_hash[author]
		main_sheet.update_cell(row, author_affiliation_column, author_affiliations)
		editor = main_sheet.cell(row, editor_column).value
		if editor not in affiliations_hash:
			try:
				editor_affiliations = scrapeLinkedIn(driver, editor)
			except:
				continue
			affiliations_hash[editor] = editor_affiliations
		else:
			editor_affiliations = affiliations_hash[editor]
		main_sheet.update_cell(row, editor_affiliation_column, editor_affiliations)

	driver.close()


if __name__ == "__main__":
	main()



