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

MACHINES = 32  # Must be >= 1


'''
System overview:
Each machine can write to the first sheet in the Google Spreadsheet (main sheet) and it's own spreadsheet, which corresponds to its machine ID.
The machine can read from any sheet. 
Tentatively, the Google Spreadsheet has 33 sheets -- 1 main sheet and 32 other sheets for 32 machines.
Upon starting up, a machine will obtain permissions to access the Google Spreadsheet, then mark it's presense on the main sheet. It will also 
set its status as "1" to indicate everything is OK. For the rest of the time the machine is online, it's execute executeJobs(). Now the machine
will search through all of the sheets (except the main one) to find a job that has yet to be done. It will mark that it's executing that job, 
then wait a random amount of time (between 5 and 15 seconds). This makes the request pattern to the Journal's server less consistent. Secondly,
it allows for the elimination of race conditions; in order to begin executing a job, the machine must first see the job is available to take off
the queue, then it will try to claim it, wait a random amount of time, then check again that no second machine has marked the job as theirs during
the random wait time. If this series of conditions is met, this first machine will begin executing the job. After the job is completed, the 
machine that executed the job will either write more jobs onto its corresponding sheet one at a time, or it will write data onto its corresponding
sheet (editor and author data). The machine never writes to any sheet other than the sheet that corresponds to its machine_id during executeJobs().
When the jobs are finally all completed, executeJobs() will silently return.

Some cautionary notes:
GSpread (the Google Spreadsheets API) isn't perfect, thus you will find that sometimes due to bad wifi, sometimes for no reason, random API calls
will raise a gspread.exceptions.RequestError. This is handled in a simple, recursive fashion in recursiveExecuteJobs(). See below.
To iterate quickly, you can always test your code with just two machines at a time before testing with many.
Always be sure to reset the spreadhseet (you can do so manually if you're in a hurry) before running the script each time.


'''

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


class Scraper:
	'''
	Main class responsible for the entire scraping process
	'''


	def __init__(self):
		'''
		Creates scraper object. Gets all spreadsheet objects, stores certain row values for quick writing (to avoid lookups)
		Attributes:
			self.main_sheet
			self.other_sheets_array
			self.machine_id
			self.scraper_sheet
			self.scraper_sheet_writerow
			self.data_input_row
		'''
		self.main_sheet, self.other_sheets_array = getCredentials()
		self.credential_time = datetime.datetime.now()
		writerow = int(self.main_sheet.cell(2,3).value)
		self.main_sheet.update_cell(2, 3, writerow + 1)
		self.machine_id = writerow - 1
		self.main_sheet.update_cell(writerow, 1, self.machine_id)
		self.main_sheet.update_cell(writerow, 2, '1')
		self.scraper_sheet = self.other_sheets_array[self.machine_id - 1]
		self.scraper_sheet_writerow = 2
		if self.machine_id == 1:
			self.scraper_sheet_writerow = 3
			self.scraper_sheet.update_cell(2, writerow_column, '3')
		self.inputNewMachine()
		print 'MACHINE ID: ', self.machine_id
		self.data_input_row = 2


	def inputNewMachine(self):
		'''
		Adds a machine to the main sheet and marks it as live
		'''
		print "inputNewMachine"
		self.main_sheet.update_cell(self.machine_id + 1, 1, str(self.machine_id))
		self.main_sheet.update_cell(self.machine_id + 1, 2, '1')


	def executeJobs(self):
		'''
		Primary function for executing/creating jobs.
		Function searches through worksheets sequentially, looking for jobs. Will claim a job, wait a random
		amount of time to provide variability in server requests and to eliminate race conditions (unwanted data overlap
		between threads). Will execute a job, then either post more jobs on the queue or write down the acquired data
		from the queue.
		'''

		print "executeJobs"
		while True:
			#check -- if too long has passed, preemptively refresh credentials
			if (datetime.datetime.now() - self.credential_time).seconds > (60*50):
				self.main_sheet, self.other_sheets_array = getCredentials()
			for attempt in xrange(MACHINES):
				sheet_index = random.randint(0, 32)

			link, job_type, assignment_row, assignment_sheet = self.waitForAssignment()
			#if you don't have any jobs left
			if not link: return
			self.completeOneJob(link, job_type, assignment_row, assignment_sheet)



	def waitForAssignment(self):
		'''
		Allows the machine to acquire a job.
		Looks
		'''
		print 'waitForAssignment'
		for attempt in xrange(MACHINES):
			next_sheet_index = random.randint(0, MACHINES-1)
			sheet = self.other_sheets_array[next_sheet_index]
			available_job_row = int(sheet.cell(2, available_job_column).value)
			writerow = int(sheet.cell(2, writerow_column).value)
			if writerow - available_job_row == 0:
				continue
			for row in xrange(available_job_row, sheet.row_count):
				if not sheet.cell(row, link_column).value:
					break
				if not sheet.cell(row, assignee_column).value:
					sheet.update_cell(row, assignee_column, self.machine_id)
					self.updateAvailableJobsRow(sheet, available_job_row)
					print "random waiting"
					time.sleep(5 + random.randint(0, 5))
					print "done waiting"
					if sheet.cell(row, assignee_column).value == str(self.machine_id):
						return sheet.cell(row, link_column).value, sheet.cell(row, job_type_column).value, row, sheet
		for next_sheet_index in xrange(MACHINES):
			sheet = self.other_sheets_array[next_sheet_index]
			available_job_row = int(sheet.cell(2, available_job_column).value)
			writerow = int(sheet.cell(2, writerow_column).value)
			if writerow - available_job_row == 0:
				continue
			for row in xrange(available_job_row, sheet.row_count):
				if not sheet.cell(row, link_column).value:
					break
				if not sheet.cell(row, assignee_column).value:
					sheet.update_cell(row, assignee_column, self.machine_id)
					self.updateAvailableJobsRow(sheet, available_job_row)
					print "random waiting"
					time.sleep(5 + random.randint(0, 5))
					print "done waiting"
					if sheet.cell(row, assignee_column).value == str(self.machine_id):
						return sheet.cell(row, link_column).value, sheet.cell(row, job_type_column).value, row, sheet
		return [None for _ in xrange(4)]


	def addJob(self, link, job_type):
		'''
		Adds a job to the queue
		'''
		print "addJob"
		self.scraper_sheet.update_cell(self.scraper_sheet_writerow, link_column, link)
		self.scraper_sheet.update_cell(self.scraper_sheet_writerow, job_type_column, job_type)
		self.scraper_sheet_writerow += 1
		self.scraper_sheet.update_cell(2, writerow_column, str(self.scraper_sheet_writerow))

	def updateAvailableJobsRow(self, sheet, available_job_row):
		'''
		Updates the "Available Jobs Row", which indicates to machines where to start
		searching for new jobs to pull off the queue. This exists only as a speed optimization.
		'''
		print "updateAvailableJobsRow"
		for row in xrange(available_job_row, sheet.row_count):
			if not sheet.cell(row, completion_column).value:
				sheet.update_cell(2, available_job_column, row + 1)
				return


	def cleanIssueLinks(self, unedited_links):
		'''
		Takes in all links from volume homepage, spits out list of issue links
		Only returns issues 2011 or more recen
		'''
		print 'cleanIssueLinks'
		edited_links = []
		matcher = 'href="http://pubsonline.informs.org/toc/mnsc/'
		for link in unedited_links:
			for sublink in str(link).split(' '):
				if matcher in sublink:
					if len(sublink.split('"')) == 3:
						edited_links.append(sublink.split('"')[1])
		recent_issues_links = []
		for link in edited_links:
			try:
				if int(link.split('/')[5]) >= 57:
					recent_issues_links.append(link)
			except ValueError:
				if 'MT' in link:
					continue
				import pdb; pdb.set_trace()
		return recent_issues_links
		


	def cleanArticleLinks(self, unedited_links):
		'''
		Takes raw HTML anchor information. Outputs complete links to articles.
		'''
		print 'cleanArticleLinks'
		prefix = 'http://pubsonline.informs.org'
		edited_links = []
		for link in unedited_links:
			for sublink in str(link).split(' '):
				if 'href' in sublink:
					edited_links.append(prefix + sublink.split('"')[1])
		return edited_links


	def cleanAuthorText(self, author_text):
		'''
		Takes in HTML information about an author. Extracts Author name and 
		affiliation.
		'''
		print "cleanAuthorText"
		try:
			clean_author_text = str(author_text)
			clean_author_text = clean_author_text.replace('<div class="authorLayer"><span class="close">x</span><div class="header">', '')
			clean_author_text = clean_author_text.replace('</div><div><a class="entryAuthor" href="/author/">Search for articles by this author</a><br/>', '!')
			clean_author_text = clean_author_text.replace('</div><div><a class="entryAuthor" href="/author/">Search for articles by this author</a><br>', '!')
			clean_author_text = clean_author_text.replace('<br/><br/></br></div></div>', '')
			clean_author_text = clean_author_text.replace('<br/><br/></div></div>', '')
			clean_author_text_unicode = unicode(clean_author_text, 'utf-8')
			clean_author_text = unidecode(clean_author_text_unicode)
			[author, affiliation] = clean_author_text.split('!')
		except:
			import pdb; pdb.set_trace()
		return author, affiliation


	def cleanEditor(self, unclean_editor):
		'''
		Takes in HTML information about the editor. Extracts editor name and
		department.
		'''
		print "cleanEditor"
		for item_index in xrange(len(unclean_editor)):
			if "<i>This paper was accepted by" in str(unclean_editor[item_index]):
				clean_editor = str(unclean_editor[item_index]).replace('<i>', '').replace('</i>', '')
				editor = clean_editor.replace('This paper was accepted by ', '')
				editor_name, department = editor.split(',')
				editor_name_unicode = unicode(editor_name, 'utf-8')
				editor_name = unidecode(editor_name_unicode)
				return editor_name.strip(), department.strip()


	def completeOneJob(self, link, job_type, assignment_row, assignment_sheet):
		print "completing job"
		page = requests.get(link)
		if str(page.status_code)[0] != '2':
			#mark dead
			self.main_sheet.update_cell(self.machine_id + 1, 2, 0)
			print "Machine's IP Has Been Blocked :("
			return False
		soup = BeautifulSoup(page.content, 'html.parser')
		#main link to home page --> create job for each issue
		if job_type == '0':
			unedited_links = soup.find_all('a')
			recent_issues_links = self.cleanIssueLinks(unedited_links)
			for issue_link in recent_issues_links:
				self.addJob(issue_link, '1')
			assignment_sheet.update_cell(assignment_row, completion_column, '1')

		#link to issue --> create job for each article
		elif job_type == '1':
			links = soup.find_all(class_='art_title linkable')
			article_links = self.cleanArticleLinks(links)
			for article_link in article_links:
				self.addJob(article_link, '2')
			assignment_sheet.update_cell(assignment_row, completion_column, '1')

		#link to an issue --> mine data, do not create a job
		elif job_type == '2':
			if not ('This paper was accepted by' in str(soup)
			and 'Accepted:' in str(soup) and 'Received:' in str(soup)):
				assignment_sheet.update_cell(assignment_row, completion_column, 'not a paper')
				return
			if 'special issue editors' in str(soup):
				assignment_sheet.update_cell(assignment_row, completion_column, 'special issue: ignore')
				return
			try:
				editor = soup.find_all('i')
				clean_editor, department = self.cleanEditor(editor)
			except:
				assignment_sheet.update_cell(assignment_row, completion_column, 'HTML error')
				return

			received = str(soup.find_all('div', {'class': 'publicationContentReceivedDate dates'}))
			received_parsed = received.split('Received: ')[1].split('\\')[0]
			accepted = str(soup.find_all('div', {'class': 'publicationContentAcceptedDate dates'}))
			accepted_parsed = accepted.split('Accepted: ')[1].split('\\')[0]
			try:
				received_date = datetime.datetime.strptime(received_parsed, '%B %d, %Y')
				accepted_date = datetime.datetime.strptime(accepted_parsed, '%B %d, %Y')
				timedelta = str((accepted_date - received_date).days) + ',' + str(received_date).split(' ')[0] + ',' + str(accepted_date).split(' ')[0]
			except:
				timedelta = 'date error'


			authors = soup.find_all(class_='authorLayer')
			for author_text in authors:
				author, affiliation = self.cleanAuthorText(author_text)
				self.scraper_sheet.update_cell(self.data_input_row, author_column, author)
				self.scraper_sheet.update_cell(self.data_input_row, author_affiliation_column, affiliation)
				self.scraper_sheet.update_cell(self.data_input_row, editor_column, clean_editor)
				self.scraper_sheet.update_cell(self.data_input_row, editor_department_column, department)
				self.scraper_sheet.update_cell(self.data_input_row, url_column, link)
				self.scraper_sheet.update_cell(self.data_input_row, timedelta_column, timedelta)
				self.data_input_row += 1
			if not assignment_sheet.cell(assignment_row, completion_column).value:
				assignment_sheet.update_cell(assignment_row, completion_column, '1')
		return True


	def trashCollect(self):
		print "trash collecting"
		if (datetime.datetime.now() - self.credential_time).seconds > (60*50):
			self.main_sheet, self.other_sheets_array = getCredentials()
		for sheet in self.other_sheets_array:
			for row in xrange(2, sheet.row_count):
				if not sheet.cell(row, link_column).value:
					break
				if not sheet.cell(row, assignee_column).value:
					if not sheet.cell(row, job_type_column).value:
						continue
					sheet.update_cell(row, assignee_column, str(self.machine_id))
					print "random waiting"
					time.sleep(random.randint(1, 10))
					print "done waiting"
					if sheet.cell(row, assignee_column).value == str(self.machine_id):
						link = sheet.cell(row, link_column).value
						job_type = sheet.cell(row, job_type_column).value
						self.completeOneJob(link, job_type, row, sheet)

							
def reset():
		'''
		"Resets" the state of the spreadsheet. Deletes everything and rewrites everything such
		that it's ready to run the script again.
		BEWARE: This function is very slow and there's not much that can be done to speed it up.
		'''	
		print "reset"
		main_sheet, other_sheets_array = getCredentials()
		credential_time = datetime.datetime.now()
		clearSheet(main_sheet, other_sheets_array, credential_time)
		main_sheet.update_cell(1, 1, 'Machine ID')
		main_sheet.update_cell(1, 2, 'Status')
		main_sheet.update_cell(1, 3, 'Writerow')
		main_sheet.update_cell(1, 4, 'Author')
		main_sheet.update_cell(1, 5, 'Affiliation (Journal)')
		main_sheet.update_cell(1, 6, 'Affiliations (LinkedIn)')
		main_sheet.update_cell(1, 7, 'Editor Accepted')
		main_sheet.update_cell(1, 8, 'Department')
		main_sheet.update_cell(1, 9, 'Editor Affiliations (LinkedIn)')
		main_sheet.update_cell(1, 10, 'URL')
		main_sheet.update_cell(1, 11, 'TimeDelta')
		main_sheet.update_cell(1, 12, 'Dates')

		main_sheet.update_cell(2, 3, '2')
		main_sheet.update_cell(3, 13, "Status=0: Machine's IP Has Been Blocked")
		main_sheet.update_cell(4, 13, 'Status=1: Machine is Online')
		main_sheet.update_cell(5, 13, 'Status=2: Machine is Done')
		main_sheet.update_cell(7, 13, 'First Link:')
		main_sheet.update_cell(8, 13, "http://pubsonline.informs.org/loi/mnsc?expanded=2016&expanded=62'")

		for sheet in other_sheets_array:
			if (datetime.datetime.now() - credential_time).seconds > 60*50:
				main_sheet, other_sheets_array = getCredentials()
			sheet.update_cell(1, link_column, 'Link')
			sheet.update_cell(1, job_type_column, 'Job Type')
			sheet.update_cell(1, assignee_column, 'Assignee')
			sheet.update_cell(1, completion_column, 'Completion')
			sheet.update_cell(1, writerow_column, 'Writerow')
			sheet.update_cell(1, available_job_column, 'Available Job Row')
			sheet.update_cell(1, author_column, 'Author')
			sheet.update_cell(1, author_affiliation_column, 'Affiliation')
			sheet.update_cell(1, editor_column, 'Editor')
			sheet.update_cell(1, editor_department_column, 'Department')
			sheet.update_cell(1, url_column, 'URL')
			sheet.update_cell(1, timedelta_column, 'TimeDelta')
			sheet.update_cell(2, writerow_column, '2')
			sheet.update_cell(2, available_job_column, '2')

		first_sheet = other_sheets_array[0]
		first_sheet.update_cell(2, link_column, "http://pubsonline.informs.org/loi/mnsc?expanded=2016&expanded=62'")
		first_sheet.update_cell(2, job_type_column, '0')


def clearSheet(main_sheet, other_sheets_array, credential_time, main_row_width=12, other_row_width=12):
	'''
	Starting at the top left corner, clear out the cells from left to right and top to bottom.
	This stops when it sees two consecutive empty rows at the bottom. Note, it clears a limited 
	width: main_row_width for the [first] main spreadsheet, and other_row_width for all other spreadsheets.
	'''
	empty_row = 0
	for row in xrange(1, main_sheet.row_count+1):
		miss_counter = 0
		for col in xrange(1, main_row_width+1):
			if main_sheet.cell(row, col).value == '':
				miss_counter += 1
			main_sheet.update_cell(row, col, '')
		if miss_counter >= main_row_width: 
			empty_row += 1
		else:
			empty_row = 0
		if empty_row >= 2:
			break
	for sheet in other_sheets_array:
		print "sheet"
		if (datetime.datetime.now() - credential_time).seconds > 60*50:
			main_sheet, other_sheets_array = getCredentials()
		empty_row = 0
		for row in xrange(1, sheet.row_count+1):
			miss_counter = 0
			for col in xrange(1, other_row_width+1):
				if sheet.cell(row, col).value == '':
					miss_counter += 1
				sheet.update_cell(row, col, '')
			if miss_counter >= other_row_width:
				empty_row += 1
			else:
				empty_row = 0
			if empty_row >= 2:
				break


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
	scraper = Scraper()
	if scraper.machine_id != 1:
		time.sleep(5)
	try:
		scraper.executeJobs()
	except gspread.exceptions.RequestError:
		recursiveExecuteJobs(scraper)
	scraper.main_sheet.update_cell(scraper.machine_id+1, 2, '2')
	try:
		scraper.trashCollect()
	except gspread.exceptions.RequestError:
		pass
	scraper.main_sheet.update_cell(scraper.machine_id+1, 2, '3')
	print "Next Step: Manually Scrape Data from Failed Links!"


def recursiveExecuteJobs(scraper):
	'''
	A simple fix for a mysterious "gspread.exceptions.RequestError"
	This error doesn't seem to have any origin besides Google Sheets
	and GSpread not being perfectly reliable.
	This will continue retrying job execution after that error comes up.
	This is the cause of some skipped jobs, however.
	'''
	try:
		scraper.executeJobs()
	except gspread.exceptions.RequestError:
		recursiveExecuteJobs()


if __name__ == '__main__':
	# reset()
	main()

	


'''
Additional Notes:
Spotty wifi will cause certain API calls hang and never return or sometimes return a strange error. Works best with stable wifi.
http://pubsonline.informs.org/toc/mnsc/58/7  notable link due to editor being Management Science
The GSpread API isn't perfectly reliable. Thus you will occassionally get skipped/empty cells. You can modify the spreadsheet
(jobs_available_row) while the script is running to push the script to go back and fix these skipped cells.
'''
