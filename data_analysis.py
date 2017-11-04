import csv


csv_file_path = ''  #input csv file path as a string

'''
csv file is expected in the form of:
column 1 = author names
column 2 = author journal affiliations
column 3 = author linkedin affiliations
column 4 = editor names
column 5 = editor department
column 6 = editor linkedin affiliations
column 7 = url
column 8 = timedelta (days)
column 9 = dates that produced timedelta. Example: 2013-07-29,2015-04-21 (must be in that format)
'''


author_name_column = 1
author_journal_affiliations_column = 2
author_linkedin_affiliations_column = 3
editor_name_column = 4
editor_department_column = 5
editor_linkedin_column = 6
url_column = 7
timedelta_column = 8
dates_column = 9


'''
Analyze:
What is the percentage of overlap between journal and linked in affiliations for authors?
What percentage of papers accepted come from a particular university?
What should be the average percentage of papers to be accepted from a particular university?
What percentage of accepted papers had author-editor affiliations in common? (Any editor affiliations, listed author affiliation)
'''


def calculatePercentAffiliationOverlapAuthors(reader):
	match_hash = {}
	match_count = 0
	row_count = 0
	for row in reader:
		row_count += 1
		if row[author_name_column] in match_hash:
			if match_hash[row[author_name_column]]:
				match_count += 1
			continue
		affiliation_set = set()
		match = False
		for affiliation in row[author_linkedin_affiliations_column]:
			affiliation_set.add(affiliation)
		for affiliation in row[author_journal_affiliations_column]:
			if affiliation in affiliation_set:
				match = True
				match_count += 1
		match_hash[row[author_name_column]] = match
	match_percentage = float(match_count)/row_count
	return match_percentage, row_count


def calculateUniversityAcceptancePercentage(reader, row_count):
	#based on linkedin affiliations
	#first, find all universities
	university_hash = {}
	for row in reader:
		author_affiliation = row[author_journal_affiliations_column]
		if author_affiliation not in university_hash:
			university_hash[author_affiliation] = 1
		else:
			university_hash[author_affiliation] += 1
	print "Showing journal acceptance rate per university"
	for university in university_hash.keys():
		print university, float(university_hash[author_affiliation])/row_count, '%'
	print 'Expected Average University Acceptance Rate:'
	print float(len(university_hash.keys()))/row_count


def calculatePercentageOfAffiliationOverlap(reader, row_count):
	match_counter = 0
	for row in reader:
		author_affiliations_set = set()
		author_affiliations_set.add(row[author_journal_affiliations_column])
		for affiliation in row[author_linkedin_affiliations_column].split(','):
			author_affiliations_set.add(affiliation)
		editor_affiliations_set = set()
		for affiliation in row[editor_linkedin_column].split(','):
			editor_affiliations_set.add(affiliation)
		overlap = auathor_affiliaitons_set.intersection(editor_affiliations_set)
		if len(overlap) > 0:
			match_counter += 1
	print 'Percentage of Author-Editor Affiliation Overlap:'
	print float(match_counter/row_count)

def main():
	new_csv_file_path = csv_file_path.split('.')[0] + '_final' + '.csv'
	csv_file = open(csv_file_path, 'r')
	reader = csv.reader(old_csv_file)
	match_percentage, row_count = calculatePercetAffiliationOverlap(reader)
	calculateUniversityAcceptancePercentage(reader, row_count)
	calculatePercentageOfAffiliationOverlap(reader, row_count)


if __name__ == '__main__':
	main()











