"""
Downloads imaging studies from YNHH VNA.
Each accession number is saved to a separate folder. If searching by MRN, each MRN's accession numbers are also saved to separate folders.
Within that folder, each series is saved to a separate subfolder, named with the series description.

If multiple keywords are specified, it is assumed that the study_id must have ALL keywords in the description in order to match.
To retrieve studies that have any of the keywords, query_terms each of the keywords separately to build a list of accession numbers,
then download based on the list.

Options include:
- Overwrite existing folders
- Exclude series with certain description keywords
- Limit by series description keywords
- Limit by modality
- Limit by study date

If searching by accession number or keyword, outputs files with the following structure:
save_directory/
> accession_num/
	> vibe_fs_axial_dynamic_post_[series_number]/
		> 0.dcm
		> 1.dcm
		> ...
		> metadata.xml
	> t2_haste_tra_trig_[series_number]/
		> ...
	> ...

If searching by MRN, it adds a folder layer for the MRN:
save_directory/
> MRN/
	> accession_num/
		> vibe_fs_axial_dynamic_post_[series_number]/
			> 0.dcm
			> ...

Usage:
	python vna_query.py
		#show GUI to guide user through downloading studies

	python vna_query.py mrn -q MR123456789
		#download studies for patient MRN MR123456789 and save in the current folder

	python vna_query.py accnum -q E123456789 E012345678 -s E:/dcms
		#download studies for accession numbers E123456789 and E012345678, and save in the folder E:/dcms

	python vna_query.py mrn -p E:/mrns.txt -s E:/dcms
		#download studies for all MRNs in mrns.txt and save in E:/dcms

	python vna_query.py keyword -q contrast mri abdom -d1 20160101 -r -l 10
		#find all accession numbers for 10 studies containing keywords "contrast", "mri" AND "abdom"
		#that took place in 2016 or later; review studies before downloading

	python vna_query.py accnum -p E:/accnums.txt -s E:/dcms -e sub cor -m MR -d1 20020101 -d2 20091231 -ovk
		#download studies for all accession numbers in accnums.txt and save in E:/dcms for studies between
		#2002 and 2009; ignore non-MR series, ignore series with "sub" or "cor" in the description, and
		#overwrite existing folders in E:/dcms if necessary; use verbose output; do not anonymize dicoms

Author: Clinton Wang, E-mail: clinton.wang@yale.edu, Github: https://github.com/clintonjwang/api-queries
"""

import argparse
import csv
import getpass
import requests
import os

#####################################
### Get user input
#####################################

def get_inputs_cmd(args):
	"""UI flow. Returns None if cancelled or terminated with error,
	else returns [user, pw, acc_nums, save_dir]."""
	user = input("Enter YNHH username: ")
	pw = getpass.getpass()

	if args.query is not None:
		if type(args.query) == list:
			query_terms = ('%20'.join(args.query)).replace(',', '')
		else:
			query_terms = args.query

	else:
		query_terms = input('Enter query_terms: ').replace(',', ' ').replace(" ","%20")

	return [user, pw, query_terms, vars(args)]

def init_options():
	options={}
	options["search_type"] = "accnum"
	options["start_date"] = None
	options["end_date"] = None
	options["modality"] = None
	options["save_dir"] = '.'
	options["exclude_terms"] = []
	options["verbose"] = False
	options["overwrite"] = True
	options["review"] = False

	return options

#####################################
### Subroutines
#####################################

def _search_montage(user, pw, search_terms):
	"""Use AcuoREST API to search VNA for study_id, series, and/or instance numbers associated with an accession number"""
	search_terms['format'] = 'json'
	search_terms["limit"] = "1"
	#search_terms["includefield"]="all"

	base_url = 'http://montage.ynhh.org'
	url = base_url + '/api/v1/index/rad/search/'

	if len(search_terms) > 0:
		query_str = '?' + '&'.join([term + '=' + search_terms[term] for term in search_terms])
		url += query_str

	r = requests.get(url, auth=(user, pw))
	if r.status_code == 403:
		raise ValueError('Access denied. Probably incorrect login information.')
	elif r.status_code >= 500:
		print(url)
		raise ValueError('Server exception. Make sure arguments were specified in the right format.')
	#if r.status_code != 200:
		#raise ValueError("Invalid request (response code %d) for URL: %s" % (r.status_code, url))
		
	return r, url

def get_first_date(study_events):
	date_time = None
	for event in study_events:
		date_time = event['date']
		if date_time is not None:
			return date_time[:date_time.find('T')]
	return "Not found"

def save_results(save_path, json_results):
	if not save_path.endswith('.csv'):
		raise ValueError("save_path must end with .csv")

	with open(save_path, 'w', newline='') as csvfile:
		header = ['mrn', 'accession_number', 'exam_description', 'approximate date', 'text']
		writer = csv.writer(csvfile)
		writer.writerow(header)
		for study in json_results['objects']:
			writer.writerow((study['patient_mrn'], study['accession_number'],
							  study['exam_type']['description'], get_first_date(study['events']), study['text']))

#####################################
### Main
#####################################

def main(args):
	"""Starting point for script"""
	ret = get_inputs_cmd(args)
	if ret is None:
		return
	[user, pw, query_terms, options] = ret

	search_terms = {}
	search_terms['q'] = "*" + query_terms + "*"
	#cmd = "curl -u %s:%s http://montage.ynhh.org/api/v1/index/rad/search/?q=*transarterial%20chemo*&format=json" % (user, pw)

	r, url = _search_montage(user, pw, search_terms)

	save_results(options['save_path'], r.json())

	#print("Searching database...")
	#mrns, acc_nums = collect_accnums(user, pw, query_terms, options)

if __name__ == "__main__":
	parser = argparse.ArgumentParser(description='Retrieve list of MRNs and accession numbers based on a Montage query_terms.')
	parser.add_argument('query', nargs='+', help='Montage query_terms (will search for the full query_terms, with wildcards at both ends)')
	parser.add_argument('-s', '--save_path', default='results.csv', help='save result to csv file')
	#parser.add_argument('-d1', '--start_date', help='limit date to those on or after this date (YYYYMMDD)')
	#parser.add_argument('-d2', '--end_date', help='limit date to those on or before this date (YYYYMMDD)')
	#parser.add_argument('-l', '--limit', help='limit number of studies to query_terms')
	
	args = parser.parse_args()
	main(args)