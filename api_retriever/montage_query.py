"""
Downloads YNHH reports from Montage.

Usage:
	WIP

Author: Clinton Wang, E-mail: clinton.wang@yale.edu, Github: https://github.com/clintonjwang/api-queries
"""

import argparse
from bs4 import BeautifulSoup
import csv
import getpass
from niftiutils import private as prv
import requests
import os

#####################################
### Get user input
#####################################

def get_inputs_cmd(args):
	"""UI flow."""
	user = input("Enter YNHH username: ")
	pw = getpass.getpass()

	"""	if args.query is not None:
			if type(args.query) == list:
				query_terms = ('%20'.join(args.query)).replace(',', '')
			else:
				query_terms = args.query

		else:
			query_terms = input('Enter query_terms: ').replace(',', ' ').replace(" ","%20")
	"""
	return [user, pw, vars(args)]

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

def _search_montage(user, pw, search_terms, resource_uri="/api/v1/index/rad/search/"):
	"""Query AcuoREST API to search VNA"""
	search_terms['format'] = 'json'
	search_terms["limit"] = "1000"

	base_url = 'http://montage.ynhh.org'
	url = base_url + resource_uri

	if len(search_terms) > 0:
		query_str = '?' + '&'.join([term + '=' + search_terms[term] for term in search_terms])
		url += query_str

	r = requests.get(url, auth=(user, pw))
	if r.status_code == 403:
		raise ValueError('Access denied. Probably incorrect login information.')
	elif r.status_code >= 500:
		print(url)
		raise ValueError('Server exception. Make sure arguments were specified in the right format.')
	elif r.status_code != 200:
		raise ValueError("Invalid request (response code %d) for URL: %s" % (r.status_code, url))
		
	return r, url

def _get_montage_report(user, pw, search_terms):
	"""Use AcuoREST API to search VNA for study_id, series, and/or instance numbers associated with an accession number"""
	return _search_montage(user, pw, search_terms, '/api/v1/report/')

def get_mrn_report(user, pw, mrn):
	"""Get report history based on MRN"""

	if prv.is_encoded(mrn):
		mrn = prv.decode(mrn)
	return _get_montage_report(user, pw, {'patient_mrn': mrn})

def get_accnum_report(user, pw, acc_num):
	"""Get report history based on accession number"""

	if prv.is_encoded(acc_num):
		acc_num = prv.decode(acc_num)
	return _get_montage_report(user, pw, {'accession_number': acc_num})

def parse_json(json_results):
	"""Translate patient report history from json to dict with a subset of fields"""

	keys = ['mrn', 'accession_number', 'exam_description', 'exam_id', 'date_exam_completed', 'text']
	study = json_results['objects'][0]
	#for study in json_results['objects']:
	values = [prv.encode(study['patient_mrn']), prv.encode(study['accession_number']),
			  study['exam_type']['description'], study['exam_type']['id'],
			  get_exam_completed_date(study['events']), parse_html(study['text'])]

	return dict(zip(keys, values))

def get_exam_completed_date(study_events):
	"""Gets the date that an exam was completed.
	Takes dict of study events as input"""

	date_time = [event['date'] for event in study_events if event['event_type'] == 5][0]
	if date_time is None:
		date_time = [event['date'] for event in study_events if event['date'] is not None][0]
	return date_time[:date_time.find('T')]

def parse_html(html_txt):
	"""Parse html-formatted report text"""

	soup = BeautifulSoup(html_txt, 'html.parser')
	return '\n'.join([line for line in soup.stripped_strings])

def save_results(save_path, json_results):
	"""Translate patient report history from json to csv file with a subset of fields"""

	if not save_path.endswith('.csv'):
		raise ValueError("save_path must end with .csv")

	with open(save_path, 'w', newline='') as csvfile:
		header = ['mrn', 'accession_number', 'exam_description', 'exam_id', 'date_exam_completed', 'text']
		writer = csv.writer(csvfile)
		writer.writerow(header)
		for study in json_results['objects']:
			writer.writerow((prv.encode(study['patient_mrn']), prv.encode(study['accession_number']),
							  study['exam_type']['description'], study['exam_type']['id'],
							  get_exam_completed_date(study['events']), parse_html(study['text'])))

#####################################
### Main
#####################################

def main(args):
	ret = get_inputs_cmd(args)
	if ret is None:
		return
	[user, pw, args] = ret

	if args['query'] is not None:
		search_terms = {}
		search_terms['q'] = "*" + args['query'] + "*"

		r, url = _search_montage(user, pw, search_terms)

	else:
		search_terms = {}
		if args['acc_num'] is not None:
			search_terms['accession_number'] = args['acc_num'][0]
		else:
			search_terms['patient_mrn'] = args['mrn'][0]

		r, url = _get_montage_report(user, pw, search_terms)

	save_results(args['save_path'], r.json())

if __name__ == "__main__":
	parser = argparse.ArgumentParser(description='Retrieve list of MRNs and accession numbers based on a Montage query_terms.')
	action = parser.add_mutually_exclusive_group() # if not included, prompts for additional info in terminal
	action.add_argument('-q', '--query', nargs='+', help='Montage query_terms (will search for the full query_terms, with wildcards at both ends)')
	action.add_argument('-m', '--mrn', nargs='+', help='Patient MRN')
	action.add_argument('-a', '--acc_num', nargs='+', help='Accession Number')
	parser.add_argument('-s', '--save_path', default='results.csv', help='save result to csv file')
	#parser.add_argument('-d1', '--start_date', help='limit date to those on or after this date (YYYYMMDD)')
	#parser.add_argument('-d2', '--end_date', help='limit date to those on or before this date (YYYYMMDD)')
	#parser.add_argument('-l', '--limit', help='limit number of studies to query_terms')
	
	args = parser.parse_args()
	main(args)