"""
Downloads imaging studies from YNHH VNA.
Each accession number is saved to a separate folder. If searching by MRN, each MRN's accession numbers are also saved to separate folders.
Within that folder, each series is saved to a separate subfolder, named with the series description.

If multiple keywords are specified, it is assumed that the study_id must have ALL keywords in the description in order to match.
To retrieve studies that have any of the keywords, query each of the keywords separately to build a list of accession numbers,
then download based on the list.

Options include:
- Overwrite existing folders
- Exclude series with certain description keywords
- Limit by series description keywords
- Limit by modality
- Limit by study date

Usage:
	python api_queries.py
		#show GUI to guide user through downloading studies

	python api_queries.py mrn -q MR123456789
		#download studies for patient MRN MR123456789 and save in the current folder

	python api_queries.py accnum -q E123456789 E012345678 -s E:/dcms
		#download studies for accession numbers E123456789 and E012345678, and save in the folder E:/dcms

	python api_queries.py keyword -q contrast mri abdom -d1 20020101
		#find all accession numbers for studies containing keywords "contrast", "mri" AND "abdom"
		#that took place in 2002 or later

	python api_queries.py mrn -p E:/mrns.txt -s E:/dcms
		#download studies for all MRNs in mrns.txt and save in E:/dcms

	python api_queries.py accnum -p E:/accnums.txt -s E:/dcms -e sub cor -o -m MR -d1 20020101 -d2 20091231
		#download studies for all accession numbers in accnums.txt and save in E:/dcms for studies between
		#2002 and 2009; ignore non-MR series, ignore series with "sub" or "cor" in the description, and
		#overwrite existing folders in E:/dcms if necessary

Author: Clinton Wang, E-mail: clinton.wang@yale.edu, Github: https://github.com/clintonjwang/api-queries
"""

import argparse
import datetime
from dicom.examples import anonymize
import easygui
import getpass
import itertools
import requests
import os
import shutil
import time

#####################################
### Core methods
#####################################

def collect_studies(user, pw, query_terms, options):
	"""Collect all studies for a query
	
	Keyword arguments:
	acc_nums -- a list of accession numbers as strings
	user -- username (optional)
	pw -- password (optional)
	query_terms -- list of query terms, formatted as strings
	options -- dict with search parameters; the keys ['search_type', '', ''] are mandatory
	"""
	study_search_terms = {}
	series_search_terms = {}
	instance_search_terms = {}
	if options['start_date'] is not None:
		if options['end_date'] is not None:
			study_search_terms["StudyDate"] = options['start_date'] + "-" + options['end_date']
		else:
			study_search_terms["StudyDate"] = options['start_date'] + "-" + datetime.date.today().strftime("%Y%m%d")
	elif options['end_date'] is not None:
		study_search_terms["StudyDate"] = "19000101-" + options['end_date']

	if options['modality'] is not None:
		study_search_terms["ModalitiesInStudy"] = "*" + options['modality'] + "*"
		series_search_terms["Modality"] = options['modality']

	study_dict = {}
	if options['search_type'] == "accnum":
		for acc_num in query_terms:
			study_search_terms["AccessionNumber"] = acc_num

			r, url = _search_vna(user, pw, search_terms=study_search_terms)
			if r.status_code == 204:
				print('Accession number', acc_num, 'has no studies associated with it.')
				continue
			
			study_id = r.json()[0]['0020000D']['Value'][0]
			study_info = (r.json()[0]['00081030']['Value'][0], r.json()[0]['00080020']['Value'][0])

			instance_dict = _create_instance_dict(user, pw, study_id, series_search_terms, instance_search_terms)
			study_dict[acc_num] = (study_id, instance_dict, study_info)

	elif options['search_type'] == "mrn":
		for mrn in query_terms:
			study_search_terms["PatientId"] = mrn

			r, url = _search_vna(user, pw, search_terms=study_search_terms)
			if r.status_code == 204:
				print('MRN', mrn, 'has no studies associated with it.')
				continue

			accnums_studyids_descr = [(json_data['00080050']['Value'][0], json_data['0020000D']['Value'][0],
						(json_data['00081030']['Value'][0], json_data['00080020']['Value'][0])) for json_data in r.json()]
			
			study_dict[mrn] = {}
			for acc_num, study_id, study_info in accnums_studyids_descr:
				instance_dict = _create_instance_dict(user, pw, study_id, series_search_terms, instance_search_terms)
				study_dict[mrn][acc_num] = (study_id, instance_dict, study_info)

	elif options['search_type'] == 'keyword':
		for keywords in itertools.permutations(query_terms):
			study_search_terms["StudyDescription"] = '*' + '*'.join(keywords) + '*'
			
			r, url = _search_vna(user, pw, search_terms=study_search_terms)
			if r.status_code == 204:
				continue

			accnums_studyids_descr = [(json_data['00080050']['Value'][0], json_data['0020000D']['Value'][0],
						(json_data['00081030']['Value'][0], json_data['00080020']['Value'][0])) for json_data in r.json()]

			for acc_num, study_id, study_info in accnums_studyids_descr:
				instance_dict = _create_instance_dict(user, pw, study_id, series_search_terms, instance_search_terms)
				study_dict[acc_num] = (study_id, instance_dict, study_info)

	else:
		raise ValueError(options['search_type'])

	return study_dict

def review_studies(study_dict, search_type):
	"""Allows the user to select among queried studies."""
	if search_type in ["accnum", "keyword"]:
		choices = []
		for acc_num in study_dict:
			_, instance_dict, study_info = study_dict[acc_num]
			study_description, study_date = study_info
			choices.append("%s / %s (%d series) | %s" % (acc_num, study_description, len(instance_dict), reformat_date(study_date)))
		
		selection = easygui.multchoicebox(title='Select studies to download', choices=choices)
		if selection is None:
			return None
		else:
			selected_accnums = [x[:x.find(' ')] for x in selection if x != "Add more choices"]
			study_dict = {acc_num: study_dict[acc_num] for acc_num in selected_accnums}
	
	elif search_type == "mrn":
		choices = []
		for mrn in study_dict:
			for acc_num in study_dict[mrn]:
				_, instance_dict, study_info = study_dict[mrn][acc_num]
				study_description, study_date = study_info
				choices.append("%s | %s / %s (%d series) | %s" % (mrn, acc_num, study_description, len(instance_dict), reformat_date(study_date)))
		
		selection = easygui.multchoicebox(title='Select studies to download', choices=choices)
		if selection is None:
			return None
		else:
			selected_mrn_accnums = [x[:x.find('/')-1].split('|') for x in selection if x != "Add more choices"]
			selected_mrns = set([x[0].strip() for x in selected_mrn_accnums])
			selected_accnums = set([x[1].strip() for x in selected_mrn_accnums])
			study_dict = {mrn: study_dict[mrn] for mrn in selected_mrns}
			for mrn in study_dict:
				study_dict[mrn] = {acc_num: study_dict[mrn][acc_num] for acc_num in selected_accnums if acc_num in study_dict[mrn]}

	else:
		raise ValueError("Invalid search_type ", search_type)

	return study_dict

def retrieve_studies(user, pw, study_dict, options, metadata_only=False, verbose=False, get_series_name=None):
	"""Download all studies associated with an accession number
	Each accession number (study_id) is saved to a separate folder named with its study_id UID.
	Within that folder, each series is saved to a separate subfolder, named with the series description.
	
	Keyword arguments:
	user -- username
	pw -- password
	studies -- a list of tuples; each tuple study_id/series/instance 
	options -- dict of parameters for how to retrieve the studies; must have 'save_dir', 'search_type' and 'overwrite' keys
	metadata_only -- if True, only retrieves image metadata xmls
	verbose -- if True, prints to screen as each series is loaded, else only prints as each study_id is loaded (default False)
	get_series_name -- specify a custom method for naming series subfolders (optional)
	"""

	if get_series_name is None:
		def get_series_name(metadata_txt):
			txt = metadata_txt
			search = '<DicomAttribute tag="0008103E" vr="LO" keyword="SeriesDescription">\r\n      <Value number="1">'
			index = txt.find(search) + len(search)
			series_name = txt[index:index + txt[index:].find("</Value>")].lower()
			series_name = series_name.replace("/", "-")
			series_name = series_name.replace("\\", "-")
			series_name = series_name.replace(":", "-")
			series_name = series_name.replace("?", "")
			series_name = series_name.replace("*", "")

			return series_name

	tot_time = time.time()

	if options['search_type'] in ["accnum", "keyword"]:
		for acc_num in study_dict:
			print("= Loading accession number", acc_num)
			study_id, instance_dict, _ = study_dict[acc_num]
			retrieve_study_from_id(user, pw, study_id, instance_dict, os.path.join(options['save_dir'], acc_num),
				options, metadata_only, verbose, get_series_name)
	
	elif options['search_type'] == "mrn":
		for mrn in study_dict:
			print("=== Loading MRN", mrn)
			for acc_num in study_dict[mrn]:
				print("= Loading accession number", acc_num)
				study_id, instance_dict, _ = study_dict[mrn][acc_num]
				retrieve_study_from_id(user, pw, study_id, instance_dict, os.path.join(options['save_dir'], mrn, acc_num),
					options, metadata_only, verbose, get_series_name)

	else:
		raise ValueError("Invalid options['search_type'] ", options['search_type'])
			
	#if verbose:
	#	print("Series loaded: ", len(series)-skip_ser, "/", len(series), sep="")
	#	print("\nTotal images loaded:", total)
	#	print("Images skipped:", skip_inst)

	print("Time elapsed: %.1fs" % (time.time()-tot_time))

#####################################
### Get user input
#####################################

def get_inputs_gui():
	"""UI flow. Returns None if cancelled or terminated with error,
	else returns [user, pw, acc_nums, save_dir]."""
	if not easygui.msgbox(('This utility downloads studies from the YNHH VNA. It can be queried by accession number, '
						'MRN or study keyword. It saves each study to its own folder, with each series stored in a '
						'separate subfolder.\n')):
		return None

	try:
		options = {}

		fieldValues = easygui.multpasswordbox(msg='Enter credentials to access VNA.', fields=["Username", "Password"])
		if fieldValues is None:
			return None
		user, pw = fieldValues

		msg = "How do you want to query studies?"
		choices = ["Accession Numbers", "Patient MRNs", "Study Keywords"]
		reply = easygui.buttonbox(msg, msg, choices=choices)
		if reply == choices[0]:
			options['search_type'] = "accnum"
			options['review'] = False
		elif reply == choices[1]:
			options['search_type'] = "mrn"
			options['review'] = True
		elif reply == choices[2]:
			options['search_type'] = "keyword"
			options['review'] = True
		else:
			return None

		msg = "Enter query parameters (only " + reply + " is mandatory)"
		title = "Query parameters"
		fieldNames = [reply + " separated by commas or spaces",
					"Start date (YYYYMMDD format)",
					"End date (YYYYMMDD format)",
					"Modality to limit the search to (use the 2-letter code used in DICOM, i.e. MR, CR, etc.)",
					"Keywords for series to exclude if they appear in the description (e.g. sub, localizer, cor)"]#,
					#"Keywords for series to include if they appear in the description"]
		fieldValues = easygui.multenterbox(msg, title, fieldNames)

		while True:
			if fieldValues is None:
				return None

			errmsg = ""
			if fieldValues[0].strip() == "":
				errmsg = errmsg + ('"%s" is a required field.\n\n' % fieldNames[0])
			elif len(fieldValues[1].strip()) > 8:
				errmsg = errmsg + ('"%s" is not in YYYYMMDD format.\n\n' % fieldValues[1])
			elif len(fieldValues[2].strip()) > 8:
				errmsg = errmsg + ('"%s" is not in YYYYMMDD format.\n\n' % fieldValues[2])
			elif len(fieldValues[3].strip()) > 2:
				errmsg = errmsg + ('"%s" is not in the DICOM 2-letter format.\n\n' % fieldValues[3])

			if errmsg == "":
				break # no problems found

			fieldValues = easygui.multenterbox(errmsg, title, fieldNames, fieldValues)

		query_terms = fieldValues[0].replace(',', ' ').split()
		options['start_date'] = _parse_field_value(fieldValues[1])
		options['end_date'] = _parse_field_value(fieldValues[2])
		options['modality'] = _parse_field_value(fieldValues[3])
		options['exclude_terms'] = fieldValues[4].replace(',', ' ').split()
		#options['include_terms'] = fieldValues[5].replace(',', ' ').split()
		options['verbose'] = True

		options['save_dir'] = easygui.diropenbox(msg='Select a folder to save your images to.')
		if options['save_dir'] is None:
			return None

		if len(os.listdir(options['save_dir'])) > 0:
			options['overwrite'] = easygui.ynbox("Overwrite any existing folders?")
			if options['overwrite'] is None:
				return None

	except:
		easygui.exceptionbox()
		return None

	return [user, pw, query_terms, options]

def get_inputs_cmd(args):
	"""UI flow. Returns None if cancelled or terminated with error,
	else returns [user, pw, acc_nums, save_dir]."""
	user = input("Enter YNHH username: ")
	pw = getpass.getpass()

	if args.txt_path is not None:
		try:
			with open(args.txt_path, 'r') as f:
				query_terms = [z for x in f.readlines() for z in x.replace(',', ' ').split()]
		except FileNotFoundError:
			print("ERROR: Invalid path", args.txt_path)
			return None

	elif args.query is not None:
		if type(args.query) == list:
			query_terms = args.query
		else:
			query_terms = [args.query]

	else:
		query_terms = input('Enter query terms (separated by commas or spaces): ').replace(',', ' ').split()

	if args.exclude_terms is not None:
		if type(args.exclude_terms) != list:
			args.exclude_terms = [args.exclude_terms]

	return [user, pw, query_terms, vars(args)]

#####################################
### Subroutines
#####################################

def retrieve_study_from_id(user, pw, study_id, instance_dict, save_dir, options, metadata_only, verbose, get_series_name):
	if os.path.exists(save_dir):
		if not options['overwrite']:
			print(save_dir, "may already have been downloaded (folder already exists in target directory). Skipping.")
			return
		else:
			shutil.rmtree(save_dir)
			while os.path.exists(save_dir):
				sleep(100)
				
	total = 0
	skip_ser = 0
	skip_inst = 0
	rmdir = []

	for series_id in instance_dict:
		#Make a folder for it
		series_dir = save_dir + "\\" + series_id
		if not os.path.exists(series_dir):
			os.makedirs(series_dir)

		#Retrieve metadata xml
		r, url = _retrieve_vna(user, pw, filepath = series_dir+"\\metadata.xml",
						  study_id=study_id, series=series_id, metadata=True)
		if r is None:
			skip_ser += 1
			if verbose:
				print("Skipping series %s (no instances found)." % series_id)
			continue

		#Rename the folder based on the metadata
		series_name = get_series_name(r.text)

		while os.path.exists(save_dir + "\\" + series_name):
			series_name += "+"
		try:
			os.rename(series_dir, save_dir + "\\" + series_name)
		except:
			series_name = "UnknownProtocol"
			while os.path.exists(save_dir + "\\" + series_name):
				series_name += "+"
			os.rename(series_dir, save_dir + "\\" + series_name)
		series_dir = save_dir + "\\" + series_name


		#Determine whether the series should be excluded based on the metadata
		skip = False
		try:
			for exc_keyword in options['exclude_terms']:
				if exc_keyword in series_name:
					skip_ser += 1
					if verbose:
						print("Skipping series", series_name)
					rmdir.append(series_name)
					skip = True
					break
		except:
			pass

		if skip or metadata_only:
			continue


		#Load the actual images
		if verbose:
			print("Loading series:", series_name)

		for count, instance_id in enumerate(instance_dict[series_id]):
			r, _ = _retrieve_vna(user, pw, filepath=series_dir+"\\"+str(count)+".dcm",
						  study_id=study_id, series=series_id, instance=instance_id)

			if r is not None:
				skip_inst += 1

		total += count

	if len(rmdir)>0 and not os.path.exists(save_dir+"\\others"):
		os.makedirs(save_dir+"\\others")

		for d in rmdir:
			os.rename(save_dir + "\\" + d, save_dir + "\\others\\" + d)

def reformat_date(date, in_format="%Y%m%d", out_format="%x"):
	return datetime.datetime.strptime(date, in_format).strftime(out_format)

def _parse_field_value(txt):
	if txt.strip() == "":
		return None
	else:
		return txt.strip()

def _create_instance_dict(user, pw, study_id, series_search_terms, instance_search_terms):
	r, url = _search_vna(user, pw, study_id=study_id, search_terms=series_search_terms)
	try:
		study_info = r.json()
	except Exception as e:
		raise ValueError('Search for study_id ' + study_id + ' encountered an error: ' + e)

	series = set([series_id['0020000E']['Value'][0] for series_id in study_info])

	instance_dict = {}
	for series_id in series:
		r, url = _search_vna(user, pw, study_id=study_id, series=series_id, search_terms=instance_search_terms)

		series_info = r.json()
		instance_dict[series_id] = [instance_id['00080018']['Value'][0] for instance_id in series_info]

	return instance_dict

def _search_vna(user, pw, study_id=None, series=None, region='test', args=None, search_terms=None):
	"""Use AcuoREST API to search VNA for study_id, series, and/or instance numbers associated with an accession number"""

	if region == 'test':
		host = 'vnatest1vt'
		port = '8083'
	elif region == 'prod':
		host = '10.47.11.221'
		port = '8083'
	else:
		raise ValueError("Unsupported region")

	url = ''.join(['http://', host, ':', port,
				   "/AcuoREST/dicomrs/search/studies"])

	if study_id is not None:
		url += "/" + study_id + "/series"

		if series is not None:
			url += "/" + series + "/instances"

	#search_terms["limit"]="2"
	#search_terms["includefield"]="all"
	if len(search_terms) > 0:
		query_str = '?' + '&'.join([term + '=' + search_terms[term] for term in search_terms])
		url += query_str

	r = requests.get(url, auth=(user, pw))
	if r.status_code == 403:
		raise ValueError('Access denied. Probably incorrect login information.')
	elif r.status_code >= 500:
		raise ValueError('Internal server error. Try again in a few minutes or contact IT.')
	#if r.status_code != 200:
		#raise ValueError("Invalid request (response code %d) for URL: %s" % (r.status_code, url))
		
	return r, url

def _retrieve_vna(user, pw, filepath, study_id=None, series=None, instance=None, region='test', metadata=False):
	"""Retrieve dicom files and metadata associated with a study_id/series/instance.
	If metadata is true, filepath should end in xml. Else end in dcm."""

	if region == 'test':
		host = 'vnatest1vt'
		port = '8083'
	elif region == 'prod':
		host = '10.47.11.221'
		port = '8083'
	else:
		raise ValueError("Unsupported region")

	if metadata:
		url = ''.join(['http://', host, ':', port,
					   "/AcuoREST/dicomrs/retrieve/studies/",
						study_id])

		if series is not None:
			url += "/series/" + series
			if instance is not None:
				url += "/instance_dict/" + instance

		url += "/metadata"+"?contentType=application/xml"

		r = requests.get(url, auth=(user, pw)) #, headers=headers

		if r.status_code != 200:
			#print("Invalid request (response code %d) for URL: %s" % (r.status_code, url))
			#raise ValueError("Invalid request (response code %d) for URL: %s" % (r.status_code, url))
			return None, url

		with open(filepath, 'wb') as fd:
			for chunk in r.iter_content(chunk_size=128):
				fd.write(chunk)

	else:
		url = ''.join(['http://', host, ':', port,
					   "/AcuoREST/wadoget?requestType=WADO&contentType=application/dicom&studyUID=",
						study_id])

		if series is not None:
			url += "&seriesUID=" + series
			if instance is not None:
				url += "&objectUID=" + instance

		r = requests.get(url, auth=(user, pw)) #, headers=headers

		if r.status_code != 200:
			#print("Skipped instance:", instance)
			#raise ValueError("Invalid request (response code %d) for URL: %s" % (r.status_code, url))
			return None, url

		save_dir = os.path.dirname(filepath)
		with open(save_dir + "\\temp.dcm", 'wb') as fd:
			for chunk in r.iter_content(chunk_size=128):
				fd.write(chunk)

		anonymize.anonymize(filename = save_dir + "\\temp.dcm", output_filename=filepath)

		os.remove(save_dir + "\\temp.dcm")
		
	return r, url

#####################################
### Main
#####################################

def main(args):
	"""Starting point for script"""
	if args.search_type is None:
		ret = get_inputs_gui()
	else:
		ret = get_inputs_cmd(args)
	if ret is None:
		return

	[user, pw, query_terms, options] = ret

	print("Searching database...")
	study_info = collect_studies(user, pw, query_terms, options)

	if len(study_info) == 0:
		return

	if options['review']:
		study_info = review_studies(study_info, options['search_type'])
		if study_info is None:
			return

	print("Downloading studies...")
	retrieve_studies(user, pw, study_info, options, verbose=options['verbose'])

if __name__ == "__main__":
	parser = argparse.ArgumentParser(description='Download imaging studies from the YNHH VNA API. \
						Uses GUI if no positional argument is specified.')
	parser.add_argument('search_type', nargs='?', choices=['accnum', 'mrn', 'keyword'],
				help='whether to search by accession numbers, MRNs, or study_id keywords; required if any other args are specified')
	action = parser.add_mutually_exclusive_group() # if not included, prompts for additional info in terminal
	action.add_argument('-q', '--query', nargs='+', help='one or more space-separated query terms')
	action.add_argument('-p', '--txt_path', help='path to a txt file containing query terms separated by line breaks, spaces or commas')

	parser.add_argument('-s', '--save_dir', default=".", help='directory to save downloaded studies')
	parser.add_argument('-e', '--exclude_terms', nargs='+',
				help='exclude series if they have any of these keywords in the description')
	#parser.add_argument('-i', '--include_series', nargs='+',
	#			help='only download series if they have these any of these keywords in the description')
	parser.add_argument('-m', '--modality', help='only include series of a specific modality (MR, CT, etc.)')
	parser.add_argument('-d1', '--start_date', help='limit study_id date to those on or after this date (YYYYMMDD)')
	parser.add_argument('-d2', '--end_date', help='limit study_id date to those on or before this date (YYYYMMDD)')
	parser.add_argument('-r', '--review', action='store_true', help='review queried study/series descriptions before downloading')
	parser.add_argument('-o', '--overwrite', action='store_true', help='overwrite any existing folders in save_dir')
	parser.add_argument('-v', '--verbose', action='store_true', help='display download progress')

	args = parser.parse_args()
	main(args)