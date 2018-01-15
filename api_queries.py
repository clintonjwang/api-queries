"""
Downloads imaging studies from YNHH VNA based on accession number.

If multiple keywords are specified, it is assumed that the study must have ALL keywords in the description in order to match.
To retrieve studies that have any of the keywords, query each of the keywords separately to build a list of accession numbers,
then download based on the list.

Advanced options include:
- Force overwrite existing folders (TBD)
- Exclude series with certain description keywords (TBD)
- Limit by series description keywords (TBD)
- Limit by modality (TBD)
- Limit by study date (TBD)

Usage:
	python api_queries.py mrn
		#download studies for one or more patient MRNs, specified using interactive command line

	python api_queries.py keyword -a
		#download studies for one or more study description keywords, specified using interactive command line; show advanced options

	python api_queries.py accnum -g
		#download studies for one or more accession numbers, specified using interactive GUI

	python api_queries.py accnum -ag
		#download studies for one or more accession numbers, specified using interactive GUI; show advanced options

	python api_queries.py mrn -q MR123456789
		#download studies for patient MRN MR123456789 and save in the current folder

	python api_queries.py accnum -q E123456789 E012345678 -s E:/dcms
		#download studies for accession numbers E123456789 and E012345678, and save in the folder E:/dcms

	python api_queries.py keyword -q contrast mri abdom
		#find all accession numbers for the studies containing keywords "contrast", "mri" AND "abdom"

	python api_queries.py mrn -p E:/mrns.txt -s E:/dcms
		#download studies for all MRNs in mrns.txt and save in E:/dcms

Author: Clinton Wang, E-mail: `clinton.wang@yale.edu`, Github: `https://github.com/clintonjwang/api-queries`
"""

#StudyDate=20130509-20130510
#ModalitiesInStudy=*MR*
#PatientID
#StudyDescription=*Yale*
#SeriesDescription=*Yale*

from dicom.examples import anonymize
import argparse
import getpass
import itertools
import requests
import os
import shutil
import time

def collect_studies(query_terms, user=None, pw=None, region="prod", options):
	"""Download all studies associated with an accession number
	Each accession number (study) is saved to a separate folder named with its study UID.
	Within that folder, each series is saved to a separate subfolder, named with the series description.
	
	Keyword arguments:
	acc_nums -- a list of accession numbers as strings
	target_dir -- the directory to save the images to
	user -- username (optional)
	pw -- password (optional)
	region -- region to search in (default prod)
	options -- dict with search parameters; the keys ['search_type', '', ''] are mandatory
	exclude_terms -- a list of terms to look for to exclude irrelevant protocols (optional)
	verbose -- if True, prints to screen as each series is loaded, else only prints as each study is loaded (default False)
	get_protocol_name -- specify a custom method for naming series subfolders (optional)
	"""

	if user is None:
		user = input("Enter YNHH username: ")
	if pw is None:
		pw = getpass.getpass()

	tot_time = time.time()
	if options['search_type'] == "accnum":
		for acc_num in primary_search_terms:
			ret = get_accnum_instances(user, pw, region, search_by, primary_search_term=acc_num, options=options)
			if ret is None:
				continue
			else:
				study, series, instances = ret

			acc_num_dir = target_dir + "\\" + acc_num
			if os.path.exists(acc_num_dir):
				if not overwrite:
					print(acc_num_dir, "may already have been downloaded (folder already exists in target directory). Skipping.")
					continue
				else:
					shutil.rmtree(acc_num_dir)
					while os.path.exists(acc_num_dir):
						sleep(100)
						
			#os.makedirs(acc_num_dir)

def retrieve_studies(primary_search_terms, target_dir, user=None, pw=None, region="prod", metadata_only=False,
		exclude_terms=[], verbose=False, overwrite=False, get_protocol_name=None, search_by="accnum", options=None):
	"""Download all studies associated with an accession number
	Each accession number (study) is saved to a separate folder named with its study UID.
	Within that folder, each series is saved to a separate subfolder, named with the series description.
	
	Keyword arguments:
	acc_nums -- a list of accession numbers as strings
	target_dir -- the directory to save the images to
	user -- username (optional)
	pw -- password (optional)
	region -- region to search in (default prod)
	verbose -- if True, prints to screen as each series is loaded, else only prints as each study is loaded (default False)
	get_protocol_name -- specify a custom method for naming series subfolders (optional)
	"""

	if user is None:
		user = input("Enter YNHH username: ")
	if pw is None:
		pw = getpass.getpass()

	if get_protocol_name is None:
		def get_protocol_name(metadata_txt):
			txt = metadata_txt
			search = '<DicomAttribute tag="0008103E" vr="LO" keyword="SeriesDescription">\r\n      <Value number="1">'
			index = txt.find(search) + len(search)
			protocol_name = txt[index:index + txt[index:].find("</Value>")].lower()
			protocol_name = protocol_name.replace("/", "-")
			protocol_name = protocol_name.replace("\\", "-")
			protocol_name = protocol_name.replace(":", "-")
			protocol_name = protocol_name.replace("?", "")
			protocol_name = protocol_name.replace("*", "")

			return protocol_name

	tot_time = time.time()
	if search_by == "accnum":
		for acc_num in primary_search_terms:
			ret = get_accnum_instances(user, pw, region, search_by, primary_search_term=acc_num, options=options)
			if ret is None:
				continue
			else:
				study, series, instances = ret

			acc_num_dir = target_dir + "\\" + acc_num
			if os.path.exists(acc_num_dir):
				if not overwrite:
					print(acc_num_dir, "may already have been downloaded (folder already exists in target directory). Skipping.")
					continue
				else:
					shutil.rmtree(acc_num_dir)
					while os.path.exists(acc_num_dir):
						sleep(100)
						
			#os.makedirs(acc_num_dir)

			total = 0
			skip_ser = 0
			skip_inst = 0
			rmdir = []

			t = time.time()
			for ser in instances:
				if verbose:
					print("Loading metadata for series", ser)
				series_dir = acc_num_dir + "\\" + ser

				if not os.path.exists(series_dir):
					os.makedirs(series_dir)

				r, url = _retrieve_vna(user, pw, region=region, filepath = series_dir+"\\metadata.xml",
								  study=study, series=ser, metadata=True)
				if r is None:
					skip_ser += 1
					if verbose:
						print("Skipping series (no instances found).")
					continue

				protocol_name = get_protocol_name(r.text)

				while os.path.exists(acc_num_dir + "\\" + protocol_name):
					protocol_name += "+"
				try:
					os.rename(series_dir, acc_num_dir + "\\" + protocol_name)
				except:
					protocol_name = "UnknownProtocol"
					while os.path.exists(acc_num_dir + "\\" + protocol_name):
						protocol_name += "+"
					os.rename(series_dir, acc_num_dir + "\\" + protocol_name)

				series_dir = acc_num_dir + "\\" + protocol_name


				skip = False
				for exc_keyword in exclude_terms:
					if exc_keyword in protocol_name:
						skip_ser += 1
						skip = True
						if verbose:
							print("Skipping series with description", protocol_name)
						rmdir.append(protocol_name)
						break

				if skip or metadata_only:
					continue

				if verbose:
					print("Loading images for series with description:", protocol_name)

				for count, inst in enumerate(instances[ser]):
					r, _ = _retrieve_vna(user, pw, region=region, filepath = series_dir+"\\"+str(count)+".dcm",
								  study=study, series=ser, instance=inst)

					if r is not None:
						skip_inst += 1

				total += count

			if len(rmdir)>0 and not os.path.exists(acc_num_dir+"\\others"):
				os.makedirs(acc_num_dir+"\\others")

				for d in rmdir:
					os.rename(acc_num_dir + "\\" + d, acc_num_dir + "\\others\\" + d)

			if verbose:
				print("Series loaded: ", len(series)-skip_ser, "/", len(series), sep="")
				print("\nTotal images loaded:", total)
				print("Images skipped:", skip_inst)
				
	print("Time elapsed: %.1fs" % (time.time()-tot_time))

def get_accnum_instances(user, pw, region, search_by, primary_search_term, options):
	study_search_terms = {}
	series_search_terms = {}
	instance_search_terms = {}
	if search_by == 'accnum':
		study_search_terms["AccessionNumber"] = primary_search_term
	elif search_by == 'mrn':
		study_search_terms["PatientId"] = primary_search_term
	elif search_by == 'keyword':
		study_search_terms["StudyDescription"] = primary_search_term
	else:
		raise ValueError(search_by)

	if 'modality' in options:
		study_search_terms["ModalitiesInStudy"] = options['modality']
		series_search_terms["Modality"] = options['modality']

	r, url = _search_vna(user, pw, region=region, search_terms=study_search_terms)
	if r.status_code == 403:
		raise ValueError('Access denied. Probably incorrect login information.')
	elif r.status_code == 204:
		print('Accession number', acc_num, 'has no studies associated with it.')
		return None
	
	study = r.json()[0]['0020000D']['Value'][0]

	r, url = _search_vna(user, pw, region=region, study=study, search_terms=series_search_terms)
	try:
		study_info = r.json()
	except:
		print('Search for ', primary_search_term, 'with study ID', study, 'encountered an unknown error.')
		return None

	series = list(set([ser['0020000E']['Value'][0] for ser in study_info]))

	instances = {}

	for ser in series:
		r, url = _search_vna(user, pw, region=region, study=study, series=ser, search_terms=instance_search_terms)
		series_info = r.json()
		instances[ser] = [inst['00080018']['Value'][0] for inst in series_info]

	return study, series, instances

def get_inputs_gui(args):
	"""UI flow. Returns None if cancelled or terminated with error,
	else returns [user, pw, acc_nums, save_dir]."""
	import easygui

	if not easygui.msgbox(('This utility retrieves studies from the YNHH VNA given a list of accession numbers. '
						'It saves each study to its own folder, with each series stored in a separate subfolder.\n'
						'NOTE: The program will not overwrite existing folders. '
						'This means that if the program is terminated early and rerun, the interrupted study may be incomplete.\n')):
		return None

	try:
		fieldValues = easygui.multpasswordbox(msg='Enter credentials to access VNA.', fields=["Username", "Password"])
		if fieldValues is None:
			return None
		user, pw = fieldValues

		exclude_terms = easygui.enterbox(msg="Terms to exclude if they appear in part of a protocol name (e.g. sub, localizer, cor).")
		if exclude_terms is None:
			return None
		else:
			exclude_terms = exclude_terms.replace(',', ' ').split()

		acc_nums = _read_accnums(args)

		if args.save_dir is None:
			args.save_dir = easygui.diropenbox(msg='Select a folder to save your images to.')
			if args.save_dir is None:
				return None
	except:
		easygui.exceptionbox()
		return None

	return [user, pw, acc_nums, args.save_dir, exclude_terms]

def get_inputs_cmd(args):
	"""UI flow. Returns None if cancelled or terminated with error,
	else returns [user, pw, acc_nums, save_dir]."""
	user = input("Enter YNHH username: ")
	pw = getpass.getpass()

	options
	if args.advanced:
		exclude_terms = [x.strip() for x in input('Terms to exclude if they appear in part of a protocol name (e.g. sub, localizer, cor).').split(',')]
		study_date = None

	download_package = get_dl_set(args)
	display_package(download_package)
	while True:
		input("Confirm to proceed with download (y/n): ")
		break

	return [user, pw, acc_nums, args.save_dir, exclude_terms]

def main(args):
	"""Starting point for script"""
	if args.gui:
		ret = get_inputs_gui(args)
	else:
		ret = get_inputs_cmd(args)

	if ret is None:
		return
	else:
		(user, pw, acc_nums, args.save_dir, exclude_terms) = ret
	study_info = collect_studies()
	retrieve_studies(study_info, args.save_dir, user, pw, exclude_terms=exclude_terms, overwrite=args.overwrite)

#####################################
### Subroutines
#####################################

def _read_accnums(args):
	if args.txt_path is not None:
		try:
			with open(args.txt_path, 'r') as f:
				acc_nums = [z for x in f.readlines() for z in x.replace(',', ' ').split()]
		except FileNotFoundError:
			print("ERROR: Invalid path", args.txt_path)
			return

	elif args.accnum is not None:
		acc_nums = args.query

	else:
		if args.gui:
			import easygui
			acc_nums = easygui.enterbox(msg='Enter accession numbers separated by commas (e.g. 12345678, E123456789, E234567890).')
			if acc_nums is None:
				return None
			else:
				acc_nums = acc_nums.replace(',', ' ').split()

		else:
			acc_nums = input('Enter accession numbers separated by commas (e.g. 12345678, E123456789, E234567890): ').replace(',', ' ').split()

	return acc_nums

def _search_vna(user, pw, study=None, series=None, region='prod', args=None, search_terms=None):
	"""Use AcuoREST API to search VNA for study, series, and/or instance numbers associated with an accession number"""

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


	if study is not None:
		url += "/" + study + "/series"

		if series is not None:
			url += "/" + series + "/instances"

	#search_terms["limit"]="2"
	#search_terms["includefield"]="all"
	if len(search_terms) > 0:
		query_str = '?' + '&'.join([term + '=' + search_terms[term] for term in search_terms])
		url += query_str

	r = requests.get(url, auth=(user, pw))
	#if r.status_code != 200:
		#raise ValueError("Invalid request (response code %d) for URL: %s" % (r.status_code, url))
		
	return r, url

def _retrieve_vna(user, pw, filepath, study=None, series=None, instance=None, region='prod', metadata=False):
	"""Retrieve dicom files and metadata associated with a study/series/instance.
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
						study])

		if series is not None:
			url += "/series/" + series
			if instance is not None:
				url += "/instances/" + instance

		url += "/metadata"+"?contentType=application/xml"

		r = requests.get(url, auth=(user, pw)) #, headers=headers

		if r.status_code != 200:
			print("Skipped series:", series)
			#raise ValueError("Invalid request (response code %d) for URL: %s" % (r.status_code, url))
			return None, url

		with open(filepath, 'wb') as fd:
			for chunk in r.iter_content(chunk_size=128):
				fd.write(chunk)

	else:
		url = ''.join(['http://', host, ':', port,
					   "/AcuoREST/wadoget?requestType=WADO&contentType=application/dicom&studyUID=",
						study])

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

if __name__ == "__main__":
	parser = argparse.ArgumentParser(description='Download imaging studies from the YNHH VNA API.')
	subparsers = parser.add_subparsers(help='sub-command help')
	subparser = subparsers.add_parser('nogui', action='store_true',
		help='use command line to specify query terms rather than GUI (fewer options available)')
	subparser.add_argument('-t', '--type', choices=['accnum', 'mrn', 'keyword'],
				help='whether to search by accession numbers, MRNs, or study keywords')
	action = parser.add_mutually_exclusive_group() # if not included, prompts for additional info in terminal
	action.add_argument('-g', '--gui', action='store_true', help='use GUI interface to specify query terms and other options')
	subparser.add_argument('-q', '--query', nargs='+', help='one or more space-separated query terms')
	subparser.add_argument('-p', '--txt_path', help='path to a txt file containing query terms separated by line breaks, spaces or commas')

	parser.add_argument('-s', '--save_dir', default=".", help='directory to save downloaded studies')
	parser.add_argument('-a', '--advanced', action='store_true',
				help='show advanced options, including excluding terms and limiting to a specific modality')
	#parser.add_argument('-o', '--overwrite', action='store_true', help='overwrite any existing folders in save_dir')
	#parser.add_argument('--modality', help='only include series of a specific modality (MR, CT, etc.); 1 max')

	args = parser.parse_args()

	main(args)