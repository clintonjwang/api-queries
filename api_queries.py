"""
Downloads imaging studies from YNHH VNA based on accession number.

Usage:
	python api_queries.py
	python api_queries.py --gui
	python api_queries.py --accnum E123456789
	python api_queries.py --txt_path accnums.txt --save_dir E:/dcms --overwrite
	python api_queries.py --search --mrn

Author: Clinton Wang, E-mail: `clinton.wang@yale.edu`, Github: `https://github.com/clintonjwang/api-queries`
"""

#StudyDate=20130509-20130510
#ModalitiesInStudy
#PatientID
#StudyDescription
#SeriesDescription

from dicom.examples import anonymize
import argparse
import getpass
import requests
import os
import shutil
import time

def search_vna(user, pw, acc_num=None, study=None, series=None, region='prod', search_terms=None, limit=None, modality=None):
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

	if acc_num is not None:
		url += "?AccessionNumber=" + acc_num

	elif study is not None:
		url += "/" + study + "/series"

		if series is not None:
			url += "/" + series + "/instances"
		elif modality is not None:
			url += "?Modality=" + modality

	if limit is not None:
		if "?" in url:
			url += "&"
		else:
			url += "?"
		url += "limit=" + str(limit)
	#url += "&includefield=all"

	r = requests.get(url, auth=(user, pw))
	#if r.status_code != 200:
		#raise ValueError("Invalid request (response code %d) for URL: %s" % (r.status_code, url))
		
	return r, url

def retrieve_vna(user, pw, filepath, study=None, series=None, instance=None, region='prod', metadata=False):
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

def download_accession_num(acc_nums, target_dir, user=None, pw=None, region="prod", exclude_terms=[], verbose=False, modality=None, overwrite=False, get_protocol_name=None):
	"""Download all studies associated with an accession number
	Each accession number (study) is saved to a separate folder named with its study UID.
	Within that folder, each series is saved to a separate subfolder, named with the series description.
	
	Keyword arguments:
	acc_nums -- a list of accession numbers as strings
	target_dir -- the directory to save the images to
	user -- username (optional)
	pw -- password (optional)
	region -- region to search in (default prod)
	modality -- modality to limit series to (optional)
	exclude_terms -- a list of terms to look for to exclude irrelevant protocols (optional)
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
	for acc_num in acc_nums:
		ret = get_accnum_instances(user, pw, region, acc_num)
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
					
		os.makedirs(acc_num_dir)

		total = 0
		skip_ser = 0
		skip_inst = 0
		rmdir = []

		t = time.time()
		for ser in instances:
			if verbose:
				print("\n==============")
				print("Loading metadata for series", ser)
			series_dir = acc_num_dir + "\\" + ser

			if not os.path.exists(series_dir):
				os.makedirs(series_dir)

			r, url = retrieve_vna(user, pw, region=region, filepath = series_dir+"\\metadata.xml",
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
						print("Skipping images for series with description", protocol_name)
					rmdir.append(protocol_name)
					break

			if skip:
				continue

			if verbose:
				print("Loading images for series with description:", protocol_name)

			for count, inst in enumerate(instances[ser]):
				r, _ = retrieve_vna(user, pw, region=region, filepath = series_dir+"\\"+str(count)+".dcm",
							  study=study, series=ser, instance=inst)

				if r is not None:
					skip_inst += 1
				if verbose:
					print(".", end="")

			total += count

		if len(rmdir)>0 and not os.path.exists(acc_num_dir+"\\others"):
			os.makedirs(acc_num_dir+"\\others")

			for d in rmdir:
				os.rename(acc_num_dir + "\\" + d, acc_num_dir + "\\others\\" + d)

		if verbose:
			print("Series loaded: ", len(series)-skip_ser, "/", len(series), sep="")
			print("Total images loaded:", total)
			print("Images skipped:", skip_inst)
			print("\nTime elapsed: %.2fs" % (time.time()-t))
			
	print("\nTime elapsed: %.1fs" % (time.time()-tot_time))

def get_accnum_instances(user, pw, region, acc_num):
	r, url = search_vna(user, pw, region=region, acc_num=acc_num)
	if r.status_code == 403:
		raise ValueError('Access denied. Probably incorrect login information.')
	elif r.status_code == 204:
		print('Accession number', acc_num, 'has no studies associated with it.')
		return None
	
	study = r.json()[0]['0020000D']['Value'][0]

	print('Loading accession number', acc_num)
	r, url = search_vna(user, pw, region=region, study=study, modality=modality)
	try:
		study_info = r.json()
	except:
		print('Accession number', acc_num, 'with study ID', study, 'encountered an unknown error.')
		return None

	series = list(set([ser['0020000E']['Value'][0] for ser in study_info]))

	instances = {}

	for ser in series:
		r, url = search_vna(user, pw, region=region, study=study, series=ser)
		series_info = r.json()
		instances[ser] = [inst['00080018']['Value'][0] for inst in series_info]

	return study, series, instances

def setup_ui(args, skip_col=False, skip_exc=True):
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
			exclude_terms = [x.strip() for x in exclude_terms]

		acc_nums = _read_accnums(args)

		if args.save_dir is None:
			args.save_dir = easygui.diropenbox(msg='Select a folder to save your images to.')
			if args.save_dir is None:
				return None
	except:
		easygui.exceptionbox()
		return None

	return [user, pw, acc_nums, args.save_dir, exclude_terms]

def _read_accnums(args):
	if args.txt_path is not None:
		try:
			with open(args.txt_path, 'r') as f:
				acc_nums = [z.strip() for x in f.readlines() for z in x.split(',')]
		except FileNotFoundError:
			print("ERROR: Invalid path", args.txt_path)
			return

	elif args.accnum is not None:
		acc_nums = [args.accnum]

	else:
		if args.gui:
			import easygui
			acc_nums = easygui.enterbox(msg='Enter accession numbers separated by commas (e.g. 12345678, E123456789, E234567890).')
			if acc_nums is None:
				return None
			else:
				acc_nums = [x.strip() for x in acc_nums.split(',')]

		else:
			acc_nums = [x.strip() for x in input('Enter accession numbers separated by commas (e.g. 12345678, E123456789, E234567890): ').split(',')]

def main(args):
	"""Starting point for script"""
	# Ask user for inputs
	if args.gui:
		ret = setup_ui(args)
		if ret is None:
			return
		else:
			(user, pw, acc_nums, args.save_dir, exclude_terms) = ret
			download_accession_num(acc_nums, args.save_dir, user, pw, exclude_terms=exclude_terms, overwrite=args.overwrite)
	else:
		acc_nums = _read_accnums(args)

		user = input("Enter YNHH username: ")
		pw = getpass.getpass()

		if args.advanced:
			exclude_terms = [x.strip() for x in input('Terms to exclude if they appear in part of a protocol name (e.g. sub, localizer, cor).').split(',')]
			study_date = None

		download_accession_num(acc_nums, args.save_dir, user, pw, exclude_terms=exclude_terms, overwrite=args.overwrite)

if __name__ == "__main__":
	parser = argparse.ArgumentParser(description='Download imaging studies from the YNHH VNA API.')
	parser.add_argument('search_by', choices=['accnum', 'mrn', 'keyword'],
				help='determines how terms in -p and -k args are interpreted, or what the GUI shows')
	action = parser.add_mutually_exclusive_group()
	action.add_argument('-g', '--gui', action='store_true', help='use GUI interface rather than command line')
	action.add_argument('-k', '--keyword', help='a single accession number / MRN / keyword to search and download')
	action.add_argument('-p', '--txt_path', help='path to a txt file containing terms to query, separated by line breaks or commas')

	parser.add_argument('-s', '--save_dir', default=".", help='directory to save downloaded studies')
	parser.add_argument('-o', '--overwrite', action='store_true', help='overwrite any existing folders in save_dir')
	parser.add_argument('-a', '--advanced', action='store_true', help='show advanced options, including excluding terms and limiting to a specific modality')
	#parser.add_argument('--modality', help='only include series of a specific modality (MR, CT, etc.); 1 max')

	args = parser.parse_args()

	main(args)