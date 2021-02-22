''' 

	Nuke v12 accsyn compute app script.

	Finds and executes Nuke by building a commandline out of 'item'(frame number)
	and parameters provided.

	Changelog:

		* v1r2; Parse Nuke output and finish render if hangs 5s on "Total render time:"
		* v1r1; Compliance to accsyn v1.4.

	This software is provided "as is" - the author and distributor can not be held 
	responsible for any damage caused by executing this script in any means.

	Author: Henrik Norin, HDR AB

'''

import os
import sys
import logging
import traceback
import socket
import datetime
import threading
import time
try:
	from common import Common 
except ImportError,e:
	print >> sys.stderr, "Cannot import accsyn common app (required), make sure to name it 'common.py' add its parent directory to PYTHONPATH. Details: %s"%e
	raise

class App(Common):

	__revision__ = 2 # Will be automatically increased each publish

	# App configuration
	# IMPORTANT NOTE:
	# This section defines app behaviour and should not be refactored or moved away from the enclosing START/END markers. Read into memory by cloud at start and publish. 
	# -- APP CONFIG START --

	# Can be retreived during execution from: self.get_compute()['settings']
	SETTINGS = {
		"items" : True, 
		"default_range" : "1001-1100", 
		"default_bucketsize" : 5,
		"filename_extensions" : ".nk"
	}

	# Can be retreived during execution from: self.get_compute()['parameters']
	PARAMETERS = {
		"arguments" : "-txV"
	}

	# -- APP CONFIG END --

	NUKE_VERSION = "12"
	
	def __init__(self, argv):
		super(App, self).__init__(argv)
		self.date_finish_expire = None

	@staticmethod
	def get_path_version_name():
		p = os.path.realpath(__file__)
		parent = os.path.dirname(p)
		return (os.path.dirname(parent), os.path.basename(parent), os.path.splitext(os.path.basename(p))[0])

	@staticmethod
	def version():
		(unused_cp, cv, cn) = Common.get_path_version_name()
		(unused_p, v, n) = App.get_path_version_name()
		Common.info("   Accsyn compute app '%s' v%s-%s(common: v%s-%s) "%(n, v, App.__revision__, cv, Common.__revision__))

	@staticmethod
	def usage():
		(unused_p, unused_v, name) = App.get_path_version_name()
		Common.info("")
		Common.info("   Usage: python %s {--probe|<path_json_data>}"%name)
		Common.info("")
		Common.info("       --probe           Have app check if it is found and of correct version.")
		Common.info("")
		Common.info("       <path_json_data>  Execute app on data provided in the JSON and FILMHUB_xxx environment variables.")
		Common.info("")

	def probe(self):
		''' (Optional) Do nothing if found, raise execption otherwise. '''
		exe = self.get_executable()
		assert (os.path.exists(exe)),("'%s' does not exist!"%exe)
		# Check if correct version here
		return True

	def get_envs(self): 
		''' (Optional) Get dynamic environment variables '''
		result = {}
		return result

	def get_executable(self, preferred_nuke_version=None):
		''' (REQUIRED) Return path to executable as string ''' 

		def find_executable(p_base, prefix):
			if os.path.exists(p_base):
				candidates = []
				for fn in os.listdir(p_base):
					if fn.startswith(prefix):
						candidates.append(fn)
				if 0<len(candidates):
					if preferred_nuke_version and preferred_nuke_version in candidates:
						dirname = preferred_nuke_version
					else:
						dirname = sorted(candidates)[-1]
					return os.path.join(p_base, dirname)
				else:
					raise Exception("No {} application version found on system!".format(prefix))
			else:
				raise Exception("Application base directory '{}' not found on system!".format(p_base))

		# Use highest version
		p_base = p_app = None
		if Common.is_lin():
			p_base = "/usr/local"
			p_app = find_executable(p_base, "Nuke{}".format(App.NUKE_VERSION))
		elif Common.is_mac():
			p_base = "/Applications"
			p_app = find_executable(p_base, "Nuke{}".format(App.NUKE_VERSION))
		elif Common.is_win():
			p_base = "C:\\Program Files"
			p_app = find_executable(p_base, "Nuke{}".format(App.NUKE_VERSION))
		if p_app:
			if Common.is_mac():
				return os.path.join(p_app, '{}.app'.format(os.path.basename(p_app)), "Contents","MacOS", os.path.basename(p_app))
			else:
				return os.path.join(p_app, "nuke{}{}".format(App.NUKE_VERSION, ".exe" if Common.is_win() else ""))
		else:
			raise Exception("Nuke not supported on this platform!")

	def get_commandline(self, item):
		''' (REQUIRED) Return command line as a string array '''

		args=[]
		if 'parameters' in self.get_compute():
			parameters = self.get_compute()['parameters']

			if 0<len(parameters.get('arguments') or ""):
				arguments = parameters['arguments']
				if 0<len(arguments):
					args.extend(arguments.split(" "))

			# Grab a workstation license?
			hostname = socket.gethostname().lower()
			if hostname.find("a")==0 or hostname.find("b")==0 or hostname.find("c")==0:
				args.append("-i")
		if not self.item is None and self.item != "all":
			# Add range
			start = end = self.item
			if -1<self.item.find("-"):
				parts = self.item.split("-")
				start = parts[0]
				end = parts[1]
			args.extend(["-F", "%s-%s"%(start, end)])

		input_path = self.normalize_path(self.data['compute']['input'])
		args.extend([input_path])
		# Find out preffered nuke version from script, expect:
		#   #! C:/Program Files/Nuke10.0v6/nuke-10.0.6.dll -nx
		#   version 10.0 v6
		#   define_window_layout_xml {<?xml version="1.0" encoding="UTF-8"?>
		preferred_nuke_version = None
		with open(input_path, "r") as f_input:
			for line in f_input:
				if line.startswith('version '):
					#  version 10.0 v6
					preferred_nuke_version = line[8:].replace(" ","")
					Common.info("Parsed Nuke version: '%s'"%preferred_nuke_version)

		if Common.is_lin():
			retval = ["/bin/bash", "-c", self.get_executable(preferred_nuke_version=preferred_nuke_version)]
			retval.extend(args)
			return retval
		elif Common.is_mac():
			retval = [self.get_executable(preferred_nuke_version=preferred_nuke_version)]
			retval.extend(args)
			return retval
		elif Common.is_win():
			retval = [self.get_executable(preferred_nuke_version=preferred_nuke_version)]
			retval.extend(args)
			return retval

		raise Exception("This OS is not recognized by this Accsyn app!")

	def process_output(self, stdout, stderr):
		''' Sift through stdout/stderr and take action, return exitcode instead of None if should abort '''
		sys.stdout.flush()
		def check_stuck_render():
			while self.executing:
				time.sleep(1.0)
				if self.executing and not self.date_finish_expire is None and self.date_finish_expire<datetime.datetime.now():
					Common.warning('Nuke finished but still running (hung?), finishing up.')
					self.exitcode_force = 0
					self.kill()
					break # We are done

		''' Nuke might stuck on finished render, handle this. '''
		if -1<(stdout+stderr).find('Total render time:'):
			Common.info('Finished Nuke render will expire in 5s...')
			self.date_finish_expire = datetime.datetime.now() + datetime.timedelta(seconds=5)
			thread = threading.Thread(target=check_stuck_render)
			thread.start()

if __name__ == '__main__':
	App.version()
	if "--help" in sys.argv:
		App.usage()
	else:
		#Common.set_debug(True)
		try:
			app = App(sys.argv)
			if "--probe" in sys.argv:
				app.probe()
			else:
				app.load() # Load data
				app.execute() # Run
		except:
			App.warning(traceback.format_exc())
			App.usage()
			sys.exit(1)