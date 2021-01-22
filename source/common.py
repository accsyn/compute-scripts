
'''
	accsyn Common compute app

	Inheritated by all other apps.

	Changelog:

		* v1r13; Process creation flags support.
		* v1r12; Compliance to accsyn v1.4.

		
	This software is provided "as is" - the author and distributor can not be held 
	responsible for any damage caused by executing this script in any means.

	Author: Henrik Norin, HDR AB

'''

import os
import sys
import json
import logging
import unicodedata
import subprocess
import traceback

logging.basicConfig(
	format="(%(asctime)-15s) %(message)s", 
	level=logging.INFO, 
	datefmt='%Y-%m-%d %H:%M:%S')

class Common(object):

	__revision__ = 13 # Will be automatically increased each publish, leave this comment!

	OS_LINUX 		= "linux"
	OS_MAC 			= "mac"
	OS_WINDOWS 		= "windows"
	OS_RSB 			= "rsb"

	_dev = False
	_debug = False

	# App configuration
	# IMPORTANT NOTE:
	# This section defines app behaviour and should not be refactored or moved away from the enclosing START/END markers.
	# -- APP CONFIG START --

	# -- APP CONFIG END --

	def __init__(self, argv):
		# Expect path to data in argv
		if "--dev" in argv:
			Common._dev = True
			Common._debug = True
			Common.debug("Dev mode on%s"%(
				" dev machine" if Common.is_devmachine() else ""))
		assert (len(sys.argv)==2 or (len(sys.argv)==3 and Common.is_dev())), \
			("Please provide path to compute data (json) as only argument!")
		self.path_data = argv[1] if not Common.is_dev() else argv[2]
		# (Parallellizable apps) The part to execute
		self.item = os.environ.get("ACCSYN_ITEM")
		# Find out and report my PID, write to sidecar file
		self.path_pid = os.path.join(os.path.dirname(self.path_data), "process.pid")
		with open(self.path_pid, "w") as f:
			f.write(str(os.getpid()))
		if Common.is_debug():
			Common.debug("Accsyn PID(%s) were successfully written to '%s'.."%(os.getpid(), self.path_pid))
		else:
			Common.info("Accsyn PID(%s)"%(os.getpid()))
		self.check_mounts()

	@staticmethod
	def get_path_version_name():
		p = os.path.realpath(__file__)
		parent = os.path.dirname(p)
		return (os.path.dirname(parent), os.path.basename(parent), os.path.splitext(os.path.basename(p))[0])

	# Helpers

	def get_compute(self):
		return self.data.get("compute",{})

	def get_site_code(self):
		''' Return the name site we are running at '''
		if not self.data.get('site') is None:
			return self.data['site'].get('code')
		return None

	@staticmethod
	def get_os():
		if sys.platform == "darwin":
			if os.name == "mac" or os.name == "posix":
				return Common.OS_MAC
			else:
				return Common.OS_LINUX
		elif sys.platform == "linux2":
			if os.name == "posix":
				return Common.OS_LINUX
			else:
				return Common.OS_RSB
		elif os.name == "nt":
			return Common.OS_WINDOWS

	@staticmethod
	def is_lin():
		return Common.get_os() == Common.OS_LINUX

	@staticmethod
	def is_lin():
		return Common.get_os() == Common.OS_LINUX

	@staticmethod
	def is_mac():
		return Common.get_os() == Common.OS_MAC

	@staticmethod
	def is_win():
		return Common.get_os() == Common.OS_WINDOWS

	@staticmethod
	def safely_printable(s):
		return unicodedata.normalize("NFKD", unicode(s) if not isinstance(s, unicode) else s).encode("ascii", errors="ignore")

	@staticmethod
	def info(s):
		logging.info("[ACCSYN] {}".format(s))

	@staticmethod
	def warning(s):
		logging.warning("[ACCSYN] {}".format(s))

	# PATH CONVERSION

	def normalize_path(self, p, mkdirs=False):
		''' Based on share mappings supplied, convert a foreign path to local platform '''
		if p is None or 0==len(p):
			return p
		try:
			p_orig = str(p)
			# Turn paths
			#if Common.is_win():
			p = p.replace("\\","/")
			# On a known share that can be converted?
			prefix_from = prefix_to = None
			for share in self.data.get('shares') or []:
				for path_ident, prefix in share.get('paths',{}).items():
					Common.debug('path_ident.lower(): "{}", '
						'Common.get_os().lower(): "{}"'
						'(prefix_from: {}, prefix_to: {})'
						.format(
							path_ident.lower(), 
							Common.get_os().lower(), 
							prefix_from, 
							prefix_to))
					if path_ident.lower() == Common.get_os().lower():
						# My platform
						prefix_to = prefix
					else:
						if 0<len(prefix) and (p.startswith('share={}'.format(share['code'])) or p.startswith('share={}'.format(share['id'])) or p.lower().find(prefix.lower()) == 0):
							prefix_from = prefix
				if prefix_from:
					break

			if prefix_from and prefix_to:
				if p.startswith('share='):
					p = prefix_to + p[p.find('/'):]
				else:
					p = prefix_to + (("/" if prefix_to[-1]!="/" and p[len(prefix_from)]!="/" else "") + p[len(prefix_from):] if len(prefix_from)<len(p) else "")
			# Turn back paths
			if Common.is_win():
				p = p.replace("/","\\")
			if p != p_orig:
				Common.debug("Converted '%s'>'%s'"%(p_orig, p))
			else:
				Common.debug("No conversion of path '%s' needed (prefix_from: %s, prefix_to: %s)"%(p_orig, prefix_from, prefix_to))
			if mkdirs and not os.path.exists(p):
				os.makedirs(p)
				Common.warning("Created missing folder: '%s'"%p)
		except:
			Common.warning("Cannot normalize path, data '%s' has wrong format? Details: %s"%(json.dumps(self.data, indent=2), traceback.format_exc()))
		return p

	# DEBUGGING ###############################################################

	@staticmethod
	def is_dev():
		return Common._dev or (os.environ.get("ACCSYN_DEV") or os.environ.get("FILMHUB_DEV") or "") in ["1","true"]

	@staticmethod
	def is_debug():
		return Common._debug or (os.environ.get("ACCSYN_DEBUG") or os.environ.get("FILMHUB_DEBUG") or "") in ["1","true"]

	@staticmethod
	def is_devmachine():
		import socket
		return Common.is_dev() and -1<socket.gethostname().lower().find("ganymedes") 

	@staticmethod
	def debug(s):
		if Common.is_debug():
			logging.info("<<ACCSYN APP DEBUG>> %s"%s)

	@staticmethod
	def set_debug(debug):
		Common._debug = debug

	# Functions that should be overridden by child class ######################

	def get_envs(self): 
		''' (OPTIONAL) Return dict holding additional environment variables. '''
		return {}

	def probe(self):
		''' (OPTIONAL) Return False if not implemented, return True if found, raise execption otherwise. '''
		return False

	def check_mounts(self):
		''' (OPTIONAL) Make sure all network drives are available prior to compute. '''
		pass

	def get_executable(self):
		''' (REQUIRED) Return path to executable as string ''' 
		raise Exception("Get executable not overridden by app!")

	def get_commandline(self, item):
		''' (REQUIRED) Return command line as a string array ''' 
		raise Exception("Get commandline not overridden by app!")

	def get_stdin(self, item):
		''' (OPTIONAL) Return stdin as text to be sent to app. ''' 
		return None

	def get_creation_flags(self, item):
		''' (OPTIONAL) Return the process creation flags. ''' 
		return None

	###########################################################################

	def load(self):
		''' Load the data from disk, must be run BEFORE execution '''
		assert (os.path.exists(self.path_data) or os.path.isdir(self.path_data)),("Data not found or is directory @ '%s'!"%self.path_data)
		try:
			self.data = json.load(open(self.path_data,"r"))
		except:
			Common.warning("Loading the execution data caused exception %s: %s"%(traceback.format_exc(), open(self.path_data,"r").read()))
			raise
		Common.debug("Data loaded:\n%s"%json.dumps(self.data, indent=3))

	def get_common_envs(self):
		return self.get_envs()

	def execute(self):
		''' Compute '''
		commands = self.get_commandline(self.item)
		#def execute(commands, shell=False, log=False, log_standout=True, timeout=None, fetch_output=True, fetch_stderr=True, continue_on_error=False, exit_signaler=None, force_print=False, stdin=None, env=None, output_file=None, dry_run=False, su_user=None, squeeze_non_us_chars=True):
		if commands is None or len(commands)==0:
			raise Exception("Empty command line!")
		app_envs = self.get_common_envs()
		if len(app_envs)==0:
			app_envs = None
		log = True
		exitcode = 0
		
		try:
			new_envs = None
			if app_envs:
				new_envs = {}
				for k,v in os.environ.iteritems():
					new_envs[str(Common.safely_printable(k))] = str(Common.safely_printable(v))
				for k,v in app_envs.iteritems():
					new_envs[str(Common.safely_printable(k))] = str(Common.safely_printable(v))
			for idx in range(0, len(commands)):
				if not isinstance(commands[idx], str):
					if isinstance(commands[idx], unicode):
						commands[idx] = commands[idx].encode(u'utf-8')
			stdin = self.get_stdin(self.item)
			if new_envs:
				Common.info("Environment variables: '%s"%new_envs)
			creationflags = self.get_creation_flags(self.item)
			Common.info("Running '%s'"%(str([Common.safely_printable(s) for s in commands])))
			if stdin:
				Common.info("Stdin: '%s"%stdin)
			if creationflags:
				Common.info("Creation flags: '%s"%creationflags)
			Common.info("-"*120)

			#if fetch_output:
			#	process = subprocess.Popen(commands, shell, stdin=subprocess.PIPE, stderr=subprocess.PIPE, stdout=subprocess.PIPE, env=new_envs)
			if stdin:
				first_run = True
				process = subprocess.Popen(commands, shell, stdin=subprocess.PIPE, env=new_envs, creationflags=creationflags)
				while True:
					# Empty data waiting for us in pipes
					process.communicate(input=stdin if not stdin is None and first_run else None)
					first_run = False
					exitcode=process.poll()
			else:
				process = subprocess.Popen(commands, True, env=new_envs)
				exitcode = process.wait()

		finally:
			try:
				process.terminate()
			except:
				pass
		
		process=None

		# More commands?
		assert (exitcode == 0),("Execution failed, check log for clues...")


