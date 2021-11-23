'''
    accsyn Common compute app

    Inheritated by all other apps.

    Changelog:

        * v1r16; Fixed Windows path conversion bug.
        * v1r15; Python 3 compliance. OS dependent path conversions.
        * v1r13; Process creation flags support.
        * v1r12; Compliance to accsyn v1.4.

        
    This software is provided "as is" - the author and distributor can not be 
    held  responsible for any damage caused by executing this script in any 
    means.

    Author: Henrik Norin, HDR AB

'''
from __future__ import print_function
import os
import sys
import json
import logging
import unicodedata
import subprocess
import traceback
import socket
import time
import random
import datetime

logging.basicConfig(
    format="(%(asctime)-15s) %(message)s", 
    level=logging.INFO, 
    datefmt='%Y-%m-%d %H:%M:%S')

class Common(object):

    __revision__ = 18 # Will be automatically increased each publish.

    OS_LINUX        = "linux"
    OS_MAC          = "mac"
    OS_WINDOWS      = "windows"
    OS_RSB          = "rsb"

    _dev = False
    _debug = False

    # App configuration
    # IMPORTANT NOTE:
    # This section defines app behaviour and should not be refactored or moved 
    # away from the enclosing START/END markers.
    # -- APP CONFIG START --

    # SETTINGS
    # Can be retreived during execution from: self.get_compute()['settings']
    #  - items; If true, each inpit file can be split and executed in ranges 
    # (render)
    #  - default_range; (items) The default item range.
    #  - default_bucketsize; The default amount of items to dispatch to each 
    # compute node/machone.
    #  - filename_extensions: Comma(,) separated list of filename extensions 
    # associated with app.
    #  - binary_filename_extensions: Comma(,) separated list of filename 
    # extensions that indicated a binary non parseable format.

    # PARAMETERS
    # Can be retreived during execution from: self.get_compute()['parameters']
    #  - arguments; The additional command line arguments to pass on to app.
    #  - remote_os; The operating system ("windows", "linux" or "mac") on 
    # machine that submitted the job, used for parsing below.
    #  - input_conversion; Define what kind of input file path conversion should
    #  happen on compute cluster (non binary formats only): 
    #    * 'always' - expect that the input files always needs paths converted 
    # - contains local share mapped paths.
    #    * 'platform' - all paths are assumed being relative root share, not 
    # conversion needed except when switching platform (and platform path 
    # prefixes differs).
    #    * 'never' - all paths are assumed being relative root share and works 
    # on all platforms, or are relative and do need conversion.
    #  - mapped_share_paths(input_conversion=always); List of dicts on the form
    #  {'remote':'D:\\PROJECTS\\MYSHARE', 
    # 'global':"share=root_share_id/share_path"}, used during input file parsing.

    # -- APP CONFIG END --

    def __init__(self, argv):
        # Expect path to data in argv
        self.executing = False
        self.exitcode_force = None
        self.process = None
        self.data = {}

        if "--dev" in argv:
            Common._dev = True
            Common._debug = True
            self.debug("Dev mode on{}".format(
                " dev machine" if self.is_devmachine() else ""))
        assert (len(sys.argv)==2 or (len(sys.argv)==3 and self.is_dev())), \
            ("Please provide path to compute data (json) as only argument!")
        self.path_data = argv[1] if not self.is_dev() else argv[2]
        # (Parallellizable apps) The part to execute
        self.item = os.environ.get("ACCSYN_ITEM")
        # Find out and report my PID, write to sidecar file
        self.path_pid = os.path.join(os.path.dirname(self.path_data), 
            "process.pid")
        with open(self.path_pid, "w") as f:
            f.write(str(os.getpid()))
        if self.is_debug():
            self.debug("accsyn PID({}) were successfully written to '{}'..".format(
                os.getpid(), self.path_pid))
        else:
            Common.info("Accsyn PID({})".format(os.getpid()))
        self.check_mounts()

    @staticmethod
    def get_path_version_name():
        p = os.path.realpath(__file__)
        parent = os.path.dirname(p)
        return (os.path.dirname(parent), os.path.basename(parent), 
            os.path.splitext(os.path.basename(p))[0])

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

    def get_remote_os(self):
        remote_os = self.get_compute()['parameters'].get('remote_os')
        if remote_os is None:
            remote_os = self.get_os()
            self.debug('Do not know remote OS, assuming same.')
        return remote_os.lower()

    @staticmethod
    def safely_printable(s):
        return unicodedata.normalize("NFKD", unicode(s) if not isinstance(s, 
            unicode) else s).encode("ascii", errors="ignore")

    @staticmethod
    def info(s):
        logging.info("[ACCSYN] {}".format(s))
        sys.stdout.flush()

    @staticmethod
    def warning(s):
        logging.warning("[ACCSYN] {}".format(s))
        sys.stdout.flush()
        sys.stderr.flush()

    # PATH CONVERSION

    def normalize_path(self, p, mkdirs=False):
        ''' Based on share mappings supplied, convert a foreign path to local 
        platform '''
        self.debug('normalize_path({},{})'.format(p, mkdirs))
        if p is None or 0==len(p):
            return p
        try:
            p_orig = str(p)
            # Turn paths
            #if Common.is_win():
            p = p.replace("\\","/")
            if p.lower().startswith('share=') and 'share_paths' in \
            self.get_compute()['parameters']:
                # Could be a path relative share, must be made relative root 
                # share first
                idx_slash = p.find('/')
                p_rel = None
                if -1<idx_slash:
                    share_code = p[:idx_slash].split('=')[-1]
                    p_rel = p[idx_slash+1:]
                else:
                    share_code = p.split('=')[-1]
                if share_code in self.get_compute()['parameters']['share_paths']:
                    d = self.get_compute()['parameters']['share_paths'][share_code]
                    share_path = d['s_path']
                    p_orig = str(p)
                    p = 'share={}{}{}'.format(
                        d['r_s'], 
                        ('/'+share_path) if 0<len(share_path) and not share_path\
                         in ['/','\\'] else '',
                        ('/'+p_rel) if p_rel else ''
                    )
                    self.debug('(Share path normalize) Converted share '
                        'relative path "{}" > root share relative: "{}" (share'
                        ' paths: {})'.format(
                            p_orig, 
                            p,
                            self.get_compute()['parameters']['share_paths']))

            # On a known share that can be converted?
            prefix_from = prefix_to = None
            for share in self.data.get('shares') or []:
                for path_ident, prefix in share.get('paths',{}).items():
                    self.debug('(Root share {} path normalize) path_ident.lower()'
                        ': "{}", Common.get_os().lower(): "{}"'
                        '(prefix_from: {}, prefix_to: {})'
                        .format(
                            share['code'],
                            path_ident.lower(), 
                            Common.get_os().lower(), 
                            prefix_from, 
                            prefix_to))
                    if path_ident.lower() == Common.get_os().lower():
                        # My platform
                        prefix_to = prefix
                    else:
                        if 0<len(prefix) and p.lower().find(prefix.lower()) == 0:
                            prefix_from = prefix
                if prefix_to:
                    if not prefix_from:
                        # Starts with accsyn share notiation?
                        s = 'share={}'.format(share['code'])
                        if p.startswith(s):
                            prefix_from = s
                        else:
                            s = 'share={}'.format(share['id'])
                            if p.startswith(s):
                                prefix_from = s
                    if prefix_from:
                        break
            if prefix_from is None:
                # Any supplied path conversions?
                if 'mapped_share_paths' in self.get_compute()['parameters']:
                    for d in self.get_compute()['parameters']['mapped_share_paths']:
                        self.debug('(Supplied mapped shares path normalize)'
                            ' d: "{}"'.format(d))
                        if p.lower().startswith(d['remote'].replace("\\","/").lower()):
                            prefix_from = d['remote']
                            prefix_to = d['local']
                            if prefix_to.lower().startswith('share='):
                                share_id_or_code = prefix_to.split('=')[-1]
                                for share in self.data.get('shares') or []:
                                    if share['id'].lower() == share_id_or_code.lower() or share['code'].lower() == share_id_or_code.lower():
                                        if Common.get_os().lower() in share.get('paths',{}):
                                            prefix_to = share['paths'][Common.get_os().lower()]
                                        break
                                if prefix_to.lower().startswith('share='):
                                    raise Exception('Cannot find root share {}'
                                        ' for remote mapped path conversion {} for my os({})!'
                                        .format(share_id_or_code, d, Common.get_os()))
                            break
            if prefix_from and prefix_to:
                if p.startswith('share='):
                    idx_slash = p.find('/')
                    p = prefix_to + (p[idx_slash:] if -1<idx_slash else '')
                else:
                    p = prefix_to + (("/" if prefix_to[-1] != "/" and\
                        p[len(prefix_from)]!="/" else "") + p[len(prefix_from):]\
                         if len(prefix_from)<len(p) else "")

            # Turn back paths
            if Common.is_win():
                p = p.replace("/","\\")
            if p != p_orig:
                self.debug("Converted '%s'>'%s'"%(p_orig, p))
            elif prefix_from and prefix_to:
                self.debug('No conversion of path "{}" needed (prefix_from: '
                    '{}, prefix_to: {})'.format(p_orig, prefix_from, prefix_to))

        except:
            Common.warning('Cannot normalize path, data "{}" has wrong format?'
                'Details: {}'.format(
                    json.dumps(self.data, indent=2), 
                    traceback.format_exc()))

        if p.startswith('share='):
            # Will never work
            raise Exception('Cannot convert accsyn path {} to local!'.format(p))

        if mkdirs and not os.path.exists(p):
            os.makedirs(p)
            Common.warning('Created missing folder: "{}"'.format(p))

        return p

    # DEBUGGING ###############################################################

    def is_dev(self):
        return Common._dev or (os.environ.get("ACCSYN_DEV") or "") in ["1","true"] or\
            (self.get_compute() or {}).get('parameters',{}).get('dev') is True

    def is_devmachine(self):
        import socket
        return self.is_dev() and -1<socket.gethostname().lower().find("ganymedes") 

    def is_debug(self):
        return Common._debug or (os.environ.get("ACCSYN_DEBUG") or "") in ["1","true"] or\
        (self.get_compute() or {}).get('parameters',{}).get('debug') is True

    def debug(self, s):
        if self.is_debug():
            logging.info("<<ACCSYN APP DEBUG>> {}".format(s))

    @staticmethod
    def set_debug(debug):
        Common._debug = debug

    # Functions that should be overridden by child class ######################

    def get_envs(self): 
        ''' (OPTIONAL) Return dict holding additional environment variables. '''
        return {}

    def probe(self):
        ''' (OPTIONAL) Return False if not implemented, return True if found, 
        raise execption otherwise. '''
        return False

    def check_mounts(self):
        ''' (OPTIONAL) Make sure all network drives are available prior to 
        compute. '''
        pass

    def convert_input(self, f_src, f_dst, conversions):
        ''' Basic ASCII file path conversion, should be overridden by app to 
        support more. Raise an exception is localization fails. '''
        line_no = 1
        for line in f_src:
            try:
                for (path_from, path_to) in conversions:
                    while True:
                        idx = line.lower().find(path_from.lower())
                        if idx == -1:
                            if -1<path_from.find('\\') and -1<line.find('/'):
                                # Windows to *NIX
                                idx = line.lower().find(path_from.replace('\\',\
                                    '/').lower())
                            elif -1<path_from.find('/') and -1<line.find('\\'):
                                # *NIX to Windows
                                idx = line.lower().find(path_from.replace('/',\
                                    '\\').lower())
                        if idx == -1:
                            break
                        line_orig = str(line)
                        line = (line[0:idx] if 0<idx else "")+\
                            self.convert_path(path_to)+\
                            (line[idx+len(path_from):] if idx+len(path_from)<len(line) else "")
                        self.info('(input convert) "{}">"{}"'.format(
                            line_orig, line))
            except:
                logging.warning(traceback.format_exc())
                logging.warning('Could not convert line #{}, leaving as is...'.format(line_no))
            f_dst.write('{}'.format(line))
            line_no += 1

    def convert_path(self, p):
        ''' Can be overridden by app to provide further path alignment.'''
        return p

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

    def process_output(self, stdout, stderr):
        ''' 
        (OPTIONAL) Sift through stdout/stderr and take action. 
        
        Return value:
           None (default); Do nothing, keep execution going.
           integer; Terminate process and force exit code to this value. 
        '''
        return None

    ###########################################################################

    def load(self):
        ''' Load the data from disk, must be run BEFORE execution '''
        assert (os.path.exists(self.path_data) or os.path.isdir(self.path_data)),\
        ("Data not found or is directory @ '{}'!".format(self.path_data))
        try:
            self.data = json.load(open(self.path_data,"r"))
        except:
            Common.warning("Loading the execution data caused exception {}: {}".format(
                traceback.format_exc(), 
                open(self.path_data,"r").read()))
            raise
        self.debug("Data loaded:\n{}".format(json.dumps(self.data, indent=3)))

    def prepare(self):
        ''' Prepare execution - localize files. '''
        # Any output file?
        if "output" in self.data['compute']:
            p_output = self.normalize_path(self.get_compute()["output"], 
                mkdirs=True)
            self.get_compute()["output"] = p_output
        # Any input file?
        p_input = None
        if 'input' in self.get_compute():
            p_input = self.normalize_path(self.get_compute()['input'])
            # Store it
            self.get_compute()['input'] = p_input
            if not os.path.exists(p_input):
                Common.warning("(Localization) Input file/scene does not exists @ '{}'!".format(p_input))
            elif 'input_conversion' in self.get_compute()['parameters']:
                # Is input conversion/localizing needed?
                input_conversion = self.get_compute()['parameters']['input_conversion']
                Common.info('(Localization) Input conversion mode: {}'.format(input_conversion))
                if input_conversion is None:
                    input_conversion = 'platform'
                if input_conversion == 'auto':
                    # Set to always if any paths supplied
                    if 'mapped_share_paths' in self.get_compute()['parameters'] and\
                     0<len(self.get_compute()['parameters']['mapped_share_paths']):
                        Common.info('(Localization) Mapped share paths supplied, appling "always"'
                            ' input conversion mode.')
                        input_conversion = 'always'
                    else:
                        Common.info('(Localization) No mapped share paths supplied, appling'
                            ' "platform" input conversion mode.')
                        input_conversion = 'platform'
                if input_conversion == 'never':
                    Common.info('(Localization) Not attempting to localize input file, input'
                        ' conversion disabled.')
                else:
                    # Is an ASCII parsable format?
                    is_binary_format = False
                    binary_filename_extensions = self.get_compute().get('settings',{}).get(
                        'binary_filename_extensions')
                    if 0<len(binary_filename_extensions or ""):
                        if -1<p_input.find('.'):
                            ext = os.path.splitext(p_input)[1]
                            if binary_filename_extensions == "*" or -1<binary_filename_extensions.\
                            lower().find(ext.lower()):
                                # No, this is a binary format - cannot localize
                                Common.info('Input file "{}" is a binary format - not attempting to'
                                    ' localize...'.format(p_input))
                                is_binary_format = True
                        else:
                            Common.info('(Localization) input file does not have any extension,'
                                ' cannot determine if not binary format and in need of'
                                'localization...')
                            is_binary_format = True
                    if not is_binary_format:
                        p_input_localized = p_input
                        p_input_prefix, p_localized_ext = os.path.splitext(p_input)
                        if input_conversion == 'always':
                            p_input_localized = '{}_accsynlocalized_hq_{}{}'.format(
                                p_input_prefix, 
                                self.get_os(), 
                                p_localized_ext)
                        elif input_conversion == 'platform':
                            remote_os = self.get_remote_os()
                            if remote_os != self.get_os():
                                # Does the root share path differ between platforms?
                                # TODO: Support site root share path overrides
                                remote_prefix = local_prefix = None
                                for share in self.data.get('shares') or []:
                                    if remote_os in share.get('paths',{}):
                                        remote_prefix = share['paths'][remote_os]
                                    if self.get_os() in share.get('paths',{}):
                                        local_prefix = share['paths'][self.get_os()]
                                    if remote_prefix and local_prefix:
                                        break
                                if remote_prefix and local_prefix:
                                    if remote_prefix != local_prefix:
                                        p_input_localized = '{}_accsynlocalzed_hq_{}{}'.format(
                                            p_input_prefix, self.get_os(), p_localized_ext)
                                        Common.info('(Localization) Remote root share path prefix'
                                            ' ({}) and local ({}) differs, need to localize!'.format(
                                                remote_prefix, local_prefix))
                                    else:
                                        Common.info('(Localization) Remote root share path prefix'
                                            ' ({}) and local ({}) are the same, no need to localize'
                                            '...'.format(remote_prefix, local_prefix))
                                if remote_prefix is None:
                                    Common.warning('(Localization) Do not know remote root share'
                                        ' prefix on {}, cannot localize "{}"...'.format(
                                            remote_os, p_input))
                                if local_prefix is None:
                                    Common.warning('(Localization) Do not know local root share'
                                        ' prefix on {}, cannot localize "{}"...'.format(
                                            self.get_os(), p_input))
                            else:
                                Common.info('(Localization) On same platform({}), no need to'
                                    ' convert path.'.format(remote_os))
                    # 
                    if p_input_localized != p_input:
                        # Does it exist?
                        p_parent = os.path.dirname(p_input)
                        p_localized_metadata = os.path.join(p_parent,"{}.localized_metadata_{}".format(
                            os.path.basename(p_input), Common.get_os().lower()))
                        do_localize = True
                        localized_size = localized_mtime = None
                        if os.path.exists(p_input_localized):
                            if os.path.exists(p_localized_metadata):
                                # Find out the size and mtime input file had when last localized
                                d = json.load(open(p_localized_metadata, "r"))
                                localized_size = d['size']
                                localized_mtime = d['time']
                                if os.path.getsize(p_input)!=localized_size:
                                    Common.warning('Localized file was based on input file that'
                                        ' differs in size current ({}<>{})!'.format(
                                            localized_size, os.path.getsize(p_input)))
                                elif os.path.getmtime(p_input)!=localized_mtime:
                                    Common.warning('Localized file was based on input file that'
                                        ' differs in modification time ({}<>{})!'.format(
                                            localized_mtime, os.path.getmtime(p_input)))
                                else:
                                    # Localized is up to date
                                    do_localize = False
                            else:
                                Common.warning("Localized file metadata does not exist @ '{}'!"\
                                    .format(p_localized_metadata))
                        else:
                            Common.warning("Localized file does not exist @ '{}'!".format(
                                p_input_localized))
                        if do_localize:
                            other_is_localizing = False
                            hostname = socket.gethostname()
                            #if not os.path.exists(p_parent_localized):
                            #   Common.info("Creating %s..."%p_parent_localized)
                            #   os.makedirs(p_parent_localized)
                            p_localize_lock = os.path.join(p_parent,"{}.localize_lock_{}".format(
                                os.path.basename(p_input), Common.get_os().lower()))
                            Common.info("(Localization) Checking localize lock @ '{}'...".format(
                                p_localize_lock))
                            if os.path.exists(p_localize_lock):
                                # Is it me?
                                other_hostname = "?"
                                try:
                                    other_hostname = open(p_localize_lock, "r").read().strip()
                                except:
                                    Common.warning(traceback.format_exc())
                                if other_hostname == hostname:
                                    Common.warning('Removing previous lock file @ "{}" created by'
                                        ' me, killed in action?'.format(p_localize_lock))
                                    os.remove(p_localize_lock)
                            if not os.path.exists(p_localize_lock):
                                # Attempt to take
                                Common.info("Attempting to take lock...")
                                with open(p_localize_lock, "w") as f:
                                    f.write(hostname)
                                # Wait 2 sek + random time
                                time.sleep(2+2*random.random())
                                # Did we get the lock
                                if os.path.exists(p_localize_lock):
                                    # Is it still me?
                                    the_hostname = "?"
                                    try:
                                        the_hostname = open(p_localize_lock, "r").read().strip()
                                    except:
                                        Common.warning(traceback.format_exc())
                                    if the_hostname == hostname:
                                        conversions = []
                                        def clean_path(p):
                                            p = (p or "").strip()
                                            if p.startswith('"'):
                                                p = p[1:]
                                            if p.endswith('"'):
                                                p = p[:len(p)-1]
                                            return p
                                        # First supply root share conversions
                                        for share in self.data.get('shares') or []:
                                            prefix_from = prefix_to = None
                                            for path_ident, prefix in share.get('paths',{}).items():
                                                if path_ident.lower() == Common.get_os().lower():
                                                    # My platform
                                                    prefix_to = prefix
                                                elif path_ident.lower() == self.get_remote_os().lower():
                                                    prefix_from = prefix
                                            if prefix_from and prefix_to:
                                                conversions.append((clean_path(prefix_from), clean_path(prefix_to)))
                                        # Any conversions from remote end?
                                        if 'mapped_share_paths' in self.get_compute()['parameters']:
                                            for d in self.get_compute()['parameters']['mapped_share_paths']:
                                                if not 'os' in d or d['os'].lower() == Common.get_os().lower():
                                                    conversions.append((clean_path(d['remote']), 
                                                        clean_path(self.normalize_path(d['local']))))
                                        Common.info('Lock aquired, parsing input file'
                                            '(conversions: {})...'.format(conversions))
                                        try:
                                            with open(p_input, "r") as f_src:
                                                with open(p_input_localized, "w") as f_dst:
                                                    self.convert_input(f_src, f_dst, conversions)
                                            # Write metadata
                                            with open(p_localized_metadata, "w") as f:
                                                json.dump({
                                                    'size':os.path.getsize(p_input),
                                                    'time':os.path.getmtime(p_input)
                                                }, f)
                                        finally:
                                            if os.path.exists(p_localize_lock):
                                                os.remove(p_localize_lock)
                                                Common.info("Released lock @ '{}'..."\
                                                    .format(p_localize_lock))
                                    else:
                                        Common.info('(Localization) Another node grabbed the lock'
                                            ' after me, aborting localize: {}'.format(the_hostname))
                                        other_is_localizing = True
                                else:
                                    Common.warning('(Localization) Lock file dissappeared during'
                                        ' vote, must have been a quick localize!')
                            else:
                                other_is_localizing = True
                            if other_is_localizing:
                                other_hostname = '?'
                                try:
                                    other_hostname = open(p_localize_lock, 'r').read().strip()
                                except:
                                    Common.warning(traceback.format_exc())
                                Common.warning('(Localization) Another machine is already'
                                    ' localizing({}), waiting for it to finish...'.format(other_hostname))
                                while os.path.exists(p_localize_lock):
                                    time.sleep(1)
                        else:
                            Common.info('(Localization) Using up-to-date localized input'
                                ' file (size: {}, mtime: {})'.format(
                                    os.path.getsize(p_input_localized), 
                                    datetime.datetime.fromtimestamp(os.path.getmtime(
                                        p_input_localized))))
                        # Use this from now on
                        self.get_compute()['input'] = p_input_localized
                    else:
                        Common.info('(Localization) No need to localize ({} == {}).'\
                            .format(p_input_localized, p_input))
            else:
                Common.info('(Localization) Not attempting to localize input file,'
                    ' no input_conversion in parameters.')


    def get_common_envs(self):
        return self.get_envs()

    def kill(self):
        ''' Kill the current running PID '''
        if not self.executing or self.process is None:
            Common.warning('Refusing terminate - not running or have no process info!')
            return
        Common.warning('Terminating PID: {}'.format(self.process.pid))
        if Common.is_win():
            os.system('TASKKILL /f /PID {}'.format(self.process.pid))
        else:
            os.system('kill -9 {}'.format(self.process.pid))
        #try:
        #   self.process.terminate()
        #except:
        #   Common.warning(traceback.format_exc())

    def execute(self):
        ''' Compute '''
        self.prepare()
        commands = self.get_commandline(self.item)
        if commands is None or len(commands)==0:
            raise Exception("Empty command line!")
        app_envs = self.get_common_envs()
        if len(app_envs)==0:
            app_envs = None
        log = True
        exitcode = None
        self.executing = True
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
                Common.info("Environment variables: '{}'".format(new_envs))
            creationflags = self.get_creation_flags(self.item)
            Common.info("Running '{}'".format(str([Common.safely_printable(s) for s in commands])))
            if stdin:
                Common.info("Stdin: '{}".format(stdin))
            if creationflags:
                Common.info("Creation flags: '{}".format(creationflags))
            Common.info("-"*120)

            first_run = True
            if stdin:
                if not creationflags is None:
                    self.process = subprocess.Popen(
                        commands, 
                        shell, 
                        stdin=subprocess.PIPE, 
                        env=new_envs, 
                        creationflags=creationflags, 
                        stdout=subprocess.PIPE, 
                        stderr=subprocess.STDOUT)
                else:
                    self.process = subprocess.Popen(
                        commands, 
                        shell, 
                        stdin=subprocess.PIPE, 
                        env=new_envs, 
                        stdout=subprocess.PIPE, 
                        stderr=subprocess.STDOUT)

            else:
                if not creationflags is None:
                    self.process = subprocess.Popen(
                        commands, True, 
                        env=new_envs, 
                        creationflags=creationflags, 
                        stdout=subprocess.PIPE, 
                        stderr=subprocess.STDOUT)
                else:
                    self.process = subprocess.Popen(
                        commands, True, 
                        env=new_envs, 
                        stdout=subprocess.PIPE, 
                        stderr=subprocess.STDOUT)
            #exitcode = self.process.wait()
            while True:
                if first_run:
                    Common.info('Process PID: {}'.format(self.process.pid))
                    if stdin:
                        self.process.stdin.write(stdin)
                    first_run = False

                # Empty data waiting for us in pipes
                stdout = self.process.stdout.readline()
                print(stdout, end = '')
                sys.stdout.flush()

                process_result = self.process_output(stdout, '')
                if not process_result is None:
                    Common.warning('Premptive terminating process (pid: {}).'.format(
                        self.process.pid))
                    exitcode = process_result
                    break
                elif stdout == '' and self.process.poll() is not None:
                    break

            self.process.communicate() # Dummy call, needed?
            if exitcode is None:
                exitcode = self.process.returncode

            #exitcode = process.poll()
            #if exitcode is None:
            #   time.sleep(0.2) # Still running, breathe
            #else:
            #   break

        finally:
            try:
                self.process.terminate()
            except:
                pass
            self.executing = False

        self.process = None

        if not self.exitcode_force is None:
            exitcode = self.exitcode_force
            Common.info('Exitcode (forced): {}'.format(exitcode))
        else:
            Common.info('Exitcode: {}'.format(exitcode))

        # Check exitcode
        if exitcode != 0 and exitcode in self.get_allowed_exitcodes():
            Common.warning('Interpreting exitcode {} as ok(0)!'.format(exitcode))
            exitcode = 0

        assert (exitcode == 0) , ("Execution failed, check log for clues...")

        return exitcode

    def get_allowed_exitcodes(self):
        return [0]