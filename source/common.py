"""
    accsyn Common compute app

    Inherited by all other apps.

    Changelog:

        * v1r48; [Henrik Norin, 25.09.17] Only aggregate stdout & stderr flush during execution.
        * v1r48; [Henrik Norin, 25.09.10] Dropped Python 2 support. Improved stderr support and logging. Utility for scanning finished frames.
        * v1r47; [Henrik Norin, 25.08.18] Added str_file_size helper util.
        * v1r46; [Henrik Norin, 25.04.06] Fix bug where paths on the form volume=<id>/.. did not convert to the correct volume native path.
        * v1r45; [Henrik Norin, 24.12.23] Support situations were output path is a file and not a directory - prevent folder creation. Store raw input & output path, as it were before prepare/localization.
        * v1r44; [Henrik Norin, 24.12.13] Support wider accsyn path notations such as volume=..., folder=..., collection=... and home=...
        * v1r43; [Henrik Norin, 24.11.26] Fixed bug where it was looking for deprecated 'r_s'(root share) key in share mappings.
        * v1r42; [Henrik Norin, 24.11.14] Use new volume share notation. Support accsyn paths on the form 'share=(default)/share/path'. Define working path. Properly handle Popen shell argument, now defaults to false.
        * v1r41; [Henrik Norin, 24.10.08] Fixed bug in building arguments, default should be to not join.
        * v1r40; [Henrik Norin, 24.10.03] Fixed bug in console logging.
        * v1r39; [Henrik Norin, 24.07.02] Support JSON decode of data.
        * v1r38; [Henrik Norin, 23.11.20] Prevent crash on non encodeable stdout/stderr. Support for escaping backlash in
        arguments using %5C syntax, %20 to whitespace. Support variable substitution.
        * v1r37; [Henrik Norin, 23.10.31] Support arguments both as string and list type.
        * v1r36; [Henrik Norin, 23.10.18] Support envs supplied with job.
        * v1r35; [Henrik Norin, 23.10.03] Support multiple inputs.
        * v1r34; [Henrik Norin, 23.09.03] Change PID print to register it for additional failsafe termination on exit.
        * v1r33; [Henrik Norin, 23.08.16] Support for post and pre execution functions.
        * v1r32; [Henrik Norin, 23.05.11] PEP8 code style alignments.
        * v1r30; [Henrik Norin, 23.01.17] Support for escaping arguments with whitespaces using "%22spaced value%22"
            syntax.
        * v1r29; [Henrik Norin, 22.11.11] Comply with accsyn v2.2 task progress.
        * v1r27; [Henrik Norin, 22.08.31] Prevented infinite loop on input conversion if to path is same as from path.
        * v1r26; [Henrik Norin, 22.07.12] Change directory to home.
        * v1r25; [Henrik Norin, 22.05.12] Separate render app output (public) from accsyn script output (non restricted
            user accessible)
        * v1r24; [Henrik Norin, 22.05.09] (Localization) An additional convert_path call if a line contains
            path delimiters, mostly for fully converting Nuke scripts.
        * v1r23; [Henrik Norin, 22.05.09] Lock taken on clearing output directory.
        * v1r22; [Henrik Norin, 22.05.09] Support for clearing out output directory before render starts.
        * v1r21; [Henrik Norin, 22.05.09] Python backward compability, code style.
        * v1r20; [Henrik Norin, 21.12.16] Convert input; properly concat paths having multiple slashes.
        * v1r19; [Henrik Norin, 21.12.16] Support multiple output paths
        * v1r16; [Henrik Norin] Fixed Windows path conversion bug.
        * v1r15; [Henrik Norin] Python 3 compliance. OS dependent path conversions.
        * v1r13; [Henrik Norin] Process creation flags support.
        * v1r12; [Henrik Norin] Compliance to accsyn v1.4.
        
    This software is provided "as is" - the author and distributor can not be 
    held  responsible for any damage caused by executing this script in any 
    means.

    Author: Henrik Norin, HDR AB

"""
from __future__ import print_function
import os
import sys
import json
import subprocess
import traceback
import socket
import time
import random
import datetime
import shutil
import signal
import re
import threading
import queue  # Python 3

#logging.basicConfig(
#    format="(%(asctime)-15s) %(message)s",
#    level=logging.INFO,
#    datefmt="%Y-%m-%d %H:%M:%S",
#    handlers=[
#        logging.StreamHandler()  # Add a StreamHandler to output logs to the terminal
#    ]
#)

class Common(object):
    __revision__ = 49

    OS_LINUX = "linux"
    OS_MAC = "mac"
    OS_WINDOWS = "windows"
    OS_RSB = "rsb"

    OUTPUT_METADATA_FILENAME = ".accsyn-compute-metadata.json"

    _dev = False
    _debug = False
    _last_stdout_flush = time.time()
    _last_stderr_flush = time.time()

    # Engine configuration
    # IMPORTANT NOTE:
    # This section defines engine behaviour and should not be refactored or moved
    # away from the enclosing START/END markers.
    # -- ENGINE CONFIG START --

    # SETTINGS
    # Can be retrieved during execution from: self.get_compute()["settings"]
    #  - items; If true, each input file can be split and executed in ranges
    # (render)
    #  - default_range; (items) The default item range.
    #  - default_bucketsize; The default amount of items to dispatch to each
    # compute node/machine.
    #  - filename_extensions: Comma(,) separated list of filename extensions
    # associated with app.
    #  - binary_filename_extensions: Comma(,) separated list of filename
    # extensions that indicated a binary non parseable format.

    # PARAMETERS
    # Can be retrieved during execution from: self.get_compute()["parameters"]
    #  - arguments; The additional command line arguments to pass on to app.
    #  - remote_os; The operating system ("windows", "linux" or "mac") on
    # machine that submitted the job, used for parsing below.
    #  - input_conversion; Define what kind of input file path conversion should
    #  happen on compute cluster (non binary formats only):
    #    * "always" - expect that the input files always needs paths converted
    # - contains local share mapped paths.
    #    * "platform" - all paths are assumed being relative root share, not
    # conversion needed except when switching platform (and platform path
    # prefixes differs).
    #    * "never" - all paths are assumed being relative root share and works
    # on all platforms, or are relative and do need conversion.
    #  - mapped_share_paths(input_conversion=always); List of dicts on the form
    #  {"remote":"D:\\PROJECTS\\MYSHARE",
    # "global":"share=root_share_id/share_path"}, used during input file parsing.

    # -- ENGINE CONFIG END --

    def __init__(self, argv):
        # Expect path to data in argv
        self.executing = False
        self.exitcode_force = None
        self.process = None
        self.data = dict()

        if "--dev" in argv:
            Common._dev = True
            Common._debug = True
            self.debug("Dev mode on{0}".format(" dev machine" if self.is_devmachine() else ""))
        assert len(sys.argv) == 2 or (
            len(sys.argv) == 3 and self.is_dev()
        ), "Please provide path to compute data (json) as only argument!"
        self._path_data = argv[1] if not self.is_dev() else argv[2]
        # (Parallellizable apps) The part to execute
        self.item = os.environ.get("ACCSYN_ITEM")
        # Find out and report my PID, write to sidecar file
        self.path_pid = os.path.join(os.path.dirname(self._path_data), "process.pid")
        with open(self.path_pid, "w") as f:
            f.write(str(os.getpid()))
        if self.is_debug():
            self.debug("accsyn PID({0}) were successfully written to '{1}'..".format(os.getpid(), self.path_pid))
        else:
            Common.info("Accsyn PID({0})".format(os.getpid()))
        self.check_mounts()
        self._current_task = None
        self._background_worker_thread = None

    @staticmethod
    def get_path_version_name():
        p = os.path.realpath(__file__)
        parent = os.path.dirname(p)
        return os.path.dirname(parent), os.path.basename(parent), os.path.splitext(os.path.basename(p))[0]

    # Helpers

    def get_compute(self):
        return self.data.get("compute", {})

    def get_site_code(self):
        """Return the name site we are running at"""
        if not self.data.get("site") is None:
            return self.data["site"].get("code")
        return None

    @staticmethod
    def get_os():
        if sys.platform == "darwin":
            if os.name == "mac" or os.name == "posix":
                return Common.OS_MAC
            else:
                return Common.OS_LINUX
        elif sys.platform in ["linux", "linux2"]:
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
    def is_mac():
        return Common.get_os() == Common.OS_MAC

    @staticmethod
    def is_win():
        return Common.get_os() == Common.OS_WINDOWS

    def get_remote_os(self):
        remote_os = self.get_compute()["parameters"].get("remote_os")
        if remote_os is None:
            remote_os = self.get_os()
            self.debug("Do not know remote OS, assuming same.")
        return remote_os.lower()

    @staticmethod
    def safely_printable(s):
        if s is None:
            return None
        if isinstance(s, bytes):
            try:
                return s.decode("utf-8")
            except:
                try:
                    return s.decode("utf-16")
                except Exception as e:
                    Common.warning(str(e))
        if not isinstance(s, str):
            s = str(s)
            if isinstance(s, bytes):
                try:
                    return s.decode("utf-8")
                except Exception as e:
                    Common.warning(str(e))
                    return "(unprintable bytes)"
            else:
                return s
        else:
            return s

    @staticmethod
    def log(s, end=None, with_date=True, aggregated_flush=False):
        """Log to public log @ stdout"""
        try:
            date_part = f'({datetime.datetime.now()}) ' if with_date else ''
            if end is not None:
                print(f"!{date_part}{s}", end=end)
            else:
                print(f"!{date_part}{s}")
            if not aggregated_flush or time.time() - Common._last_stdout_flush > 2: # Flush every 2 seconds
                Common._last_stdout_flush = time.time()
                sys.stdout.flush()
        except Exception as e:
            Common.warning(str(e))
 
    @staticmethod
    def log_stderr(s, end=None, with_date=True, aggregated_flush=False):
        """ Log to public log @ stderr """
        try:
            date_part = f'({datetime.datetime.now()}) ' if with_date else ''
            if end is not None:
                print(f"!{date_part}{s}", file=sys.stderr, end=end)
            else:
                print(f"!{date_part}{s}", file=sys.stderr)
            if not aggregated_flush or time.time() - Common._last_stderr_flush > 2: # Flush every 2 seconds
                Common._last_stderr_flush = time.time()
                sys.stderr.flush()
        except Exception as e:
            Common.warning(str(e))
 
    @staticmethod
    def info(s, end=None, with_date=True, aggregated_flush=False):
        """ Log to service log"""
        try:
            date_part = f'({datetime.datetime.now()}) ' if with_date else ''
            if end is not None:
                print(f"{date_part}[INFO] [ACCSYN] {s}", end=end)
            else:
                print(f"{date_part}[INFO] [ACCSYN] {s}")
            if not aggregated_flush or time.time() - Common._last_stdout_flush > 2: # Flush every 2 seconds
                Common._last_stdout_flush = time.time()
                sys.stdout.flush()
        except Exception as e:
            Common.warning(str(e))

    @staticmethod
    def warning(s, append_prefix=True, end=None, with_date=True, aggregated_flush=False):
        """ Warn to service log"""
        try:
            date_part = f'({datetime.datetime.now()})' if with_date else ''
            if end is not None:
                print(f"{date_part}{'[WARNING] [ACCSYN] ' if append_prefix else ''}{s}", file=sys.stderr, end=end)
            else:
                print(f"{date_part}{'[WARNING] [ACCSYN] ' if append_prefix else ''}{s}", file=sys.stderr)
            if not aggregated_flush or time.time() - Common._last_stderr_flush > 2: # Flush every 2 seconds
                Common._last_stderr_flush = time.time()
                sys.stderr.flush()
        except Exception as e:
            Common.warning(str(e))

    # PATH CONVERSION

    def get_mapped_share_path(self, share_code_or_id):
        self.debug("get_mapped_share_path({0})".format(share_code_or_id))
        if "config" not in self.data["client"]:
            return None
        if "sharepaths" not in self.data["client"]["config"]:
            return None
        for entry in self.data["client"]["config"]["sharepaths"]:
            if entry["share"] == share_code_or_id or entry.get("share_hr") == share_code_or_id:
                self.debug(
                    "(Share {0} path normalize) Using locally mapped path: {1}".format(share_code_or_id, entry["path"])
                )
                return entry["path"]

    @staticmethod
    def is_accsyn_path(path):
        """Return true if *path* is on the accsyn form 'share=...' or similar"""
        return (path and path.lower().startswith("share=") or path.lower().startswith("volume=") or
                path.lower().startswith("folder=") or path.lower().startswith("collection=") or path.lower().startswith("home="))

    def normalize_path(self, p):
        """Based on share mappings supplied, convert a foreign raw accsyn path on the form
        'share=<volume, shared folder or collection code or id>/path' to local platform"""
        self.debug("normalize_path({0})".format(p))
        if p is None or 0 == len(p):
            return p
        try:
            p_orig = str(p)
            prefix_from = prefix_to = share_code_or_id = None
            # Turn paths
            p = p.replace("\\", "/")
            if Common.is_accsyn_path(p):
                # A path relative share, must be made relative volume first
                idx_slash = p.find("/")
                p_rel = None
                if -1 < idx_slash:
                    share_code_or_id = p[:idx_slash].split("=")[-1]
                    p_rel = p[idx_slash + 1:]
                else:
                    share_code_or_id = p.split("=")[-1]
                if share_code_or_id.lower() == "(default)":
                     # Locate and use the default share
                     for share in self.data.get("shares") or []:
                        if share.get("default") is True:
                            share_code_or_id = share["code"]
                            break
                # Check if re-mapped locally
                prefix_to = self.get_mapped_share_path(share_code_or_id)
                # Should be provided so we can convert to volume relative path
                if prefix_to is None and "share_paths" in self.get_compute()["parameters"]:
                    if share_code_or_id in self.get_compute()["parameters"]["share_paths"]:
                        d = self.get_compute()["parameters"]["share_paths"][share_code_or_id]
                        share_path = d["s_path"]
                        p_orig = str(p)
                        p = "share={0}{1}{2}".format(
                            d.get("volume", d["v"]),
                            ("/" + share_path) if 0 < len(share_path) and share_path not in ["/", "\\"] else "",
                            ("/" + p_rel) if p_rel else "",
                        )
                        self.debug(
                            "(Share path normalize) Converted share "
                            "relative path '{0}' > volume relative: '{1}' (share"
                            " paths: {2})".format(p_orig, p, self.get_compute()["parameters"]["share_paths"])
                        )
            if prefix_to is None:
                # On a known share that can be converted?
                for share in self.data.get("shares") or []:
                    if share_code_or_id and not (share["code"].lower() == share_code_or_id.lower() or
                                                 share["id"].lower() == share_code_or_id.lower()):
                        continue
                    for path_ident, prefix in share.get("paths", {}).items():
                        self.debug(
                            "(Volume {0} path normalize) path_ident.lower()"
                            ": '{1}', Common.get_os().lower(): '{2}'"
                            "(prefix_from: {3}, prefix_to: {4})".format(
                                share['code'], path_ident.lower(), Common.get_os().lower(), prefix_from, prefix_to
                            )
                        )
                        if path_ident.lower() == Common.get_os().lower():
                            # My platform
                            prefix_to = prefix
                        else:
                            if 0 < len(prefix) and p.lower().find(prefix.lower()) == 0:
                                prefix_from = prefix
                    if prefix_to:
                        if not prefix_from:
                            # Starts with accsyn share notation?
                            s = "share={0}".format(share["code"])
                            if p.startswith(s):
                                prefix_from = s
                            else:
                                s = "share={0}".format(share["id"])
                                if p.startswith(s):
                                    prefix_from = s
                        # Mapped locally?
                        prefix_to_mapped = self.get_mapped_share_path(share["code"])
                        if prefix_to_mapped:
                            prefix_to = prefix_to_mapped
                        if prefix_from:
                            break
                if prefix_from is None:
                    # Any supplied mapped share path conversions?
                    if "mapped_share_paths" in self.get_compute()["parameters"]:
                        for d in self.get_compute()["parameters"]["mapped_share_paths"]:
                            self.debug("(Supplied mapped shares path normalize) entry: '{0}'".format(d))
                            if p.lower().startswith(d["remote"].replace("\\", "/").lower()):
                                prefix_from = d["remote"]
                                prefix_to = d["local"]
                                if Common.is_accsyn_path(prefix_to):
                                    share_id_or_code = prefix_to.split("=")[-1]
                                    for share in self.data.get("shares") or []:
                                        if (
                                            share["id"].lower() == share_id_or_code.lower()
                                            or share["code"].lower() == share_id_or_code.lower()
                                        ):
                                            if Common.get_os().lower() in share.get("paths", {}):
                                                prefix_to = share["paths"][Common.get_os().lower()]
                                            break
                                    if Common.is_accsyn_path(prefix_to):
                                        raise Exception(
                                            "Cannot find volume {0}"
                                            " for remote mapped path conversion {1} for my os({2})!".format(
                                                share_id_or_code, d, Common.get_os()
                                            )
                                        )
                                break
            if prefix_to:
                if Common.is_accsyn_path(p):
                    idx_slash = p.find("/")
                    p = prefix_to + (p[idx_slash:] if -1 < idx_slash else "")
                elif prefix_from:
                    p = prefix_to + (
                        ("/" if prefix_to[-1] != "/" and p[len(prefix_from)] != "/" else "") + p[len(prefix_from) :]
                        if len(prefix_from) < len(p)
                        else ""
                    )

            # Turn back paths
            if Common.is_win():
                p = p.replace("/", "\\")
            if p != p_orig:
                self.debug("Converted '{}'>'{}'".format(p_orig, p))
            elif prefix_from and prefix_to:
                self.debug(
                    "No conversion of path '{0}' needed (prefix_from: "
                    "{1}, prefix_to: {2})".format(p_orig, prefix_from, prefix_to)
                )
        except:
            Common.warning(
                "Cannot normalize path, data '{0}' has wrong format?"
                "Details: {1}".format(json.dumps(self.data, indent=3, cls=JSONEncoder), traceback.format_exc())
            )

        if Common.is_accsyn_path(p):
            # Will never work
            raise Exception("Cannot convert accsyn path {0} to local!".format(p))

        return p

    # DEBUGGING ###############################################################

    def is_dev(self):
        return (
            Common._dev
            or (os.environ.get("ACCSYN_DEV") or "") in ["1", "true"]
            or (self.get_compute() or {}).get("parameters", {}).get("dev") is True
        )

    def is_devmachine(self):
        import socket

        return self.is_dev() and -1 < socket.gethostname().lower().find("ganymedes")

    def is_debug(self):
        return (
            Common._debug
            or (os.environ.get("ACCSYN_DEBUG") or "") in ["1", "true"]
            or (self.get_compute() or {}).get("parameters", {}).get("debug") is True
        )

    def debug(self, s):
        if self.is_debug():
            print("<<ACCSYN APP DEBUG>> {0}".format(s))
            if time.time() - Common._last_stdout_flush > 2: # Flush every 2 seconds
                Common._last_stdout_flush = time.time()
                sys.stdout.flush()

    @staticmethod
    def set_debug(debug):
        Common._debug = debug

    # Functions that should be overridden by child class ######################

    def get_envs(self):
        """(OPTIONAL) Return dict holding additional environment variables."""
        return {}

    def probe(self):
        """(OPTIONAL) Return False if not implemented, return True if found,
        raise exception otherwise."""
        return False

    def check_mounts(self):
        """(OPTIONAL) Make sure all network drives are available prior to
        compute."""
        pass

    @staticmethod
    def concat_paths(p1, p2):
        if p1 is None or p2 is None:
            return None
        while p1.replace("\\", "/").endswith("/"):
            p1 = p1[:-1]
        while p2.replace("\\", "/").startswith("/"):
            p2 = p2[1:]
        if 0 < len(p1) and 0 < len(p2):
            return os.path.join(p1, p2)
        elif 0 < len(p1):
            return p1
        else:
            return p2

    def convert_input(self, f_src, f_dst, conversions):
        """Basic ASCII file path conversion, should be overridden by engine to
        support custom conversions. Raise an exception is localization fails."""
        line_no = 1
        # Collect envs
        envs = self.get_common_envs()
        for line in f_src:
            try:
                had_conversion = False
                line_orig = str(line)
                for path_from, path_to in conversions:
                    if path_from == path_to:
                        continue
                    while True:
                        idx = line.lower().find(path_from.lower())
                        if idx == -1:
                            if -1 < path_from.find("\\") and -1 < line.find("/"):
                                # Windows to *NIX
                                idx = line.lower().find(path_from.replace("\\", "/").lower())
                            elif -1 < path_from.find("/") and -1 < line.find("\\"):
                                # *NIX to Windows
                                idx = line.lower().find(path_from.replace("/", "\\").lower())
                        if idx == -1:
                            break
                        line = (line[0:idx] if 0 < idx else "") + Common.concat_paths(
                            self.convert_path(path_to, envs=envs),
                            (line[idx + len(path_from) :] if idx + len(path_from) < len(line) else ""),
                        )
                        had_conversion = True
                if had_conversion:
                    if -1 < line.find("/") or -1 < line.find("\\"):
                        line = self.convert_path(line, envs=envs)
                    if line != line_orig:
                        self.info("(input convert) '{0}'>'{1}'".format(line_orig, line))
            except:
                Common.warning(traceback.format_exc())
                Common.warning("Could not convert line #{0}, leaving as is...".format(line_no))
            f_dst.write("{0}".format(line))
            line_no += 1

    def convert_path(self, p, envs=None):
        """Can be overridden by engine to provide further path alignment."""
        if envs:
            # Replace ${ENV} occurrences in path
            for key, value in list(envs.items()):
                search_str = "${%s}" % key
                if search_str in p:
                    p = p.replace(search_str, value)
                    self.debug("(Convert path '{}') Replaced with env {}={}".format(
                        p, key, value))
        return p

    def get_executable(self):
        """(REQUIRED) Return path to executable as string"""
        raise NotImplementedError("Get executable not overridden by engine!")

    def get_commandline(self, item):
        """(REQUIRED) Return command line as a string array"""
        raise NotImplementedError("Get commandline not overridden by engine!")

    def get_stdin(self, item):
        """(OPTIONAL) Return stdin as text to be sent to engine."""
        return None

    def get_creation_flags(self, item):
        """(OPTIONAL) Return the process creation flags."""
        return None

    def get_working_path(self):
        """(OPTIONAL) Return the working path for the process, should be overridden by engine."""
        return os.path.expanduser("~")

    def shell(self):
        """(OPTIONAL) Return True if command should be executed in shell."""
        return False

    def do_print(stderr=False):
        """ Return True if the output should be printed to the console. To be overridden by engine. """
        return True

    def process_output(self, stdout, stderr):
        """
        Sift through stdout/stderr and take action. To be overridden by engine.

        Return value:
           None (default); Do nothing, keep execution going.
           integer; Terminate process and force exit code to this value.
        """       
        return None

    def get_background_worker(self):
        """(interval, func) tuple containing the interval in seconds (float) and function to run in the background. Return None if no background worker is needed. To be overridden by engine."""
        return None

    ###########################################################################

    def load(self):
        """Load the data from disk, must be run BEFORE execution"""
        assert os.path.exists(self._path_data) or os.path.isdir(
            self._path_data
        ), "Data not found or is directory @ '{0}'!".format(self._path_data)
        try:
            self.data = json.load(open(self._path_data, "r"), cls=JSONDecoder)
        except:
            Common.warning(
                "Loading the execution data caused exception {0}: {1}".format(
                    traceback.format_exc(), open(self._path_data, "r").read()
                )
            )
            raise
        self.debug("Data loaded:\n{0}".format(json.dumps(self.data, indent=3, cls=JSONEncoder)))

    def take_lock(self, base_path, operation):
        """Return True if we can take a lock on a file operation."""
        other_is_localizing = False
        hostname = socket.gethostname()
        lock_path = os.path.join(
            os.path.dirname(base_path), "{0}.{1}_lock".format(os.path.basename(base_path), operation)
        )
        Common.info("Checking {0} lock @ '{1}'...".format(operation, lock_path))
        if os.path.exists(lock_path):
            # Is it me?
            other_hostname = "?"
            try:
                other_hostname = open(lock_path, "r").read().strip()
            except:
                Common.warning(traceback.format_exc())
            if other_hostname == hostname:
                Common.warning(
                    "Removing previous lock file @ '{0}' created by me, killed in action?".format(lock_path)
                )
                os.remove(lock_path)
        if not os.path.exists(lock_path):
            # Attempt to take
            Common.info("Attempting to take {0} lock...".format(operation))
            with open(lock_path, "w") as f:
                f.write(hostname)
            # Wait 2 sek + random time
            time.sleep(2 + 2 * random.random())
            # Did we get the lock
            if os.path.exists(lock_path):
                # Is it still me?
                the_hostname = "?"
                try:
                    the_hostname = open(lock_path, "r").read().strip()
                except:
                    Common.warning(traceback.format_exc())
                if the_hostname == hostname:
                    return True, lock_path
                else:
                    Common.info(
                        "Another node grabbed the lock after me, aborting {0}: {0}".format(operation, the_hostname)
                    )
                    other_is_localizing = True
            else:
                Common.warning("Lock file disappeared during vote, must have been a quick {0}!".format(operation))
        else:
            other_is_localizing = True
        if other_is_localizing:
            other_hostname = "?"
            try:
                other_hostname = open(lock_path, "r").read().strip()
            except:
                Common.warning(traceback.format_exc())
            Common.warning(
                "(Localization) Another machine is already doing {0}({1}), waiting for it to finish...".format(
                    operation, other_hostname
                )
            )
            while os.path.exists(lock_path):
                time.sleep(1)
        return False, lock_path

    def get_input(self):
        """ Return the input file, single entry or based on item/other parameter. Can be overridden by engine
        as needed """
        if "input" in self.get_compute():
            return self.get_compute()["input"]
        return None

    def set_input(self, value):
        """ Set the input file, single entry or based on item/other parameter. Can be overridden by engine as needed"""
        if "raw_input" not in self.get_compute():
            self.get_compute()["raw_input"] = self.get_input()  # Store original input
        self.get_compute()["input"] = value

    def get_output(self):
        """ Return the output file or folder, single entry or based on item/other parameter. Can be overridden by engine
        as needed """
        if "output" in self.get_compute():
            return self.get_compute()["output"]
        return None

    def create_output_folder(self):
        """ Return true if the output folder should be created during prepare, false if it should not.
        Can be overridden by engine"""
        return self.get_compute().get('create_output_folder', True)

    def output_is_folder(self):
        """ Return true if the output is a folder, false if it is a file. Can be overridden by engine as needed """
        return self.get_compute().get("output_is_folder", True)

    def prepare(self):
        """Prepare execution - localize files."""
        # Any input file?
        p_input = self.get_input()
        if p_input:
            p_input = self.normalize_path(p_input)
            # Store it
            self.set_input(p_input)
            if p_input and -1 < p_input.find(os.sep):
                if not os.path.exists(p_input):
                    Common.warning("(Localization) Input file/scene does not exists @ '{}'!".format(p_input))
                elif "input_conversion" in self.get_compute()["parameters"]:
                    # Is input conversion/localizing needed?
                    input_conversion = self.get_compute()["parameters"]["input_conversion"]
                    Common.info("(Localization) Input conversion mode: {}".format(input_conversion))
                    if input_conversion is None:
                        input_conversion = "platform"
                    if input_conversion == "auto":
                        # Set to always if any paths supplied
                        if "mapped_share_paths" in self.get_compute()["parameters"] and 0 < len(
                            self.get_compute()["parameters"]["mapped_share_paths"]
                        ):
                            Common.info(
                                "(Localization) Mapped share paths supplied, applying 'always' input conversion mode."
                            )
                            input_conversion = "always"
                        else:
                            Common.info(
                                "(Localization) No mapped share paths supplied, appling 'platform' input conversion "
                                "mode."
                            )
                            input_conversion = "platform"
                    if input_conversion == "never":
                        Common.info("(Localization) Not attempting to localize input file, input conversion disabled.")
                    else:
                        # Is an ASCII parsable format?
                        is_binary_format = False
                        binary_filename_extensions = self.SETTINGS.get("binary_filename_extensions")
                        if 0 < len(binary_filename_extensions or ""):
                            if -1 < p_input.find("."):
                                ext = os.path.splitext(p_input)[1]
                                if binary_filename_extensions == "*" or -1 < binary_filename_extensions.lower().find(
                                    ext.lower()
                                ):
                                    # No, this is a binary format - cannot localize
                                    Common.info(
                                        "Input file '{}' is a binary format - not attempting to localize...".format(
                                            p_input
                                        )
                                    )
                                    is_binary_format = True
                                else:
                                    Common.info(
                                        "Input file '{}' is a ASCII format - extension {} does not match {}...".format(
                                            p_input, ext, binary_filename_extensions
                                        )
                                    )
                            else:
                                Common.info(
                                    "(Localization) input file does not have any extension, cannot determine if not "
                                    "binary format and in need of localization..."
                                )
                                is_binary_format = True
                        else:
                            Common.info("All file formats are ASCII (no 'binary_filename_extensions' in config)")
                        if not is_binary_format:
                            p_input_localized = p_input
                            p_input_prefix, p_localized_ext = os.path.splitext(p_input)
                            if input_conversion == "always":
                                p_input_localized = "{}_accsynlocalized_hq_{}{}".format(
                                    p_input_prefix, self.get_os(), p_localized_ext
                                )
                            elif input_conversion == "platform":
                                remote_os = self.get_remote_os()
                                if remote_os != self.get_os():
                                    # Does the volume path differ between platforms?
                                    # TODO: Support site volume path overrides
                                    remote_prefix = local_prefix = None
                                    for share in self.data.get("shares") or []:
                                        if remote_os in share.get("paths", {}):
                                            remote_prefix = share["paths"][remote_os]
                                        if self.get_os() in share.get("paths", {}):
                                            local_prefix = share["paths"][self.get_os()]
                                        if remote_prefix and local_prefix:
                                            break
                                    if remote_prefix and local_prefix:
                                        if remote_prefix != local_prefix:
                                            p_input_localized = "{}_accsynlocalized_hq_{}{}".format(
                                                p_input_prefix, self.get_os(), p_localized_ext
                                            )
                                            Common.info(
                                                "(Localization) Remote volume path prefix ({}) and local ({}) "
                                                "differs, need to localize!".format(remote_prefix, local_prefix)
                                            )
                                        else:
                                            Common.info(
                                                "(Localization) Remote volume path prefix ({}) and local ({}) are "
                                                "the same, no need to localize...".format(remote_prefix, local_prefix)
                                            )
                                    if remote_prefix is None:
                                        Common.warning(
                                            "(Localization) Do not know remote volume prefix on {}, cannot "
                                            "localize '{}'...".format(remote_os, p_input)
                                        )
                                    if local_prefix is None:
                                        Common.warning(
                                            "(Localization) Do not know local volume prefix on {}, cannot localize"
                                            " '{}'...".format(self.get_os(), p_input)
                                        )
                                else:
                                    Common.info(
                                        "(Localization) On same platform({}), no need to convert path.".format(
                                            remote_os
                                        )
                                    )

                            if p_input_localized != p_input:
                                # Does it exist?
                                # Check if needs to be parsed and have paths converted
                                p_parent = os.path.dirname(p_input)
                                p_localized_metadata = os.path.join(
                                    p_parent, "{}.localized_metadata".format(os.path.basename(p_input))
                                )
                                do_localize = True
                                if os.path.exists(p_input_localized):
                                    if os.path.exists(p_localized_metadata):
                                        # Find out the size and mtime input file had when last localized
                                        d = json.load(open(p_localized_metadata, "r"))
                                        localized_size = d["size"]
                                        localized_mtime = d["time"]
                                        if os.path.getsize(p_input) != localized_size:
                                            Common.warning(
                                                "Localized file was based on input file that differs in size current "
                                                "({}<>{})!".format(localized_size, os.path.getsize(p_input))
                                            )
                                        elif os.path.getmtime(p_input) != localized_mtime:
                                            Common.warning(
                                                "Localized file was based on input file that differs in modification "
                                                "time ({}<>{})!".format(localized_mtime, os.path.getmtime(p_input))
                                            )
                                        else:
                                            # Localized is up to date
                                            do_localize = False
                                    else:
                                        Common.warning(
                                            "Localized file metadata does not exist @ '{}'!".format(
                                                p_localized_metadata
                                            )
                                        )
                                else:
                                    Common.warning("Localized file does not exist @ '{}'!".format(p_input_localized))
                                if do_localize:
                                    lock_taken, p_localize_lock = self.take_lock(p_input, "localize")
                                    if lock_taken:
                                        try:
                                            conversions = []
                                            # First supply volume conversions
                                            for share in self.data.get("shares") or []:
                                                prefix_from = prefix_to = None
                                                for path_ident, prefix in share.get("paths", {}).items():
                                                    if path_ident.lower() == Common.get_os().lower():
                                                        # My platform
                                                        prefix_to = prefix
                                                    elif path_ident.lower() == self.get_remote_os().lower():
                                                        prefix_from = prefix
                                                if len(prefix_from or "") > 0 and len(prefix_to or "") > 0:
                                                    conversions.append((prefix_from, prefix_to))
                                            # Any conversions from remote end?
                                            if "mapped_share_paths" in self.get_compute()["parameters"]:
                                                for mapping_data in self.get_compute()["parameters"][
                                                    "mapped_share_paths"
                                                ]:
                                                    if (
                                                        len(mapping_data["remote"] or "") > 0
                                                        and len(mapping_data["local"] or "") > 0
                                                    ):
                                                        if (
                                                            "os" not in mapping_data
                                                            or mapping_data["os"] == self.get_os().lower()
                                                        ):
                                                            try:
                                                                conversions.append(
                                                                    (
                                                                        mapping_data["remote"],
                                                                        self.normalize_path(mapping_data["local"]),
                                                                    )
                                                                )
                                                            except:
                                                                # Not critical
                                                                Common.warning(traceback.format_exc())
                                            Common.info(
                                                "Lock acquired, parsing input file using path conversions: {}".format(
                                                    conversions
                                                )
                                            )
                                            with open(p_input, "r") as f_src:
                                                with open(p_input_localized, "w") as f_dst:
                                                    self.convert_input(f_src, f_dst, conversions)
                                            # Write metadata
                                            with open(p_localized_metadata, "w") as f:
                                                json.dump(
                                                    {
                                                        "size": os.path.getsize(p_input),
                                                        "time": os.path.getmtime(p_input),
                                                    },
                                                    f,
                                                )
                                        finally:
                                            if os.path.exists(p_localize_lock):
                                                os.remove(p_localize_lock)
                                                Common.info("Released lock @ '{}'...".format(p_localize_lock))
                                else:
                                    Common.info(
                                        "(Localization) Using up-to-date localized input file "
                                        "(size: {}, mtime: {})".format(
                                            os.path.getsize(p_input_localized),
                                            datetime.datetime.fromtimestamp(os.path.getmtime(p_input_localized)),
                                        )
                                    )
                                # Use this from now on
                                self.get_compute()["input"] = p_input_localized
                            else:
                                Common.info(
                                    "(Localization) No need to localize ({} == {}).".format(p_input_localized, p_input)
                                )
                else:
                    Common.info(
                        "(Localization) Not attempting to localize input file, no input_conversion in parameters."
                    )
            else:
                Common.warning(
                    "(Localization) Not attempting to localize input file, null or not recognized as a path!"
                )
        # Any output file?
        if "output" in self.data["compute"]:
            self.get_compute()["raw_output"] = self.get_output()
            p_output = self.normalize_path(
                self.get_output()
            )
            self.get_compute()["output"] = p_output
            # Create output directory?
            if self.create_output_folder():
                p_output_folder = p_output if self.output_is_folder() else os.path.dirname(p_output)
                if not os.path.exists(p_output_folder):
                    try:
                        os.makedirs(p_output_folder)
                        self.warning("Created missing output folder: '{0}'".format(p_output_folder))
                    except:
                        self.warning(traceback.format_exc())
                else:
                    self.info("Folder '{0}' exists.".format(p_output_folder))
            if "clear_output_directory" in self.get_compute()["parameters"]:
                do_clear_output = None
                cod = self.get_compute()["parameters"]["clear_output_directory"]
                site_code = self.get_site_code()
                if cod.lower() == "true":
                    do_clear_output = True
                else:
                    parts = (cod or "").split(":")
                    if len(parts) == 2:
                        if parts[0] == "site":
                            do_clear_output = parts[1].lower() == site_code.lower()
                            if not do_clear_output:
                                self.debug("Not clearing out output directory - not on site: {0}".format(parts[1]))
                        elif parts[0] == "!site":
                            do_clear_output = parts[1].lower() != site_code.lower()
                            if not do_clear_output:
                                self.debug("Not clearing out output directory - we are on site: {0}".format(parts[1]))
                if do_clear_output is None:
                    self.warning("Do not know how to interpret 'clear_output_directory' setting: {0}".format(cod))
                elif do_clear_output:
                    p_metadata = os.path.join(p_output, Common.OUTPUT_METADATA_FILENAME)
                    do_move_files = False
                    if "job" in self.data:
                        job_data = self.data["job"]
                    else:
                        # BWCOMP, look in compute
                        job_data = {}
                        for key in ["id", "code", "user", "created"]:
                            job_data[key] = self.get_compute()[key]
                    self.info(
                        "Checking if output directory {0} needs to be cleared out "
                        "(setting: {1}, my site: {2}, job_data: {3})".format(p_output, cod, site_code, job_data)
                    )
                    if not os.path.exists(p_output):
                        self.debug("Output does not exists!")
                    else:
                        # Read metadatafile
                        if not os.path.exists(p_metadata):
                            self.warning("Metadata file does not exists, clearing content!")
                            do_move_files = True
                        else:
                            try:
                                prev_job_data = json.load(open(p_metadata, "r"))
                            except:
                                sys.stderr.write(traceback.format_exc())
                                self.warning("Metadata file is corrupt / unreadable, clearing content!")
                                os.remove(p_metadata)
                                do_move_files = True
                            else:
                                if prev_job_data["id"] == job_data["id"]:
                                    # Most likely
                                    self.info("Same job outputting to directory, not touching existing files!")
                                else:
                                    do_move_files = True
                                    self.warning(
                                        "Another job ({0}) has output to this directory, clearing...".format(
                                            prev_job_data
                                        )
                                    )
                        if do_move_files:
                            # Grab lock
                            lock_taken, p_clear_lock = self.take_lock(p_output, "output clear")
                            if lock_taken:
                                try:
                                    # Find files
                                    files_to_move = []
                                    p_destination_base = os.path.join(
                                        os.path.dirname(p_output), "ZZZ-TEMP", os.path.basename(p_output)
                                    )
                                    for filename in os.listdir(p_output):
                                        files_to_move.append(filename)
                                    if len(files_to_move) == 0:
                                        self.info("No output files to move away!")
                                    else:
                                        self.warning(
                                            "Moving {0} files(s) to '{1}'".format(
                                                len(files_to_move), p_destination_base
                                            )
                                        )
                                        for filename in files_to_move:
                                            p_source = os.path.join(p_output, filename)
                                            p_destination = os.path.join(p_destination_base, filename)
                                            if not os.path.exists(p_destination_base):
                                                self.warning("Creating: {0}".format(p_destination_base))
                                                os.makedirs(p_destination_base)
                                            elif os.path.exists(p_destination):
                                                self.warning("   Removing existing file: {0}".format(p_destination))
                                                try:
                                                    if os.path.isfile(p_destination):
                                                        os.remove(p_destination)
                                                    else:
                                                        shutil.rmtree(p_destination)
                                                except:
                                                    self.warning(traceback.format_exc())
                                                    time.sleep(2)
                                                if os.path.exists(p_destination):
                                                    self.warning(
                                                        "   Could not remove existing cleaned output file: {0}!".format(
                                                            p_destination
                                                        )
                                                    )
                                            try:
                                                os.rename(p_source, p_destination)
                                            except:
                                                self.warning(traceback.format_exc())
                                                time.sleep(2)
                                            if os.path.exists(p_source):
                                                self.warning(
                                                    "Could not clean existing output file to temp dir: {0} > {1} - "
                                                    "check permissions and disk space! Removing...".format(
                                                        p_source, p_destination
                                                    )
                                                )
                                                if os.path.isfile(p_source):
                                                    os.remove(p_source)
                                                else:
                                                    shutil.rmtree(p_source)
                                                if os.path.exists(p_source):
                                                    self.warning(
                                                        "   Could not remove output file: {0}!".format(p_source)
                                                    )
                                            else:
                                                self.warning("   Cleared out old output: '{0}'!".format(filename))
                                finally:
                                    if os.path.exists(p_clear_lock):
                                        os.remove(p_clear_lock)
                                        Common.info("Released lock @ '{}'...".format(p_clear_lock))

                            if not os.path.exists(p_metadata):
                                self.info("Writing job data {0} to metadata file: {1}".format(job_data, p_metadata))
                                # Create metadata file with our job info
                                if not os.path.exists(p_output):
                                    os.makedirs(p_output)
                                with open(p_metadata, "w") as f:
                                    f.write(json.dumps(job_data, indent=3, cls=JSONEncoder))
            else:
                self.info("No output clear directive passed!")

    def pre(self):
        """Pre execution, to be overridden"""
        pass

    def post(self, exitcode):
        """Post execution, to be overridden."""
        pass

    def get_common_envs(self):
        result = self.get_envs()
        if 'envs' in self.get_compute():
            # Encode envs, either given directly or in platform sub dicts
            platform_envs = False
            if 'common' in self.get_compute()['envs'] and isinstance(self.get_compute()['envs']['common'], dict):
                platform_envs = True
            if platform_envs:
                result.update(self.get_compute()['envs']['common'])
                if self.get_os() in self.get_compute()['envs']:
                    result.update(self.get_compute()['envs'][self.get_os()])
            else:
                result.update(self.get_compute()['envs'])
        return result

    @staticmethod
    def build_arguments(arguments, escaped_quotes=True, join=False):
        """
        Convert arguments, if *escaped_quotes* is true url encoded quotes is
        supported in *arguments* to allow whitespace in arguments, using %22 notation.

        Example:

        -r arnold -rl %22layer1 layer2%22 -v

        %5C is also substituted with backlash (\\) and #20 is substituted with space ( )

        """
        if arguments is None:
            return []
        if not isinstance(arguments, list):
            arguments = (arguments or "").replace("%5C", "\\")
            if "%22" in arguments and escaped_quotes:
                result = []
                # Preprocess escaped quoted
                for index, part in enumerate(arguments.split("%22")):
                    if (index % 2) == 0:
                        result.extend([s for s in part.split(" ") if 0 < len(s.strip())])  # Normal arg
                    else:
                        result.append(part)  # An arg with whitespaces
            else:
                result = arguments.split(" ")
        else:
            result = arguments
        result = [argument.replace("%5C", "\\").replace("%22", '"').replace("%20", " ")
                  for argument in result if argument is not None]
        result = [argument for argument in result if 0 < len(argument.strip())]
        if join:
            return " ".join(result)
        else:
            return result

    @staticmethod
    def substitute(s, mapping):
        """ Substitute ${KEY} with VALUE for items in *mapping* + environment variables."""
        if not mapping:
            mapping = {}
        mapping.update(os.environ.items())
        for key, value in list(mapping.items()):
            s = s.replace("${%s}" % key, value)
        return s

    def task_started(self, uri):
        """A task has been started within a bucket"""
        self.info("Task started: {}".format(uri))
        if self._current_task is not None and self._current_task != uri:
            # Current task is done
            print("""{"taskstatus":true,"uri":"%s","status":"done"}""" % (self._current_task))
        self._current_task = uri

    def task_done(self, uri):
        """A task has been completed"""
        self.info("Task done: {}".format(uri))
        if self._current_task is None or self._current_task != uri:
            # Current task is done
            print("""{"taskstatus":true,"uri":"%s","status":"done"}""" % (uri))
        self._current_task = uri

    @staticmethod
    def scan_file_sequences(root_folder):
        """
        Scan a folder recursively for file sequences and return them grouped by pattern.
        
        Args:
            root_folder (str): Root folder path to scan
            
        Returns:
            list: List of dictionaries with sequence information:
                [{"subdir":"layer1", "filename":"SC0120_CAM_0070.PathTraced", "frames":[1517,1518,..], "ext":"exr"}]
        """
        if not os.path.exists(root_folder):
            Common.warning(f"Folder does not exist: {root_folder}")
            return []
        
        sequences = {}  # key: (subdir, filename, ext), value: set of frame numbers
        
        # Walk through all subdirectories
        for root, dirs, files in os.walk(root_folder):
            # Calculate relative subdirectory path
            subdir = os.path.relpath(root, root_folder)
            if subdir == ".":
                subdir = ""
            
            for file in files:
                # Skip hidden files and files without extensions
                if file.startswith('.'):
                    continue
                
                # Split filename into parts
                parts = file.split('.')
                if len(parts) < 3:  # Need at least: prefix.number.ext
                    continue
                
                # Extract extension (last part)
                ext = parts[-1]
                
                # Try to find frame number (second to last part should be numeric)
                try:
                    frame_num = int(parts[-2])
                except (ValueError, IndexError):
                    continue  # Skip if frame number is not numeric
                
                # Reconstruct filename prefix (everything except frame number and extension)
                filename_prefix = '.'.join(parts[:-2])
                
                # Create sequence key
                seq_key = (subdir, filename_prefix, ext)
                
                # Add to sequences dictionary
                if seq_key not in sequences:
                    sequences[seq_key] = set()
                sequences[seq_key].add(frame_num)
        
        # Convert to list format
        result = []
        for (subdir, filename, ext), frames in sequences.items():
            result.append({
                "subdir": subdir,
                "filename": filename,
                "frames": sorted(list(frames)),
                "ext": ext
            })
        
        # Sort by subdirectory and filename for consistent ordering
        result.sort(key=lambda x: (x["subdir"], x["filename"]))
        
        return result

    @staticmethod
    def parse_number(fragment):
        """Try to get last number from a string fragment, for example:

        /pat_sc045_2165_lighting_v0003_Canyon.1009.exr'
        """
        result = -1
        number = None
        try:
            for c in reversed(fragment or ""):
                if c in ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"]:
                    if number is None:
                        number = ""
                    number = "{}{}".format(c, number)
                elif number is not None:
                    # Wrap
                    result = int(number)
                    break
        except:
            print(traceback.format_exc())
        return result
    
    def _run_background_worker(self):
        """ Run the background worker thread """
        Common.info("Background worker thread started")
        (interval, func) = self.get_background_worker()
        while self.executing:
            had_error = False
            try:
                func()
            except Exception as e:
                had_error = True
                Common.warning(traceback.format_exc())
                Common.warning(f"Error when running background worker: {e}")
            time.sleep(interval * (10 if had_error else 0))
        Common.info("Background worker thread finished")

    def execute(self):
        """Run computation/render"""
        self.prepare()
        self.pre()
        exitcode = -1
        background_worker = self.get_background_worker()
        self.executing = True
        try:
            if background_worker is not None:
                self._background_worker_thread = threading.Thread(target=self._run_background_worker)
                self._background_worker_thread.start()
            try:
                if "max_bucketsize" in self.SETTINGS and self.SETTINGS["max_bucketsize"] == 1 and self.item.find("-") > 0:
                    Common.info("Renderer can only execute one item at a time.")
                    # Render a batch of items, one by one
                    first = int(self.item.split("-")[0])
                    last = int(self.item.split("-")[-1])
                    for item in range(first, last + 1):
                        # Tell accsyn previous task is done
                        self.task_started(str(item))
                        Common.info("*" * 100)
                        Common.info("Batch executing item {} of [{}-{}]".format(item, first, last))
                        exitcode = self._execute(item, additional_envs={"ACCSYN_ITEM": str(item)})
                else:
                    exitcode = self._execute(self.item)
            finally:
                self.post(exitcode)
        finally:
            self.executing = False
            if self._background_worker_thread:
                self._background_worker_thread.join()
                self._background_worker_thread = None

    def _execute(self, item, additional_envs=None):
        """Internal execution function, can be overridden in case engine script does its own execution handling instead
        of calling a subprocess."""
        commands = self.get_commandline(item)
        if commands is None or len(commands) == 0:
            raise Exception("Empty command line!")
        app_envs = self.get_common_envs()
        if len(app_envs) == 0:
            app_envs = None
        log = True
        exitcode = None     
        try:
            new_envs = None
            if app_envs or additional_envs:
                new_envs = {}
                for k, v in os.environ.items():
                    new_envs[str(Common.safely_printable(k))] = str(Common.safely_printable(v))
                if app_envs:
                    for k, v in app_envs.items():
                        new_envs[str(Common.safely_printable(k))] = str(Common.safely_printable(v))
                if additional_envs:
                    for k, v in additional_envs.items():
                        new_envs[str(Common.safely_printable(k))] = str(Common.safely_printable(v))
            stdin = self.get_stdin(item)
            if new_envs:
                Common.info("Environment variables: '{0}'".format(new_envs))
            creationflags = self.get_creation_flags(item)
            working_path = self.get_working_path()
            if working_path is not None:
                Common.info("Changing directory to: {}".format(working_path))
                os.chdir(working_path)
            shell = self.shell()
            Common.info("Running: '{0}' (shell={1})".format(str([Common.safely_printable(s) for s in commands]), shell))
            if stdin:
                Common.info("Stdin: '{0}".format(stdin))
            if creationflags:
                Common.info("Creation flags: '{0}".format(creationflags))
            Common.info("-" * 120)

            first_run = True
            if stdin:
                if creationflags is not None:
                    self.process = subprocess.Popen(
                        commands,
                        shell=shell,
                        stdin=subprocess.PIPE,
                        env=new_envs,
                        creationflags=creationflags,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        cwd=working_path
                    )
                else:
                    self.process = subprocess.Popen(
                        commands,
                        shell=shell,
                        stdin=subprocess.PIPE,
                        env=new_envs,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        cwd=working_path
                    )

            else:
                if creationflags is not None:
                    self.process = subprocess.Popen(
                        commands,
                        shell=shell,
                        env=new_envs,
                        creationflags=creationflags,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        cwd=working_path
                    )
                else:
                    self.process = subprocess.Popen(
                        commands,
                        shell=shell,
                        env=new_envs,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        cwd=working_path
                    )
            # Create queues for stdout and stderr
            stdout_queue = queue.Queue()
            stderr_queue = queue.Queue()
            
            def read_stdout():
                Common.info("stdout thread spawned")
                try:
                    while True:
                        line = self.process.stdout.readline()
                        if line:
                            stdout_queue.put(line)
                        else:
                            break
                except:
                    pass
                finally:
                    stdout_queue.put(None)  # Signal end
                Common.info("stdout thread finished")
            
            def read_stderr():
                Common.info("stderr thread spawned")
                try:
                    while True:
                        line = self.process.stderr.readline()
                        if line:
                            stderr_queue.put(line)
                        else:
                            break
                except:
                    pass
                finally:
                    stderr_queue.put(None)  # Signal end
                Common.info("stderr thread finished")
            
            # Start threads for reading stdout and stderr
            stdout_thread = threading.Thread(target=read_stdout)
            stderr_thread = threading.Thread(target=read_stderr)
            stdout_thread.daemon = True
            stderr_thread.daemon = True
            stdout_thread.start()
            stderr_thread.start()
            
            stdout_ended = False
            stderr_ended = False
            
            exitcode = -999999

            while not (stdout_ended and stderr_ended):
                if first_run:
                    Common.info("Additional accsyn PID({0}) - main process".format(self.process.pid))
                    if stdin:
                        self.process.stdin.write(stdin)
                        self.process.stdin.close()
                    first_run = False

                # Check for stdout data
                try:
                    while not stdout_ended and not stdout_queue.empty():
                        line = stdout_queue.get(timeout=0.1)
                        if line is None:
                            stdout_ended = True
                        else:
                            stdout = Common.safely_printable(line)

                            if Common.do_print(False):
                                Common.log(stdout, end="", aggregated_flush=True)
                            else:
                                Common.info(stdout, end="", aggregated_flush=True)
        
                            process_result = self.process_output(stdout, "")
                            if process_result is not None:
                                Common.warning("Pre-emptive terminating process (pid: {0}).".format(self.process.pid))
                                exitcode = process_result
                                break
                except queue.Empty:
                    pass
                except:
                    Common.warning(traceback.format_exc())
                
                # Check for stderr data
                try:
                    while not stderr_ended and not stderr_queue.empty():
                        line = stderr_queue.get(timeout=0.1)
                        if line is None:
                            stderr_ended = True
                        else:
                            stderr = Common.safely_printable(line)
                            if Common.do_print(True):
                                Common.log_stderr(stderr, end="", aggregated_flush=True)
                            else:
                                Common.warning(stderr, append_prefix=False, end="", aggregated_flush=True)
        
                            # Also pass stderr to process_output for compatibility
                            process_result = self.process_output("", stderr)
                            if process_result is not None:
                                Common.warning("Pre-emptive terminating process (pid: {0}).".format(self.process.pid))
                                exitcode = process_result
                                break
                except queue.Empty:
                    pass
                except:
                    Common.warning(traceback.format_exc())
                
                # Check if process has ended
                exitcode = self.process.poll()
                if exitcode is not None and stdout_ended and stderr_ended:
                    Common.info("Process has ended, and all output has been captured.")
                    break
            
            # Wait for threads to finish
            Common.info("Waiting for stdout and stderr threads to finish...")
            stdout_thread.join(timeout=1.0)
            stderr_thread.join(timeout=1.0)
            
            # Make sure process is finished
            Common.info("Waiting for process to finish...")
            self.process.wait()
            if exitcode is None:
                exitcode = self.process.returncode

        finally:
            try:
                self.process.terminate()
            except:
                pass
 
        self.process = None

        if self.exitcode_force is not None:
            exitcode = self.exitcode_force
            Common.info("Exitcode (forced): {0}".format(exitcode))
        else:
            Common.info("Exitcode: {0}".format(exitcode))

        # Check exitcode
        if exitcode != 0 and exitcode in self.get_allowed_exitcodes():
            Common.warning("Interpreting exitcode {0} as ok(0)!".format(exitcode))
            exitcode = 0

        assert exitcode == 0, "Execution failed, check log for clues..."

        return exitcode


    @staticmethod
    def recursive_kill_windows_pid(pid):
        output = subprocess.check_output(
            "wmic process where (ParentProcessId={0}) get ProcessId".format(pid), shell=True
        )
        if 0 < len(output or ""):
            for line in output.decode("utf-8").split("\n"):
                try:
                    Common.recursive_kill_windows_pid(int(line.strip()))
                except:
                    pass
        print("os.system('WMIC PROCESS WHERE processid={0} CALL Terminate')".format(pid))

    def kill(self):
        """Kill the current running PID"""
        if not self.executing or self.process is None:
            Common.warning("Refusing terminate - not running or have no process info!")
            return
        Common.warning("Terminating PID: {0}".format(self.process.pid))
        if Common.is_win():
            Common.recursive_kill_windows_pid(self.process.pid)
            # os.system('TASKKILL /f /PID {0}'.format(self.process.pid))
        else:
            os.killpg(self.process.pid, signal.SIGKILL)
            # os.system('kill -9 {0}'.format(self.process.pid))

    def get_allowed_exitcodes(self):
        return [0]

    def __del__(self):
        """ Flush stdout and stderr on exit """
        sys.stdout.flush()
        sys.stderr.flush()

    # UTILS

    @staticmethod
    def str_file_size(num, suffix='B', si=True):
        if num is None:
            return None
        elif num == -1:
            return "-"
        divider = 1000.0 if si else 1024.0
        for unit in ['', 'K', 'M', 'G', 'T', 'P', 'E', 'Z']:
            if abs(num) < divider:
                return "%3.1f%s%s" % (num, "%s%s" % (unit, "i" if not si else ""), suffix)
            num /= divider
        return f"{num:.1f}Y{suffix}"

class JSONEncoder(json.JSONEncoder):
    @staticmethod
    def encode_accsyn_json(obj):
        if isinstance(obj, datetime.date) or isinstance(obj, datetime.datetime):
            # Make UTC ISO E8601DTw.d: 2008-09-15T15:53:00
            return obj.strftime("%Y-%m-%dT%H:%M:%S")
        elif isinstance(obj, bytes):
            return obj.decode('utf-8')

    def default(self, obj):
        result = self.encode_accsyn_json(obj)
        if result:
            return result
        return super(JSONEncoder, self).default(obj)

class JSONDecoder(json.JSONDecoder):
    def decode(self, json_string):
        json_data = json.loads(json_string)

        def recursive_decode(d):
            if isinstance(d, dict):
                for key in list(d.keys()):
                    if isinstance(d[key], dict):
                        d[key] = recursive_decode(d[key])
                    elif isinstance(d[key], list):
                        newlist = []
                        for i in d[key]:
                            newlist.append(recursive_decode(i))
                        d[key] = newlist
                    elif isinstance(d[key], str):
                        if re.match(
                            r"^\d{2,4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3,6}$",
                            str(d[key]),
                        ):  # Date with millis: 2024-07-22T12:22:18.527675
                            if len(d[key].split("-")[0]) == 4:
                                d[key] = datetime.datetime.strptime(d[key], "%Y-%m-%dT%H:%M:%S.%f")
                            else:
                                d[key] = datetime.datetime.strptime(d[key], "%y-%m-%dT%H:%M:%S.%f")
            return d

        return recursive_decode(json_data)