'''

    V-Ray Next accsyn compute app script.

    Finds and executes app by building a commandline out of 'item'(frame number)
    and parameters provided.

    Changelog:

        v1r1; Initial version

    This software is provided "as is" - the author and distributor can not be held
    responsible for any damage caused by executing this script in any means.

    Author: Henrik Norin, HDR AB

'''

import os
import sys
import logging
import traceback
import json
import time
import datetime

try:
    if 'ACCSYN_COMPUTE_COMMON_PATH' in os.environ:
        sys.path.append(os.environ['ACCSYN_COMPUTE_COMMON_PATH'])
    from common import Common
except ImportError as e:
    sys.stderr.write(
        'Cannot import accsyn common app (required), '
        'make sure to name it "common.py" add its parent directory to '
        ' PYTHONPATH. Details: %s\n' % e
    )
    raise


class App(Common):

    __revision__ = 1  # Increment this after each update

    # App configuration
    # IMPORTANT NOTE:
    # This section defines app behaviour and should not be refactored or moved
    # away from the enclosing START/END markers. Read into memory by cloud at
    # start and publish.
    # -- APP CONFIG START --

    SETTINGS = {
        "items": True,
        "default_range": "1001-1100",
        "default_bucketsize": 1,
        "filename_extensions": ".ma,.mb",
        "binary_filename_extensions": ".mb",
    }

    PARAMETERS = {"project": "", "arguments": "-r vray", "input_conversion": "always"}

    ENVS = {}

    VRAY_VERSION = "5.20.01"

    # Have exact V-ray version validated
    CHECK_VRAY_VERSION = True
    LIB_REF_SIZES = [45071872, 45083136]

    # -- APP CONFIG END --

    def __init__(self, argv):
        super(App, self).__init__(argv)

    @staticmethod
    def get_path_version_name():
        p = os.path.realpath(__file__)
        parent = os.path.dirname(p)
        return (os.path.dirname(parent), os.path.basename(parent), os.path.splitext(os.path.basename(p))[0])

    @staticmethod
    def usage():
        (unused_cp, cv, cn) = Common.get_path_version_name()
        (unused_p, v, n) = App.get_path_version_name()
        Common.info(
            '   accsyn compute app "%s" v%s-%s(common: v%s-%s) ' % (n, v, App.__revision__, cv, Common.__revision__)
        )
        Common.info('')
        Common.info('   Usage: python %s {--probe|<path_json_data>}' % n)
        Common.info('')
        Common.info('       --probe           Have app check if it is found and' ' of correct version.')
        Common.info('')
        Common.info(
            '       <path_json_data>  Execute app on data provided in '
            'the JSON and ACCSYN_xxx environment variables.'
        )
        Common.info('')

    def probe(self):
        '''(Optional) Do nothing if found, raise execption otherwise.'''
        exe = self.get_executable()
        assert os.path.exists(exe), "'%s' does not exist!" % exe
        # TODO, check if correct version
        return True

    def get_executable(self):
        '''(REQUIRED) Return path to executable as string'''
        if Common.is_lin():
            return "/usr/autodesk/maya2022/bin/Render"
        elif Common.is_mac():
            return "/Applications/Autodesk/maya2022/Maya.app/Contents/bin/Render"
        elif Common.is_win():
            return "C:\\Program Files\\Autodesk\\Maya2022\\bin\\Render.exe"

    def get_envs(self):
        '''Return site specific envs here'''
        result = {}
        return result

    def get_commandline(self, item):
        '''(REQUIRED) Return command line as a string array'''
        # Check if correct version of V-ray - verify size of main library
        path_vray_dll = None
        if Common.is_lin():
            pass  # No version check here, defined by envs above
        elif Common.is_mac():
            raise Exception("V-ray for Maya not supported on Mac yet!")
        elif Common.is_win():
            path_vray_dll = "C:\\Program Files\\Autodesk\\Maya2020\\vray\\bin\\vray.dll"
            assert os.path.exists(path_vray_dll), (
                'V-ray for Maya 2020 not ' 'properly installed! (missing: "%s")' % path_vray_dll
            )
            if App.CHECK_VRAY_VERSION:
                dll_size_mismatch = -1
                for dll_size_match in App.LIB_REF_SIZES:
                    dll_size = os.path.getsize(path_vray_dll)
                    if dll_size == dll_size_match:
                        dll_size_mismatch = None
                        break
                    else:
                        dll_size_mismatch = dll_size
                assert (
                    dll_size_mismatch is None
                ), "V-ray for Maya {} is not the correct installed version on this node (DLL size:{})!".format(
                    App.VRAY_VERSION, os.path.getsize(dll_size_mismatch)
                )

        args = []
        if "parameters" in self.get_compute():
            parameters = self.get_compute()["parameters"]
            if 0 < len(parameters.get("arguments") or ""):
                arguments = parameters["arguments"]
                if 0 < len(arguments):
                    args.extend(arguments.split(" "))
            if "project" in parameters and 0 < len(parameters["project"]):
                args.extend(["-proj", self.normalize_path(parameters["project"])])
            if "renderlayer" in parameters:
                args.extend(["-rl", parameters["renderlayer"]])
        if self.item and self.item != "all":
            # Add range
            start = end = self.item
            if -1 < self.item.find("-"):
                parts = self.item.split("-")
                start = parts[0]
                end = parts[1]
            args.extend(["-s", str(start), "-e", str(end)])
        if "output" in self.get_compute()["compute"]:
            # Output has already been converted to local platform
            args.extend(["-rd", self.get_compute()["compute"]["output"]])
        # Input has already been converted to local platform
        p_input = self.normalize_path(self.get_compute()["input"])
        args.extend([p_input])
        if Common.is_lin():
            retval = [self.get_executable()]
            retval.extend(args)
            return retval
        elif Common.is_mac():
            retval = [self.get_executable()]
            retval.extend(args)
            return retval
        elif Common.is_win():
            retval = [self.get_executable()]
            retval.extend(args)
            return retval

        raise Exception('This operating system is not recognized by this accsyn' ' app!')

    def get_creation_flags(self, item):
        '''Always run on low priority on windows'''
        if Common.is_win():

            ABOVE_NORMAL_PRIORITY_CLASS = 0x00008000
            BELOW_NORMAL_PRIORITY_CLASS = 0x00004000
            HIGH_PRIORITY_CLASS = 0x00000080
            IDLE_PRIORITY_CLASS = 0x00000040
            NORMAL_PRIORITY_CLASS = 0x00000020
            REALTIME_PRIORITY_CLASS = 0x00000100

            return NORMAL_PRIORITY_CLASS


if __name__ == "__main__":
    if "--help" in sys.argv:
        App.usage()
    else:
        # Common.set_debug(True)
        try:
            app = App(sys.argv)
            if "--probe" in sys.argv:
                app.probe()
            else:
                app.load()  # Load data
                app.execute()  # Run
        except:
            App.warning(traceback.format_exc())
            App.usage()
            sys.exit(1)
