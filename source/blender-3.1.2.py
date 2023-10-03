'''

    Blender accsyn compute app script.

    Finds and executes app by building a commandline out of 'item'(frame number)
    and parameters provided.

    Changelog:

        v1r2; (Henrik, 22.11.11) Url encoded arguments support.
        v1r1; Initial version.

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
    # away from the enclosing START/END markers. Read into memory by backend at
    # start and publish.
    # -- APP CONFIG START --

    SETTINGS = {
        "items": True,
        "default_range": "1001-1100",
        "default_bucketsize": 1,
        "filename_extensions": ".blend",
        "binary_filename_extensions": ".blend",
    }

    PARAMETERS = {"arguments": "-b", "input_conversion": "always"}

    ENVS = {}

    # -- APP CONFIG END --

    # The installed Blender version
    BLENDER_VERSION = "3.1.2"

    # Do not launch if installed blender version does not matches version above
    CHECK_BLENDER_VERSION = True

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
        '''(Optional) Do nothing if found, raise exception otherwise.'''
        exe = self.get_executable()
        assert os.path.exists(exe), "'{}' does not exist!".format(exe)
        # TODO, check if correct version
        return True

    def get_executable(self):
        '''(REQUIRED) Return path to executable as string'''
        if Common.is_lin():
            return "/usr/local/blender/bin/Blender"
        elif Common.is_mac():
            return "/Applications/Blender.app/Contents/MacOS/Blender"
        elif Common.is_win():
            return "C:\\Program Files\\Blender Foundation\\Blender 3.1.2\\Blender.exe"

    def get_envs(self):
        '''Return site specific envs here'''
        result = {}
        return result

    def get_commandline(self, item):
        '''(REQUIRED) Return command line as a string array'''
        # Check if correct version of V-ray - verify size of main library
        path_executable = self.get_executable()
        if App.CHECK_BLENDER_VERSION:
            if Common.is_lin():
                if not os.path.exists(path_executable):
                    raise Exception(
                        "Blender {} not found or is not the correct installed version on this Linux station!".format(
                            App.BLENDER_VERSION
                        )
                    )
            elif Common.is_mac():
                path_info_plist = os.path.join(os.path.dirname(os.path.dirname(path_executable)), 'Info.plist')
                Common.info(
                    'Checking for Blender version string "{}" within {}'.format(App.BLENDER_VERSION, path_info_plist)
                )
                if (
                    not os.path.exists(path_info_plist)
                    or (open(path_info_plist, 'r').read()).find(App.BLENDER_VERSION) == -1
                ):
                    raise Exception(
                        "Blender {} not found or is not the correct installed version on this Mac!".format(
                            App.BLENDER_VERSION
                        )
                    )
            elif Common.is_win():
                if not os.path.exists(path_executable):
                    raise Exception(
                        "Blender {} not found or is not the correct installed version on this PC!".format(
                            App.BLENDER_VERSION
                        )
                    )

        args = ['-b']
        # Input has already been converted to local platform
        p_input = self.normalize_path(self.get_compute()["input"])
        args.append(p_input)
        if self.item and self.item != "all":
            # Add range
            if -1 < self.item.find("-"):
                parts = self.item.split("-")
                start = parts[0]
                end = parts[1]
                args.extend(["-s", str(start), "-e", str(end), "-a"])
            else:
                args.extend(["-f", str(self.item)])
        if "output" in self.get_compute():
            # Output has already been converted to local platform
            args.extend(["-o", self.get_compute()["output"]])
        if "parameters" in self.get_compute():
            parameters = self.get_compute()["parameters"]
            if 0 < len(parameters.get("arguments") or ""):
                args.extend(Common.build_arguments(parameters['arguments']))
        if Common.is_lin():
            retval = [path_executable]
            retval.extend(args)
            return retval
        elif Common.is_mac():
            retval = [path_executable]
            retval.extend(args)
            return retval
        elif Common.is_win():
            retval = [path_executable]
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
