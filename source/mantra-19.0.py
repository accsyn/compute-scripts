"""

    Mantra(Houdini) 19.0 accsyn compute engine script.

    Finds and executes Mantra by building a commandline out of 'item'(frame number)
    and parameters provided.

    Changelog:

        v1r2; (Henrik Norin, 22.11.11) Url encoded arguments support.
        v1r1; Initial version

    This software is provided "as is" - the author and distributor can not be held
    responsible for any damage caused by executing this script in any means.

    Author: Henrik Norin, HDR AB

"""

import os
import sys
import traceback

try:
    if 'ACCSYN_COMPUTE_COMMON_PATH' in os.environ:
        sys.path.append(os.environ['ACCSYN_COMPUTE_COMMON_PATH'])
    from common import Common
except ImportError as e:
    sys.stderr.write("Cannot import accsyn common engine (required), make sure to name it 'common.py' add its "
                     "parent directory to PYTHONPATH. Details: {}".format(e))
    raise


class Engine(Common):
    __revision__ = 3  # Increment this after each update

    # Engine configuration
    # IMPORTANT NOTE: This section defines engine behaviour and should not be refactored or moved away from the
    # enclosing START/END markers. Read into memory by backend at start and publish.
    # --- Start edit here
    # -- ENGINE CONFIG START --

    SETTINGS = {
        "items": True,
        "default_range": "1001-1100",
        "default_bucketsize": 1,
        "max_bucketsize": 1,
        "filename_extensions": ".ifd",
        "color": "229,76,27",
        "vendor": "SideFX Software"
    }

    PARAMETERS = {"arguments": "-V 2p"}

    ENVS = {}

    # -- ENGINE CONFIG END --
    # -- Stop edit here

    def __init__(self, argv):
        super(Engine, self).__init__(argv)

    @staticmethod
    def get_path_version_name():
        """Don't touch this"""
        p = os.path.realpath(__file__)
        parent = os.path.dirname(p)
        return os.path.dirname(parent), os.path.basename(parent), os.path.splitext(os.path.basename(p))[0]

    @staticmethod
    def usage():
        """Don't touch this"""
        (unused_cp, cv, cn) = Common.get_path_version_name()
        (unused_p, v, n) = Engine.get_path_version_name()
        Common.info(
            '   accsyn compute engine "{}" v{}-{}(common: v{}-{})'.format(n, v, Engine.__revision__, cv,
                                                                          Common.__revision__)
        )
        Common.info("")
        Common.info("   Usage: python %s {--probe|<path_json_data>}" % n)
        Common.info("")
        Common.info("       --help            Show this help.")
        Common.info("       --dev             Development mode, debug is on and launch dummy executable.")
        Common.info("")
        Common.info("       --probe           Have engine check if it is found and of correct version.")
        Common.info("")
        Common.info(
            "       <path_json_data>  Execute engine on data provided in the JSON and ACCSYN_xxx environment variables."
        )
        Common.info("")

    def probe(self):
        """(Optional) Do nothing if found, raise exception otherwise."""
        exe = self.get_executable()
        assert os.path.exists(exe), "'{}' does not exist!".format(exe)
        # TODO, check if correct versions of dependencies
        return True

    #
    # --- Start edit here

    def get_executable(self):
        """(REQUIRED) Return path to executable as string"""
        if not Common._dev:
            if Common.is_lin():
                return "/opt/hfs19.0.383/bin/mantra"
            elif Common.is_mac():
                return ("/Applications/Houdini/Houdini19.0.383//Frameworks/Houdini.framework/Versions/18.5/Resources/"
                        "bin/mantra")
            elif Common.is_win():
                return "C:\\Program Files\\Side Effects Software\\Houdini 19.0.383\\bin\\mantra.exe"
        else:
            if Common.is_lin():
                raise Exception("Houdini dev app not supported on Linux yet!")
            elif Common.is_mac():
                raise Exception("Houdini dev app not supported on Mac yet!")
            elif Common.is_win():
                raise Exception("Houdini dev app not supported on Windows yet!")

    def get_envs(self):
        """Get site specific envs"""
        result = {}
        return result

    def get_commandline(self, item):
        """
        (REQUIRED) Return command line as a string array
        """
        args = []
        path_input = self.data['compute']['input']
        path_input = path_input % (int(item))
        if 'parameters' in self.get_compute():
            parameters = self.get_compute()['parameters']
            if 'arguments' in parameters:
                args.extend(Common.build_arguments(parameters['arguments']))
        if 'output' in self.data['compute']:
            path_output = self.normalize_path(
                self.data['compute']['output'], mkdirs=True
            )  # Do this so folder is created
            self.debug("Rendering to '%s'" % path_output)
        args.extend(["-f", self.normalize_path(path_input)])

        if Common.is_win() or True:
            retval = [self.get_executable()]
            retval.extend(args)
            return retval
        else:
            retval = ["/bin/bash", "-c", self.get_executable()]
            retval.extend(args)
            return retval

    def get_creation_flags(self, item):
        """Always run on low priority on windows"""
        if Common.is_win():
            ABOVE_NORMAL_PRIORITY_CLASS = 0x00008000
            BELOW_NORMAL_PRIORITY_CLASS = 0x00004000
            HIGH_PRIORITY_CLASS = 0x00000080
            IDLE_PRIORITY_CLASS = 0x00000040
            NORMAL_PRIORITY_CLASS = 0x00000020
            REALTIME_PRIORITY_CLASS = 0x00000100

            return BELOW_NORMAL_PRIORITY_CLASS


# -- Stop edit here

if __name__ == '__main__':
    if "--help" in sys.argv:
        Engine.usage()
    else:
        try:
            engine = Engine(sys.argv)
            if "--probe" in sys.argv:
                engine.probe()
            else:
                engine.load()  # Load data
                engine.execute()  # Run
        except:
            Engine.warning(traceback.format_exc())
            Engine.usage()
            sys.exit(1)
