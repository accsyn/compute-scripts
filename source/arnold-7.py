'''

	Arnold Standalone (Maya 2023) compute app script.

	Finds and executes Arnold by building a commandline out of 'item'(frame number)
	and parameters provided.

	Changelog:

        v1r2; (Henrik, 22.11.11) Url encoded arguments support.
		v1r1; Initial version

	This software is provided "as is" - the author and distributor can not be held
	responsible for any damage caused by executing this script in any means.

	Author: Henrik Norin, HDR AB

'''

import os, sys, logging, traceback

try:
    if 'ACCSYN_COMPUTE_COMMON_PATH' in os.environ:
        sys.path.append(os.environ['ACCSYN_COMPUTE_COMMON_PATH'])
    from common import Common
except ImportError as e:
    print >> sys.stderr, "Cannot import accsyn common app (required), make sure to name it 'common.py' add its parent directory to PYTHONPATH. Details: %s" % e
    raise


class App(Common):
    __revision__ = 2  # Increment this after each update

    # App configuration
    # IMPORTANT NOTE: This section defines app behaviour and should not be refactored or moved away from the enclosing START/END markers. Read into memory by backend at start and publish.
    # --- Start edit here
    # -- APP CONFIG START --

    SETTINGS = {
        "items": True,
        "default_range": "1001-1100",
        "default_bucketsize": 1,
        "max_bucketsize": 1,
        "filename_extensions": ".ass",
        "output_readonly": True,
        "color": "160,255,180",
        "vendor": "Autodesk"
    }

    PARAMETERS = {"arguments": "-dw -nstdin -v 2"}

    ENVS = {}

    # -- APP CONFIG END --
    # -- Stop edit here

    def __init__(self, argv):
        super(App, self).__init__(argv)

    @staticmethod
    def get_path_version_name():
        '''Don't touch this'''
        p = os.path.realpath(__file__)
        parent = os.path.dirname(p)
        return (os.path.dirname(parent), os.path.basename(parent), os.path.splitext(os.path.basename(p))[0])

    @staticmethod
    def usage():
        '''Don't touch this'''
        (unused_cp, cv, cn) = Common.get_path_version_name()
        (unused_p, v, n) = App.get_path_version_name()
        Common.info(
            "   accsyn compute app '%s' v%s-%s(common: v%s-%s) " % (n, v, App.__revision__, cv, Common.__revision__)
        )
        Common.info("")
        Common.info("   Usage: python %s {--probe|<path_json_data>}" % n)
        Common.info("")
        Common.info("       --help            Show this help.")
        Common.info("       --dev             Development mode, debug is on and launch dummy executable.")
        Common.info("")
        Common.info("       --probe           Have app check if it is found and of correct version.")
        Common.info("")
        Common.info(
            "       <path_json_data>  Execute app on data provided in the JSON and ACCSYN_xxx environment variables."
        )
        Common.info("")

    def probe(self):
        '''(Optional) Do nothing if found, raise exception otherwise.'''
        exe = self.get_executable()
        assert os.path.exists(exe), "'{}' does not exist!".format(exe)
        # TODO, check if correct versions of dependencies
        return True

    #
    # --- Start edit here

    def get_executable(self):
        '''(REQUIRED) Return path to executable as string'''
        if not Common._dev:
            if Common.is_lin():
                return "/usr/autodesk/arnold/maya2023/bin/kick"
            elif Common.is_mac():
                return "/Applications/Autodesk/Arnold/maya2023/bin/kick"
            elif Common.is_win():
                return "C:\\Program Files\\Autodesk\\Arnold\\maya2023\\bin\\kick.exe"
        else:
            if Common.is_lin():
                raise Exception("Arnold dev render app not supported on Linux yet!")
            elif Common.is_mac():
                raise Exception("Arnold dev render app not supported on Mac yet!")
            elif Common.is_win():
                raise Exception("Arnold dev render app not supported on Windows yet!")

    def get_envs(self):
        '''Get site specific envs'''
        result = {}
        return result

    def get_commandline(self, item):
        '''
        (REQUIRED) Return command line as a string array

        Example: "C:\\Program Files\\Autodesk\\Arnold\\maya2023\\bin\\kick.exe" -i J:\\Pat\\_RESOURCES\\ArnoldRenderTest\\AssFiles_5minRender\\5minTest.0001.ass -o imagename -of jpg -dw
        '''
        args = []

        # Input
        path_input = self.data['compute']['input']
        path_input = path_input % (int(item))
        args.extend(["-i", self.normalize_path(path_input)])

        if 'parameters' in self.get_compute():
            parameters = self.get_compute()['parameters']
            if 'arguments' in parameters:
                args.extend(Common.build_arguments(parameters['arguments']))

        if Common.is_win() or True:
            # retval = ["CMD", "/C", self.get_executable()]
            retval = [self.get_executable()]
        else:
            retval = ["/bin/bash", "-c", self.get_executable()]
        retval.extend(args)
        return retval

        raise Exception("This OS is not recognized by this Accsyn app!")


# -- Stop edit here

if __name__ == '__main__':
    if "--help" in sys.argv:
        App.usage()
    else:
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
