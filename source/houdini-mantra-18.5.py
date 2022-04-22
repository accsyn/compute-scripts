'''

	Houdini 18.5 accsyn compute app script.

	Finds and executes Houdini by building a commandline out of 'item'(frame number)
	and parameters provided.

	Changelog:

		v1r2; Wrong call to debug.
		v1r1; Compliance to accsyn v1.4.

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
    __revision__ = 3  # Increment this after each update

    # App configuration
    # IMPORTANT NOTE: This section defines app behaviour and should not be refactored or moved away from the enclosing START/END markers. Read into memory by cloud at start and publish.
    # --- Start edit here
    # -- APP CONFIG START --

    SETTINGS = {
        "items": True,
        "default_range": "1001-1100",
        "default_bucketsize": 1,
        "max_bucketsize": 1,
        "filename_extensions": ".ifd"
    }

    PARAMETERS = {
        "arguments": "-V 2p"
    }

    ENVS = {
    }

    # -- APP CONFIG END --
    # -- Stop edit here

    def __init__(self, argv):
        super(App, self).__init__(argv)

    @staticmethod
    def get_path_version_name():
        ''' Don't touch this '''
        p = os.path.realpath(__file__)
        parent = os.path.dirname(p)
        return (os.path.dirname(parent), os.path.basename(parent), os.path.splitext(os.path.basename(p))[0])

    @staticmethod
    def usage():
        ''' Don't touch this '''
        (unused_cp, cv, cn) = Common.get_path_version_name()
        (unused_p, v, n) = App.get_path_version_name()
        Common.info(
            "   Accsyn compute app '%s' v%s-%s(common: v%s-%s) " % (n, v, App.__revision__, cv, Common.__revision__))
        Common.info("")
        Common.info("   Usage: python %s {--probe|<path_json_data>}" % n)
        Common.info("")
        Common.info("       --help            Show this help.")
        Common.info("       --dev             Development mode, debug is on and launch dummy executable.")
        Common.info("")
        Common.info("       --probe           Have app check if it is found and of correct version.")
        Common.info("")
        Common.info(
            "       <path_json_data>  Execute app on data provided in the JSON and ACCSYN_xxx environment variables.")
        Common.info("")

    def probe(self):
        ''' (Optional) Do nothing if found, raise execption otherwise. '''
        exe = self.get_executable()
        assert (os.path.exists(exe)), ("'%s' does not exist!" % exe)
        # TODO, check if correct versions of dependencies
        return True

    #
    # --- Start edit here

    def get_executable(self):
        ''' (REQUIRED) Return path to executable as string '''
        if not Common._dev:
            if Common.is_lin():
                # return "/opt/hfs18.0.597/bin/mantra"
                return "/library/software/networkinstall/linux/houdini/hfs18.5.563/bin/mantra"
            elif Common.is_mac():
                return "/Applications/Houdini/Houdini18.5.563//Frameworks/Houdini.framework/Versions/18.5/Resources/bin/mantra"
            elif Common.is_win():
                return "C:\\Program Files\\Side Effects Software\\Houdini 18.5.563\\bin\\mantra.exe"
        else:
            if Common.is_lin():
                raise Exception("Houdini dev app not supported on Linux yet!")
            elif Common.is_mac():
                return Exception("Houdini dev app not supported on Mac yet!")
            elif Common.is_win():
                return Exception("Houdini dev app not supported on Windows yet!")

    def get_envs(self):
        ''' Get site specific envs '''
        result = {}
        return result

    def get_commandline(self, item):
        '''
        (REQUIRED) Return command line as a string array
        '''
        args = []
        path_input = self.data['compute']['input']
        path_input = path_input % (int(item))
        if 'parameters' in self.get_compute():
            parameters = self.get_compute()['parameters']
            if 'arguments' in parameters:
                args.extend([parameters['arguments']])
        if 'output' in self.data['compute']:
            path_output = self.normalize_path(self.data['compute']['output'],
                                              mkdirs=True)  # Do this so folder is created
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

        raise Exception("This OS is not recognized by this Accsyn app!")

    def get_creation_flags(self, item):
        ''' Always run on low priority on windows '''
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
