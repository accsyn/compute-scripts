"""

    accsyn server cluster script.

    Finds and executes file server daemon app, with environment set from task.

    Changelog:

        * v1r4; (Henrik, 24.11.01) Ident directory to empty file
        * v1r3; (Henrik, 24.09.11) Create store directory
        * v1r2; (Henrik, 24.09.11) Create store directory
        * v1r1; (Henrik, 24.08.20) Initial version

    This software is provided "as is" - the author and distributor can not be held
    responsible for any damage caused by executing this script in any means.

    Author: Henrik Norin, HDR AB

"""
import os
import sys
import traceback
import time


try:
    if 'ACCSYN_COMPUTE_COMMON_PATH' in os.environ:
        sys.path.append(os.environ['ACCSYN_COMPUTE_COMMON_PATH'])
    from common import Common
except ImportError as e:
    sys.stderr.write(
        'Cannot import accsyn common engine (required), '
        'make sure to name it "common.py" add its parent directory to '
        ' PYTHONPATH. Details: {}\n'.format(e)
    )
    raise


class Engine(Common):
    __revision__ = 4  # Increment this after each update

    # Engine configuration
    # IMPORTANT NOTE:
    #   This section defines engine behaviour and should not be refactored or moved
    #   away from the enclosing START/END markers. Read into memory by backend at
    #   launch and publish of new engine.
    # -- ENGINE CONFIG START --

    SETTINGS = {
        "items": False,
        "multiple_inputs": True,
        "filename_extensions": "",
        "binary": True,
        "type": "hosting",
        "color": "36,65,85",
        "vendor": "accsyn.com"
    }

    PARAMETERS = {"arguments": "daemon", "input_conversion": "never"}

    ENVS = {}

    # -- ENGINE CONFIG END --

    def __init__(self, argv):
        super(Engine, self).__init__(argv)
        as_server_root_path = os.environ.get('AS_ROOT_PATH')
        assert as_server_root_path, 'AS_ROOT_PATH not set!'
        if not os.path.exists(as_server_root_path):
            os.makedirs(as_server_root_path, exist_ok=True)
            Common.info(f"Created server path: {as_server_root_path}")
        as_hosting_root_path = os.path.dirname(as_server_root_path)
        storage_path = os.path.join(as_hosting_root_path, 'storage')
        if not os.path.exists(storage_path):
            os.makedirs(storage_path)
            Common.info(f"Created storage directory: {storage_path}")
            workspace_code = os.environ.get('AS_WORKSPACE')
            if workspace_code:
                # Help identify workspace
                ident_path = os.path.join(as_hosting_root_path, workspace_code)
                if not os.path.exists(ident_path):
                    with open(ident_path, 'w') as f:
                        f.write('')
                    Common.info(f"Created workspace ident file: {ident_path}")

    @staticmethod
    def get_path_version_name():
        p = os.path.realpath(__file__)
        parent = os.path.dirname(p)
        return os.path.dirname(parent), os.path.basename(parent), os.path.splitext(os.path.basename(p))[0]

    @staticmethod
    def usage():
        (unused_cp, cv, cn) = Common.get_path_version_name()
        (unused_p, v, n) = Engine.get_path_version_name()
        Common.info(
            '   accsyn compute engine "{}" v{}-{}(common: v{}-{}) '.format(n, v, Engine.__revision__, cv,
                                                                           Common.__revision__)
        )
        Common.info('')
        Common.info('   Usage: python %s {--probe|<path_json_data>}' % n)
        Common.info('')
        Common.info('       --probe           Have engine check if it is found and' ' of correct version.')
        Common.info('')
        Common.info(
            '       <path_json_data>  Execute engine on data provided in '
            'the JSON and ACCSYN_xxx environment variables.'
        )
        Common.info('')

    def probe(self):
        """(Optional) Do nothing if found, raise exception otherwise."""
        exe = self.get_executable()
        assert os.path.exists(exe), "'{}' does not exist!".format(exe)
        # TODO, check if correct version
        return True

    def get_executable(self):
        """(REQUIRED) Return path to executable as string"""
        if Common.is_lin():
            result = "/usr/local/accsyn/accsyn"
            return result
        elif Common.is_mac():
            raise Exception("Engine not available on Mac")
        elif Common.is_win():
            raise Exception("Engine not available on Windows")

    def get_envs(self):
        """Return site specific envs here"""
        result = {}
        return result

    def get_commandline(self, item):
        """(REQUIRED) Return command line as a string array"""
        args = []
        if "parameters" not in self.get_compute():
            raise Exception("No parameters for engine")

        parameters = self.get_compute()["parameters"]

        if "arguments" not in parameters:
            raise Exception("No arguments for engine")

        arguments = str(parameters["arguments"])

        args.extend(Common.build_arguments(arguments, join=False))

        args.extend([f"--hostingIdent={os.environ.get('AS_WORKSPACE') or '-'}"])

        Common.log("Running accsyn daemon, args: ".format(args))

        if Common.is_lin():
            retval = [self.get_executable()]
            retval.extend(args)
            return retval

        raise Exception('This operating system is not recognized by this accsyn engine!')

    def process_output(self, stdout, stderr):
        # TODO: Parse progress and return
        # {"p":24.41037257081048,"a":1.1420566468984208,"s":0.0,"c":1.3324628267027712,"download: ":true,"t":"0",
        # "e":"04m 38s","f":"A001_C011_09187Ia.mov","metrics":true,"tp":97}
        pass


if __name__ == "__main__":
    if "--help" in sys.argv:
        Engine.usage()
    else:
        # Common.set_debug(True)
        try:
            engine = Engine(sys.argv)
            if "--probe" in sys.argv:
                engine.probe()
            else:
                engine.load()  # Load data
                engine.execute()  # Run
        except:
            print(traceback.format_exc())
            Engine.usage()
            time.sleep(2)
            sys.exit(1)
