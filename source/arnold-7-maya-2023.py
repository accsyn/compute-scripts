"""

    Arnold for Maya accsyn compute engine script.

    Finds and executes app by building a commandline out of 'item'(frame number)
    and parameters provided.

    Changelog:

      * v1r3: (Henrik Norin, 24.11.16) Aligned with v3
      * v1r2; (Henrik, 22.11.11) Url encoded arguments support.
      * v1r1; Initial version

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
    sys.stderr.write(
        'Cannot import accsyn common engine (required), '
        'make sure to name it "common.py" add its parent directory to '
        ' PYTHONPATH. Details: {}\n'.format(e)
    )
    raise


class Engine(Common):
    __revision__ = 3  # Increment this after each update

    # Engine configuration
    # IMPORTANT NOTE:
    # This section defines engine behaviour and should not be refactored or moved
    # away from the enclosing START/END markers. Read into memory by backend at
    # start and publish.
    # -- ENGINE CONFIG START --

    SETTINGS = {
        "items": True,
        "default_range": "1001-1100",
        "default_bucketsize": 1,
        "filename_extensions": ".ma,.mb",
        "binary_filename_extensions": ".mb",
        "color": "160,255,180",
        "vendor": "Autodesk"
    }

    PARAMETERS = {"project": "", "arguments": "-r arnold -ai:lve 2 -ai:alf true", "input_conversion": "always"}

    ENVS = {}

    ARNOLD_VERSION = "7.1.0.0"

    # -- ENGINE CONFIG END --

    def __init__(self, argv):
        super(Engine, self).__init__(argv)

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
            return "/usr/autodesk/maya2023/bin/Render"
        elif Common.is_mac():
            return "/Applications/Autodesk/maya2023/Maya.app/Contents/bin/Render"
        elif Common.is_win():
            return "C:\\Program Files\\Autodesk\\Maya2023\\bin\\Render.exe"

    def get_envs(self):
        """Return site specific envs here"""
        result = {}
        return result

    def get_commandline(self, item):
        """(REQUIRED) Return command line as a string array"""
        args = []
        if "parameters" in self.get_compute():
            parameters = self.get_compute()["parameters"]
            if 0 < len(parameters.get("arguments") or ""):
                args.extend(Common.build_arguments(parameters['arguments']))
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
        if "output" in self.get_compute():
            # Output has already been converted to local platform
            args.extend(["-rd", self.get_compute()["output"]])
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

        raise Exception('This operating system is not recognized by this accsyn engine!')

    def get_creation_flags(self, item):
        """Always run on low priority on windows"""
        if Common.is_win():
            ABOVE_NORMAL_PRIORITY_CLASS = 0x00008000
            BELOW_NORMAL_PRIORITY_CLASS = 0x00004000
            HIGH_PRIORITY_CLASS = 0x00000080
            IDLE_PRIORITY_CLASS = 0x00000040
            NORMAL_PRIORITY_CLASS = 0x00000020
            REALTIME_PRIORITY_CLASS = 0x00000100

            return NORMAL_PRIORITY_CLASS

    def process_output(self, stdout, stderr):
        """(Override)

                Arnold example progress output:

        00:15:54 41777MB         |     0% done - 16 rays/pixel
        00:16:18 47607MB         |     5% done - 11 rays/pixel
        00:17:12 49116MB         |    10% done - 5044 rays/pixel
        00:17:24 49136MB         |    15% done - 1336 rays/pixel
        00:17:24 49137MB         |    20% done - 80 rays/pixel
        00:17:48 49152MB         |    25% done - 2625 rays/pixel
        00:17:54 49158MB WARNING |   found NaN/inf in aiNormalMap283.out.VECTOR when shading object /Dogwood_Sapling_PAT_B/Dogwood_Sapling_PAT_BShape
        00:18:06 49165MB         |    30% done - 2081 rays/pixel
        00:18:20 49171MB         |    35% done - 1537 rays/pixel
        00:18:52 49184MB         |    40% done - 3434 rays/pixel
        00:19:25 49194MB         |    45% done - 3025 rays/pixel
        00:19:36 49197MB         |    50% done - 1198 rays/pixel
        00:19:45 49202MB         |    55% done - 932 rays/pixel
        00:20:07 49216MB         |    60% done - 2170 rays/pixel
        00:20:16 49220MB         |    65% done - 997 rays/pixel
        00:20:31 49228MB         |    70% done - 1574 rays/pixel
        00:20:46 49235MB         |    75% done - 1702 rays/pixel
        00:21:05 49244MB         |    80% done - 2118 rays/pixel
        00:21:20 49250MB         |    85% done - 1732 rays/pixel
        00:21:33 49255MB         |    90% done - 1400 rays/pixel
        00:21:49 49260MB         |    95% done - 2207 rays/pixel
        00:21:54 49262MB         |   100% done - 812 rays/pixel
        00:21:54 49262MB         |  render done in 6:59.988
        00:21:54 49262MB         |  [driver_exr] writing file `J:/pat/sc045/2165/lighting/render/pat_sc045_2165_lighting_v0001/pat_sc045_2165_lighting_v0003/pat_sc045_2165_lighting_v0003/Canyon/pat_sc045_2165_lighting_v0003_Canyon.1009.exr'

        """
        if -1 < stdout.find('[driver_') and -1 < stdout.find('writing file'):
            # Find out which frame number
            idx = stdout.rfind('/')
            if idx == -1:
                stdout.rfind('\\')
            if 1 < idx:
                frame_number = Common.parse_number(stdout[idx:])
                if frame_number is not None:
                    self.task_started(frame_number)


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
            Engine.warning(traceback.format_exc())
            Engine.usage()
            sys.exit(1)
