'''

    ffmpeg transcode accsyn compute app script.

    Finds and executes app by building a commandline out of 'item'(frame number)
    and parameters provided.

    Changelog:

        * v1r1; (Henrik, 23.09-29) Initial version

    This software is provided "as is" - the author and distributor can not be held
    responsible for any damage caused by executing this script in any means.

    Author: Henrik Norin, HDR AB

'''
import os
import sys
import traceback

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
    #   This section defines app behaviour and should not be refactored or moved
    #   away from the enclosing START/END markers. Read into memory by backend at
    #   launch and publish of new app.
    # -- APP CONFIG START --

    SETTINGS = {
        "items": False,
        "multiple_inputs": True,
        "filename_extensions": ".mov,.mp4,.wmv,.avi,.mpg,.mpeg,.mxf,.m2v,.m4v,.dv,.3gp,.3g2,.flv,.mkv,.vob,.webm",
        "binary": True,
        "profiles": {
            "h264": {
                "arguments": "-c:v libx264 -c:a aac -vf format=yuv420p -movflags +faststart -strict -2",
                "description": "Transcode to H264/AAC",
                "extension": ".mp4"
            }
        }
    }

    PARAMETERS = {"arguments": "-y -i ${INPUT} ${PROFILE} ${OUTPUT}", "profile": "h264", "input_conversion": "never"}

    ENVS = {}

    # -- APP CONFIG END --

    def __init__(self, argv):
        super(App, self).__init__(argv)

    @staticmethod
    def get_path_version_name():
        p = os.path.realpath(__file__)
        parent = os.path.dirname(p)
        return os.path.dirname(parent), os.path.basename(parent), os.path.splitext(os.path.basename(p))[0]

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
            return "/usr/bin/ffmpeg"
        elif Common.is_mac():
            return "/opt/local/bin/ffmpeg"
        elif Common.is_win():
            return "C:\\ffmpeg\\bin\\ffmpeg.exe"

    def get_envs(self):
        '''Return site specific envs here'''
        result = {}
        return result

    def get_commandline(self, item):
        '''(REQUIRED) Return command line as a string array'''
        args = []
        if "parameters" not in self.get_compute():
            raise Exception("No parameters for app")

        parameters = self.get_compute()["parameters"]

        if "arguments" not in parameters:
            raise Exception("No arguments for app")

        arguments = str(parameters["arguments"])

        for required_argument_spec in ["${INPUT}", "${OUTPUT}"]:
            if required_argument_spec not in arguments:
                raise Exception("No %s in arguments for app" % required_argument_spec)

        input_path = self.get_input()

        if not os.path.exists(input_path):
            Common.warning("Input media not found @ {}!".format(input_path))

        arguments = arguments.replace("${INPUT}", input_path)

        profile_data = None
        profile_arguments = ""
        if "${PROFILE}" in arguments:
            profile = (parameters.get("profile", "") or "").strip()

            if len(profile) > 0:
                # Locate profile among profiles
                profiles = App.SETTINGS["profiles"]
                if profile not in profiles:
                    raise Exception("Profile '{}' not found among profiles".format(profile))
                profile_data = profiles[profile]
                profile_arguments = profile_data["arguments"]
            else:
                Common.warning("Not profile specified, no transcoding will be done!")

            arguments = arguments.replace("${PROFILE}", profile_arguments)

        suffix = ""
        if "output" in self.get_compute():
            output_path = self.normalize_path(self.get_compute()["output"])
        else:
            Common.warning("No output path defined, will output to same folder as input.")
            output_path = os.path.dirname(input_path)
            suffix = "_transcoded"

        extension = ""
        if profile_data and "extension" in profile_data:
            extension = profile_data["extension"]

        if not os.path.exists(output_path):
            Common.warning("Output media path not found @ {}, creating".format(output_path))
            os.makedirs(output_path)
        elif output_path == os.path.dirname(input_path) and suffix == "":
            Common.warning("Output path is same as input path, will overwrite input media!")

        filename_output = os.path.basename(input_path)
        if suffix != "" or extension != "":
            filename_output = os.path.splitext(filename_output)[0] + suffix + (extension if extension != "" else os.path.splitext(filename_output)[1])

        output_path = os.path.join(output_path, filename_output)
        arguments = arguments.replace("${OUTPUT}", output_path)

        args.extend(arguments.split(" "))

        print("Transcoding '{}' => '{}' using ffmpeg, arguments: {}".format(input_path, output_path, profile_arguments))

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
