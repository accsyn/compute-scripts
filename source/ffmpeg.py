'''

    ffmpeg transcode accsyn compute app script.

    Finds and executes app by building a commandline out of 'item'(frame number)
    and parameters provided.

    Changelog:

        * v1r5; (Henrik, 23.11.24) Support for escape sequences in profile. Support
        space in input path.
        * v1r4; (Henrik, 23.11.01) Fix profile bug; H265 profile.
        * v1r1; (Henrik, 23.09.29) Initial version

    This software is provided "as is" - the author and distributor can not be held
    responsible for any damage caused by executing this script in any means.

    Author: Henrik Norin, HDR AB

'''
import copy
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
    __revision__ = 4  # Increment this after each update

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
                "suffix": "_${PROFILE_NAME}",
                "extension": ".mp4",
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
            result = "/usr/local/bin/ffmpeg"
            if not os.path.exists(result):
                result = "/usr/bin/ffmpeg"
            return result
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

        arguments = arguments.replace("${INPUT}", input_path.replace(" ", "%20")) # Preserve whitespace

        profile = None
        if 'profile' in self.get_compute():
            profile = self.get_compute()['profile']
        elif "${PROFILE}" in arguments:
            profile = (parameters.get("profile", "") or "").strip()
        profile_data = None
        profile_arguments = ""

        if profile is not None and len(profile) > 0:
            # Locate profile among profiles
            profiles = App.SETTINGS["profiles"]
            if profile not in profiles:
                raise Exception("Profile '{}' not found among profiles".format(profile))
            profile_data = profiles[profile]
            profile_arguments = profile_data["arguments"]
        else:
            Common.warning("Not profile specified, no transcoding will be done!")

        arguments = arguments.replace("${PROFILE}", Common.build_arguments(profile_arguments, escaped_quotes=False))

        suffix = ""
        if profile_data and "suffix" in profile_data:
            suffix = Common.substitute(profile_data["suffix"], {
                "PROFILE_NAME": profile
            })

        if "output" in self.get_compute():
            output_path = self.normalize_path(self.get_compute()["output"])
        else:
            if profile_data and 'default_output' in profile_data:
                Common.info("No output path defined, falling back on default output: {}".format(
                    profile_data['default_output']))
                output_path = self.normalize_path(profile_data['default_output'])
            else:
                Common.warning("No output path defined, will output to same folder as input.")
                output_path = os.path.dirname(input_path)

        extension = ""
        if profile_data and "extension" in profile_data:
            extension = profile_data["extension"]

        if not os.path.exists(output_path):
            Common.warning("Output media path not found @ {}, creating".format(output_path))
            os.makedirs(output_path)
        elif output_path == os.path.dirname(input_path) and suffix == "":
            suffix = "_transcoded"
            Common.warning("Output path is same as input path, appending '_transcoded' suffix to prevent overwrite of "
                           "input media!")

        filename_output = os.path.basename(input_path)
        if suffix != "" or extension != "":
            filename_output = os.path.splitext(filename_output)[0] + suffix + (extension if extension != "" else
                                                                               os.path.splitext(filename_output)[1])

        output_path = os.path.join(output_path, filename_output)
        arguments = arguments.replace("${OUTPUT}", output_path.replace(" ", "%20"))

        args.extend(Common.build_arguments(arguments, join=False))

        print("Transcoding '{}' => '{}' using ffmpeg profile {}, arguments: {}".format(
            input_path, output_path, profile, profile_arguments))

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