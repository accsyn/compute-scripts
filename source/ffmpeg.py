"""

    ffmpeg transcode accsyn compute engine script.

    Finds and executes app by building a commandline out of 'item'(frame number)
    and parameters provided.

    Changelog:

        * v1r7; (Henrik, 24.11.08) Support output path already defined in profile, with new hls profile. Align with v3 changes in common.
        * v1r6; (Henrik, 24.10.08) Adjusted arguments to argument build function due to changes in common.
        * v1r5; (Henrik, 23.11.24) Support for escape sequences in profile. Support
        space in input path.
        * v1r4; (Henrik, 23.11.01) Fix profile bug; H265 profile.
        * v1r1; (Henrik, 23.09.29) Initial version

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
    __revision__ = 7  # Increment this after each update

    # Engine configuration
    # IMPORTANT NOTE:
    #   This section defines engine behaviour and should not be refactored or moved
    #   away from the enclosing START/END markers. Read into memory by backend at
    #   launch and publish of new engine.
    # -- ENGINE CONFIG START --

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
                "extension": ".mp4"
            },
            "hls": {
                "arguments": "-filter:v:0 scale=-2:1080 -b:v:0 6000k -filter:v:1 scale=-2:720 -b:v:1 2800k -filter:v:2 scale=-2:480 -b:v:2 800k -map 0:v -map 0:a? -map 0:v -map 0:a? -map 0:v -map 0:a? -var_stream_map v:0,a:0%20v:1,a:1%20v:2,a:2 -f hls -hls_time 4 -hls_playlist_type vod -hls_segment_filename stream_%v_%03d.ts -master_pl_name index.m3u8 stream_%v.m3u8",
                "description": "",
                "default_output": "share=(default)/__STREAMING_MEDIA__",
                "chdir_output": True,
                "no_append_output": True,
                "suffix": "_${PROFILE_NAME}",
                "output_directory": True
            }
        },
        "type": "transcode",
        "color": "8,139,9",
        "vendor": "ffmpeg.org"
    }

    PARAMETERS = {"arguments": "-y -i ${INPUT} ${PROFILE} ${OUTPUT}", "profile": "h264", "input_conversion": "never"}

    ENVS = {}

    # -- ENGINE CONFIG END --

    def __init__(self, argv):
        super(Engine, self).__init__(argv)
        self._working_path = None

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
            result = "/usr/local/bin/ffmpeg"
            if not os.path.exists(result):
                result = "/usr/bin/ffmpeg"
            return result
        elif Common.is_mac():
            result = "/opt/local/bin/ffmpeg"
            if not os.path.exists(result):
                result = "/opt/homebrew/bin/ffmpeg"
            return result
        elif Common.is_win():
            return "C:\\ffmpeg\\bin\\ffmpeg.exe"

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

        for required_argument_spec in ["${INPUT}", "${OUTPUT}"]:
            if required_argument_spec not in arguments:
                raise Exception("No {} in arguments for engine".format(required_argument_spec))

        input_path = self.get_input()

        if not os.path.exists(input_path):
            Common.log("[WARNING] Input media not found @ {}!".format(input_path))

        arguments = arguments.replace("${INPUT}", input_path.replace(" ", "%20"))  # Preserve whitespace

        profile = None
        if 'profile' in self.get_compute():
            profile = self.get_compute()['profile']
        elif "${PROFILE}" in arguments:
            profile = (parameters.get("profile", "") or "").strip()
        profile_data = None
        profile_arguments = ""

        if profile is not None and len(profile) > 0:
            # Locate profile among profiles
            profiles = Engine.SETTINGS["profiles"]
            if profile not in profiles:
                raise Exception("Profile '{}' not found among profiles".format(profile))
            profile_data = profiles[profile]
            profile_arguments = profile_data["arguments"]
        else:
            Common.log("[WARNING] No profile specified, no transcoding will be done!")

        arguments = arguments.replace("${PROFILE}", profile_arguments)

        suffix = ""
        if profile_data and "suffix" in profile_data:
            suffix = Common.substitute(profile_data["suffix"], {
                "PROFILE_NAME": profile
            })

        chdir_output = profile_data.get('chdir_output', False) if profile_data else False
        output_directory = profile_data.get('output_directory', False) if profile_data else False
        no_append_output = profile_data.get('no_append_output', False) if profile_data else False
        if "output" in self.get_compute():
            output_path = self.normalize_path(self.get_compute()["output"])
        else:
            if profile_data and 'default_output' in profile_data:
                Common.log("No output path defined, falling back on default output: {}".format(
                    profile_data['default_output']))
                output_path = self.normalize_path(profile_data['default_output'])
            else:
                Common.log("[WARNING] No output path defined, will output to same folder as input.")
                output_path = os.path.dirname(input_path)

            if output_directory:
                subdir = os.path.splitext(os.path.basename(input_path))[0]
                Common.log(f"Output to subdirectory: '{subdir}'")
                output_path = os.path.join(output_path, subdir)

        if not os.path.exists(output_path):
            Common.log("[WARNING] Output media path not found @ {}, creating".format(output_path))
            os.makedirs(output_path)

        if not no_append_output:
            extension = ""
            if profile_data and "extension" in profile_data:
                extension = profile_data["extension"]

            if output_path == os.path.dirname(input_path) and suffix == "":
                suffix = "_transcoded"
                Common.log("[WARNING] Output path is same as input path, appending '_transcoded' suffix to prevent "
                           "overwrite of input media!")

            filename_output = os.path.basename(input_path)
            if suffix != "" or extension != "":
                filename_output = os.path.splitext(filename_output)[0] + suffix + (extension if extension != "" else
                                                                                   os.path.splitext(filename_output)[1])

            output_path = os.path.join(output_path, filename_output)

            arguments = arguments.replace("${OUTPUT}", output_path.replace(" ", "%20"))
        else:
            Common.log(f"Not appending output path")
            arguments = arguments.replace("${OUTPUT}", "")

        if chdir_output:
            Common.log(f"Transcoding in: {output_path}")
            self._working_path = output_path

        args.extend(Common.build_arguments(arguments))

        Common.log("Transcoding '{}' => '{}' using ffmpeg profile {}, arguments: {}".format(
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

    def get_working_path(self):
        if self._working_path is not None:
            return self._working_path
        return super(Engine, self).get_working_path()

    def shell(self):
        """(OPTIONAL) Return True if command should be executed in shell."""
        return False

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
