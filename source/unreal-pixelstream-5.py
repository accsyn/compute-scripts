'''

    Unreal PixelStream accsyn compute app script.

    Executes a pixel stream server instance based on the game exe provided, the computer and lane. Spawns
    an additional Signalling Web server, designed to be reachable over the WAN.

    WAN access requires NAT port forwarding of required streaming ports, see mapping table in settings.

    Will update task description with pixel stream URL.

    Changelog:

        v1r1; Initial version.

    This software is provided "as is" - the author and distributor can not be held
    responsible for any damage caused by executing this script in any means.

    Author: Henrik Norin, HDR AB

'''

import os
import sys
import time
import traceback
import socket
import copy
from threading import Thread
import subprocess

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
        "max_bucketsize": 1,
        "filename_extensions": ".exe",
        "ports": {
            "YODA": {
                "stream": 18888,
                "http": 180,
                "https": 1443,
                "turn": 19303,
                "http_public": 80,
                "turn_public": 19303,
            }
        },
    }

    PARAMETERS = {"arguments": "-RenderOffscreen", "input_conversion": "never"}

    ENVS = {}

    # -- APP CONFIG END --

    INFRASTRUCTURE_PATH = os.path.join("C:\\", "ProgramData", "accsyn", "compute", "PixelStreamingInfrastructure")

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
        '''(Optional) Do nothing if found, raise execption otherwise.'''
        exe = self.get_executable()
        assert os.path.exists(exe), "'%s' does not exist!" % exe
        # TODO, check if correct version
        return True

    def get_executable(self):
        '''(REQUIRED) Return path to executable as string'''
        if Common.is_lin():
            raise Exception("Unreal pixel stream only supported on Windows")
        elif Common.is_mac():
            raise Exception("Unreal pixel stream only supported on Mac")
        elif Common.is_win():
            return self.normalize_path(self.get_compute()["input"])

    def get_envs(self):
        '''Return site specific envs here'''
        result = {}
        return result

    def pre(self):
        '''Spawn pixelstream web server, requires Git installed by this guide:

        https://docs.unrealengine.com/5.2/en-US/getting-started-with-pixel-streaming-in-unreal-engine/

        '''

        if "ACCSYN_LANE" not in os.environ:
            raise Exception(
                'Need ACCSYN_LANE set in environment variables, make sure you are running accsyn app v2.5 or later'
            )

        LANE_NUMBER = int(os.environ["ACCSYN_LANE"])

        Common.info("Preparing web server infrastructure")
        if not os.path.exists(App.INFRASTRUCTURE_PATH):
            Common.info("")
            Common.info(
                "Refer to https://docs.unrealengine.com/5.2/en-US/"
                "getting-started-with-pixel-streaming-in-unreal-engine/"
            )
            Common.info("")
            Common.info(
                "Also remember to allow PowerShell script execution - launch powershell as an administrator and run: "
                "Set-ExecutionPolicy -ExecutionPolicy UnRestricted"
            )
            Common.info("")
            raise Exception(
                "Could not locate pixel stream infrastructure @ '{}', please git glone and setup it up.".format(
                    App.INFRASTRUCTURE_PATH
                )
            )

        self.p_script_base = os.path.join(App.INFRASTRUCTURE_PATH, "SignallingWebServer", "platform_scripts", "cmd")

        p_script_common_src = os.path.join(self.p_script_base, "Start_Common.ps1")
        parts = os.path.splitext(p_script_common_src)
        self.p_script_common_dst = "{}-{}{}".format(parts[0], self.data['process']['id'], parts[1])

        Common.info("Preparing config")
        hostname = socket.gethostname()
        if hostname not in App.SETTINGS['ports']:
            raise Exception("This host({}) is not in app port settings!".format(hostname))

        self.port_config = copy.deepcopy(App.SETTINGS['ports'][hostname])
        for key, value in self.port_config.items():
            self.port_config[key] = self.port_config[key] + LANE_NUMBER - 1

        Common.info("   Transposed port config: {}".format(self.port_config))

        Common.info("Creating common launch script @ '{}'".format(self.p_script_common_dst))
        with open(p_script_common_src, "r") as f_src:
            with open(self.p_script_common_dst, "w") as f_dst:
                for line in f_src.readlines():
                    f_dst.write("{}".format(line.replace("19303", str(self.port_config["turn"]))))

        p_script_src = os.path.join(self.p_script_base, "Start_WithTURN_SignallingServer.ps1")
        parts = os.path.splitext(p_script_src)
        self.p_script_dst = "{}-{}{}".format(parts[0], self.data['process']['id'], parts[1])

        Common.info("Creating launch script @ '{}'".format(self.p_script_dst))
        with open(p_script_src, "r") as f_src:
            with open(self.p_script_dst, "w") as f_dst:
                for line in f_src.readlines():
                    if line.find("Start_Common.ps1") > -1:
                        line = line.replace("Start_Common.ps1", os.path.basename(self.p_script_common_dst))
                    if line.find("Start_TURNServer.ps1") > -1:
                        line = "{} -NoNewWindow\n".format(line.replace("\n", ""))
                    f_dst.write("{}".format(line))

        # Execute in separate thread
        self.webserver_executing = False
        Common.info("Running web server in separate thread")
        thread = Thread(target=self._run_webserver)
        thread.start()

        # Wait for it to start executing
        waited_s = 0
        while not self.webserver_executing and waited_s < 100:
            time.sleep(1.0)
            waited_s += 1
            if waited_s > 10:
                Common.warning("Waited {}s for pixel stream web server to launch...")

        assert self.webserver_executing, "Timeout waiting for web server to launch!"

    def _run_webserver(self):
        commands = [
            "PowerShell",
            "-File",
            os.path.basename(self.p_script_dst),
            "--HttpPort={}".format(self.port_config["http"]),
            "--HttpsPort={}".format(self.port_config["https"]),
            "--StreamerPort={}".format(self.port_config["stream"]),
        ]

        exitcode = None

        self.terminate_web_process = False

        Common.info("Changing to web server directory ({0})...".format(self.p_script_base))
        os.chdir(self.p_script_base)
        Common.info("Running web server: '{0}'".format(str([Common.safely_printable(s) for s in commands])))

        try:
            self.webserver_process = subprocess.Popen(commands, True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

            while True:
                # Read data waiting for us in pipes
                stdout = Common.safely_printable(self.webserver_process.stdout.readline())
                print('!{}'.format(stdout), end='')
                sys.stdout.flush()

                # Parse output for public IP
                for line in stdout.split("\n"):
                    if line.find("Public IP address") > -1:
                        # Expect: Public IP address : 207.189.207.12
                        self.public_ip = line[line.find(":") + 1 :].replace("\n", "").strip()
                        # Now we can update description on task
                        print(
                            """{"taskdescription":"Pixel stream link: http://%s:%s","uri":"%s"}"""
                            % (self.public_ip, self.port_config["http_public"], self.item)
                        )
                        self.webserver_executing = True
                    elif line.find("Lib vorbis DLL was dynamically loaded") > -1:
                        # @@@ test
                        print("""{"taskdescription":"%s","uri":"%s"}""" % (line, self.item))
                if self.terminate_web_process:
                    Common.warning(
                        'Pre-emptive terminating web server process (pid: {0}).'.format(self.webserver_process.pid)
                    )
                    exitcode = 1
                    break
                elif stdout == '' and self.webserver_process.poll() is not None:
                    break

            self.webserver_process.communicate()
            if exitcode is None:
                exitcode = self.webserver_process.returncode

        finally:
            try:
                self.webserver_process.terminate()
            except:
                pass

        self.webserver_process = None

        Common.info("Web server exitcode: {0}".format(exitcode))

        if not self.terminate_web_process:
            # We terminated but Unreal is still running, needs to be killed
            if self.executing:
                Common.warning("Terminating Unreal...")
                try:
                    self.process.terminate()
                except:
                    pass
            assert exitcode == 0, "Web server execution failed, check log for clues..."

        self.webserver_exitcode = exitcode

    def get_commandline(self, item):
        '''(REQUIRED) Return command line as a string array'''
        # Check if correct version of V-ray - verify size of main library
        path_executable = self.get_executable()
        args = []
        if "parameters" in self.get_compute():
            parameters = self.get_compute()["parameters"]
            if 0 < len(parameters.get("arguments") or ""):
                args.extend(Common.build_arguments(parameters['arguments']))
        if Common.is_lin():
            pass
        elif Common.is_mac():
            pass
        elif Common.is_win():
            retval = [path_executable]
            retval.extend(args)
            return retval

        raise Exception('This operating system is not recognized by this accsyn' ' app!')

    def post(self, exitcode):
        '''Post execution, to be overridden.'''
        Common.info("Telling web server process (and TURN server) to exit")
        self.terminate_web_process = True
        # Remove temporary scripts
        Common.info("Cleaning up Powershell temp scripts")
        try:
            os.remove(self.p_script_common_dst)
        except:
            pass
        try:
            os.remove(self.p_script_dst)
        except:
            pass


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
