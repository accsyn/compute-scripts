"""

    Unreal Movie Render Queue accsyn compute engine script.

    Generates and rendere movie render queue based on the project, map, sequence, frame range and output path.

    WAN access requires NAT port forwarding of required streaming ports, see mapping table in settings.

    Will update task description with pixel stream URL.

    Changelog:

        v1r1; [250908, Henrik Norin] Initial version.

    This software is provided "as is" - the author and distributor can not be held
    responsible for any damage caused by executing this script in any means.

    Author: Henrik Norin, HDR AB

"""
import datetime
import os
import sys
import tempfile
import traceback
from threading import Thread
import subprocess
import csv
from io import StringIO
import socket


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

MRQ_GEN_TEMPLATE = """
import unreal
import os

def create_mrq(
    level_path="/Game/Main",
    sequence_path="/Game/NewLevelSequence",
    preset_path="/Game/TestPreset",
    output_dir="C:/TEMP/TestRender",
    start_frame=100,
    end_frame=150,
    asset_path="/Game/TempMRQ",
):

    # Load assets
    sequence = unreal.load_asset(sequence_path)
    if not sequence:
        unreal.log_error(f"[ERROR] Failed to load sequence: {sequence_path}")
        return None
    preset = unreal.load_asset(preset_path)
    if not preset:
        unreal.log_error(f"[ERROR] Failed to load preset: {preset_path}")
        return None

    unreal.log(f"[INFO] Generating temp MRQ asset for sequence: {sequence}, map: {level_path}")

    # Extract package path and name
    asset_name = asset_path.split("/")[-1]
    package_path = "/".join(asset_path.split("/")[:-1])

    # Build in-memory MRQ queue
    temp_queue = unreal.MoviePipelineQueue()
    job = temp_queue.allocate_new_job(unreal.MoviePipelineExecutorJob)
    job.job_name = asset_name
    job.map = unreal.SoftObjectPath(level_path)
    job.sequence = unreal.SoftObjectPath(sequence.get_path_name())
    
    # MRG
    job.set_graph_preset(preset)

    # First, read in all variables.
    vars=preset.get_variables()
    mapped_vars = dict()
    for var in vars:
        mapped_vars[var.get_member_name()] = var

    unreal.log(f"Got MRG variables: {mapped_vars.keys()}")
  
    gvas=job.get_editor_property("graph_variable_assignments")
    vas=gvas[0]
    vas.update_graph_variable_overrides()
    value=vas.get_editor_property("value")
    unreal.log(f"Got MRG default variable assignments: {value.export_text()}")

    if start_frame is not None and end_frame is not None:
        vas.set_value_int32(mapped_vars["Custom Start Frame"], start_frame)
        vas.set_variable_assignment_enable_state(mapped_vars["Custom Start Frame"], True)
        vas.set_value_int32(mapped_vars["Custom End Frame"], end_frame+1)
        vas.set_variable_assignment_enable_state(mapped_vars["Custom End Frame"], True)
    
    vas.set_value_serialized_string(mapped_vars["Output Directory"], f'(Path="{output_dir}")')
    vas.set_variable_assignment_enable_state(mapped_vars["Output Directory"], True)

    if "Include ProRes" in mapped_vars:
        vas.set_value_bool(mapped_vars["Include ProRes"], False)
        vas.set_variable_assignment_enable_state(mapped_vars["Include ProRes"], True)

    # MRQ
    #job.set_configuration(preset)
 
    #config = job.get_configuration()
    #output_node = config.get_output_node()
    #output_setting = config.find_or_add_setting_by_class(unreal.MoviePipelineOutputSetting)
    #output_setting.output_directory = unreal.DirectoryPath(output_dir)
    #output_setting.file_name_format = "{sequence_name}_{frame_number}"
    #if start_frame is not None and end_frame is not None:
    #    output_setting.use_custom_playback_range = True
    #    output_setting.custom_start_frame = start_frame
    #    output_setting.custom_end_frame = end_frame+1

    # Create dummy MRQ asset on disk
    asset_tools = unreal.AssetToolsHelpers.get_asset_tools()
    new_queue_asset = asset_tools.create_asset(
        asset_name=asset_name,
        package_path=package_path,
        asset_class=unreal.MoviePipelineQueue,
        factory=None  # <- no factory needed
    )

    if not new_queue_asset:
        unreal.log_error(f"[ERROR] Failed to create asset at {asset_path}")
        return None

    # Copy from in-memory to saved asset
    new_queue_asset.copy_from(temp_queue)

    # Save to disk
    saved = unreal.EditorAssetLibrary.save_asset(asset_path, only_if_is_dirty=False)
    if saved:
        unreal.log(f"[INFO] MRQ asset successfully created: {asset_path}")
        return asset_path
    else:
        unreal.log_error(f"[ERROR] Failed to save MRQ asset at {asset_path}")
        return None

create_mrq(level_path="%s", sequence_path="%s", preset_path="%s", output_dir="%s", start_frame=%s, end_frame=%s, asset_path="%s")
"""

PIPELINE_CREDENTIALS_PATH="\\\\192.168.111.3\\RAIDPOOL01\\_TOOLS\\ACCSYN_TOOLS\\pipeline.csv"

class Engine(Common):
    __revision__ = 5  # Increment this after each update

    # Engine configuration
    # IMPORTANT NOTE:
    # This section defines engine behaviour and should not be refactored or moved
    # away from the enclosing START/END markers. Read into memory by backend at
    # start and publish.
    # -- ENGINE CONFIG START --

    SETTINGS = {
        "items": True,
        "default_range": "0-1000",
        "default_bucketsize": 100,
        "filename_extensions": ".uproject",
        "color": "71,187,226",
        "vendor": "Epic Games"
    }

    PARAMETERS = {
        "arguments":"-game -windowed -StdOut -allowStdOutLogVerbosity -Unattended",
        "input_conversion":"never"
    }

    ENVS = {}

    # -- ENGINE CONFIG END --

    def __init__(self, argv):
        super(Engine, self).__init__(argv)
        self.output_path = None
        # Initialize frame tracking variables
        self.previous_sequences = []
        self.target_frame_range = None
        self.completed_frames = set()
        
        
    def _parse_target_frame_range(self):
        """Parse self.item to determine the target frame range."""
        if not self.item or self.item == "all":
            return None
            
        if "-" in self.item:
            # Range format: "start-end"
            parts = self.item.split("-")
            try:
                start = int(parts[0])
                end = int(parts[1])
                return set(range(start, end + 1))
            except (ValueError, IndexError):
                Common.warning(f"Invalid frame range format: {self.item}")
                return None
        else:
            # Single frame
            try:
                frame = int(self.item)
                return {frame}
            except ValueError:
                Common.warning(f"Invalid frame number: {self.item}")
                return None

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
            raise Exception("Unreal Movie Render Queue not supported on Linux yet!")
        elif Common.is_mac():
            raise Exception("Unreal Movie Render Queue not supported on Mac yet!")
        elif Common.is_win():
            return "C:\\Program Files\\Epic Games\\UE_5.5\\Engine\\Binaries\\Win64\\UnrealEditor-Cmd.exe"

    def get_envs(self):
        """Return site specific envs here"""
        result = {}
        return result

    @staticmethod
    def validate_asset_path(asset_path):
        """Validate asset path"""
        if not asset_path.startswith("/Game/"):
            raise Exception(f"Asset path must start with /Game/: {asset_path}")

    def remove_temp_mrq_asset(self, warn=True):
        """Remove temp MRQ asset"""
        project_path = os.path.dirname(self.normalize_path(self.get_compute()["input"]))
        content_path = os.path.join(project_path, "Content")
        mrq_asset_path = os.path.join(content_path, f"{self.asset_name}.uasset")
        if os.path.exists(mrq_asset_path):
            Common.info(f"Removing temp MRQ asset: {mrq_asset_path}")
            os.remove(mrq_asset_path)
        else:
            if warn:
                Common.warning(f"Temp MRQ asset not found: {mrq_asset_path}")
            else:
                Common.info(f"Temp MRQ asset not found: {mrq_asset_path}")

    def pre(self):
        """Generate movie render queue asset for the given frame range and output path using Unreal"""
        # Validate data
        assert "parameters" in self.get_compute(), "Parameters are required!"
        parameters = self.get_compute()["parameters"]
        start = end = None
        if self.item and self.item != "all":
            # Add range
            start = end = self.item
            if -1 < self.item.find("-"):
                parts = self.item.split("-")
                start = parts[0]
                end = parts[1]

        # Expect input_path to be a path to the unreal project
        input_path = self.normalize_path(self.get_compute()["input"])
        assert "input" in self.get_compute(), "Unreal project input path is required!"

        assert "level" in parameters, "Level asset path is required!"
        Engine.validate_asset_path(parameters["level"])

        assert "sequence" in parameters, "Sequence asset path is required!"
        Engine.validate_asset_path(parameters["sequence"])
        self.asset_name = parameters["sequence"].split("/")[-1]

        assert "preset" in parameters, "Preset asset path is required!"
        Engine.validate_asset_path(parameters["preset"])
        
        assert "output" in self.get_compute(), "Output path is required!"
        self.output_path = self.normalize_path(self.get_compute()["output"])

         # First two elements is the drive letter and the project name
        drive_letter = input_path.split("\\")[0]
        self.project_name = input_path.split("\\")[1]
        self.project_folder_path = f"{drive_letter}\\{self.project_name}"
        Common.log(f"(pre) Project folder: {self.project_folder_path}")

         # First, sync perforce folder       
        self.sync_perforce_folder()

        # Then, generate the MRQ asset
        # Generate a temp python script path
        self.p_temp_script = tempfile.mktemp(suffix=".py", prefix=f"accsyn-mrq-gen-{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}-")
 
        result = MRQ_GEN_TEMPLATE%(
            parameters["level"],
            parameters["sequence"],
            parameters["preset"],
            self.output_path.replace("\\", "/"),
            start if start is not None else "None",
            end if end is not None else "None",
            f"/Game/{self.asset_name}")
        # Write temp script
        Common.info(f"(pre) Writing temp MRQ generation script to: {self.p_temp_script}")
        if self.is_debug():
            self.debug(f"Script {self.p_temp_script} contents: {result}")
        with open(self.p_temp_script, "w") as f:
            f.write(result)
        try:
            self.remove_temp_mrq_asset(warn=False) # Make sure to remove any existing temp MRQ asset
            # Run temp script
            commands = [
                self.get_executable(), 
                self.normalize_path(self.get_compute()["input"]), 
                f"-ExecutePythonScript={self.p_temp_script}",
                "-log"]
            Common.log(f"(pre) Generating temp MRQ asset {self.asset_name}")
            Common.info(f"Running command: {commands})")
            Common.log("")
            Common.log("-" * 120)
            Common.log("")
            # Run and capture output using communicate() to avoid deadlock
            stderr_count = 0
            with subprocess.Popen(commands, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) as process:
                stdout, stderr = process.communicate()
                
                # Log stdout line by line, to service log
                if stdout:
                    for line in stdout.splitlines():
                        line = f"(pre) {line.strip()}"
                        if line.find("[INFO]") > -1 or line.find("[WARNING]") > -1 or line.find("[ERROR]") > -1:
                            Common.log(line)
                        else:
                            Common.info(line)
                
                # Log stderr line by line, to public log
                if stderr:
                    for line in stderr.splitlines():
                        Common.log_stderr(f"(pre) {line.strip()}")
                        stderr_count += 1

            Common.log("")
            Common.log("-" * 120)
            Common.log("")
            if process.returncode != 0:
                Common.warning(f"Failed to generate MRQ asset: {process.returncode}")
                raise Exception(f"Failed to generate MRQ asset: {process.returncode}")
        except Exception as e:
            Common.warning(traceback.format_exc())
            Common.warning(f"Failed to run temp script {self.p_temp_script}: {e}")
            raise
        finally:
            # Remove temp script
            if False:
                os.remove(self.p_temp_script)
            
    def do_print(stderr=False):
        return stderr # Log errors to public log, rest to service log

    def get_commandline(self, item):
        """Run the render using Unreal CLI"""
        # "C:\Program Files\Epic Games\UE_5.5\Engine\Binaries\Win64\UnrealEditor-Cmd.exe" 
        #     "C:\Users\Stiller\Documents\Unreal Projects\HenrikTest\HenrikTest.uproject" 
        #     /Game/Main -game  -windowed -Log -StdOut -allowStdOutLogVerbosity 
        #     -Unattended -MoviePipelineConfig=/Game/Queue -log
        args = []
        
        p_input = self.normalize_path(self.get_compute()["input"])
        args.extend([p_input])

        parameters = self.get_compute()["parameters"]

        args.extend([parameters["level"]])

        if 0 < len(parameters.get("arguments") or ""):
            arguments = parameters["arguments"]
            if 0 < len(arguments):
                args.extend(arguments.split(" "))

        args.append(f"-MoviePipelineConfig=/Game/{self.asset_name}")

        if self.is_debug() or True:
            args.append("-log")
        
        Common.log(f"Rendering sequence: {parameters['sequence']} (project: {self.project_name}, level: {parameters['level']}) using preset: {parameters['preset']}, "
                        f"frame range: {self.item} to: {self.get_compute()['output']}")
        
        # Input has already been converted to local platform
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

    def post(self, exitcode):
        """Post execution, remove temp MRQ asset."""
        self.remove_temp_mrq_asset(warn=False)

    def _watch_output_folder(self):
        """ Watch the output folder for new finished frames, report back these tasks as done. """
        if not self.output_path:
            Common.warning("No output path loaded yet")
            return
        
        # Parse target frame range if not already done
        if self.target_frame_range is None:
            self.target_frame_range = self._parse_target_frame_range()
            if self.target_frame_range:
                Common.info(f"Monitoring bucket: {self.item} (frames: {sorted(list(self.target_frame_range))})")
            else:
                Common.info("No specific frame range to monitor")
                return

        # Scan current file sequences
        current_sequences = Common.scan_file_sequences(self.output_path)
        
        # Build a lookup for current sequences by (subdir, filename, ext)
        current_lookup = {}
        for seq in current_sequences:
            key = (seq["subdir"], seq["filename"], seq["ext"])
            current_lookup[key] = set(seq["frames"])
        
        # Build a lookup for previous sequences
        previous_lookup = {}
        for seq in self.previous_sequences:
            key = (seq["subdir"], seq["filename"], seq["ext"])
            previous_lookup[key] = set(seq["frames"])
        
        # Find new frames
        new_frames = set()
        for key, current_frames in current_lookup.items():
            previous_frames = previous_lookup.get(key, set())
            new_frames_in_seq = current_frames - previous_frames
            new_frames.update(new_frames_in_seq)
        
        # Filter new frames to only include those in our target range
        target_new_frames = new_frames & self.target_frame_range
        
        # Remove frames we've already reported
        unreported_frames = target_new_frames - self.completed_frames
        
        if unreported_frames:
            # Sort frames to report them in order (assuming frames are rendered in order)
            sorted_frames = sorted(list(unreported_frames))
            Common.info(f"Found {len(sorted_frames)} new frames in target range: {sorted_frames}")
            
            for frame in sorted_frames:
                Common.log(f"Reporting frame {frame} as completed")
                self.task_done(str(frame))
                self.completed_frames.add(frame)
        
        # Update previous sequences for next comparison
        self.previous_sequences = current_sequences


    def get_background_worker(self):
        """ Tell engine to run our worker every 10 seconds """
        return (10.0, self._watch_output_folder)

    # Perforce

    def set_perforce_credentials(self):
        """ Read and load P4 credentials """

        computer_name = socket.gethostname()  # Get computer name
        if not computer_name:
            Common.log("[ERROR] Unable to get the computer name.")
            sys.exit(1)

        entry = None
        with open(PIPELINE_CREDENTIALS_PATH, "r") as f:
            data = f.read()

            # Use csv.DictReader to parse the data
            reader = csv.DictReader(StringIO(data), delimiter=';')

            # Read rows into list of dicts
            rows = [row for row in reader if any(row.values())]

            # Find entry for this computer
            for row in rows:
                if -1<row['computer'].lower().find(computer_name.lower()):
                    entry = row
                    break
            if not entry:
                print(f"[ERROR] Unable to find credentials for computer {computer_name} in pipeline data file.")
                sys.exit(1)

        # Set P4USER and P4PASSWD environment variables here
        os.environ["P4USER"] = entry['p4user']  # Replace with your Perforce username
        os.environ["P4PASSWD"] = entry['p4password']  # Replace with your Perforce password
        os.environ["P4CLIENT"] = entry['p4client']  # Replace with your Perforce password
        Common.log(f"Perforce credentials set (user: {entry['p4user']})")

        return entry['p4client']

    # def get_latest_changelist(self):    
    #     # Run p4 changes to get the latest changelist for the specified path
    #     command = ["p4", "changes", "-m", "1", f"{self.project_folder_path}"]
    #     Common.log("Executing command:", " ".join(command))  # Print the command to be executed

    #     try:
    #         result = subprocess.run(command, capture_output=True, text=True, check=True)
    #         # Extract the changelist number from the output
    #         lines = result.stdout.strip().splitlines()
    #         if lines:
    #             changelist = lines[0].split()[1]  # The changelist number is the second element
    #             Common.log(f"Latest changelist: {changelist}")
    #             return changelist
    #         else:
    #             Common.log("Error: No changelist found.")
    #             return None
    #     except subprocess.CalledProcessError as e:
    #         Common.log(f"Error getting the latest changelist: {e.stderr}")
    #         sys.exit(1)

    def sync_perforce_folder(self):
        """Sync the Perforce folder"""
        workspace = self.set_perforce_credentials()  # Set up the Perforce credentials

        Common.log(f"Syncing Perforce workspace: {workspace}")

        #changelist_number = self.get_latest_changelist()  # Get the latest changelist automatically
        # command = ["p4", "-c", workspace, "sync", f"{PROJECT_FOLDER_PATH}@{changelist_number}"]
        
        # Change dir to project folder
        os.chdir(self.project_folder_path)
        
        commands = ["p4", "sync", f"//{self.project_name}/Environments/...#head", f"//{self.project_name}/_Tools_Plugins/...#head"]
        Common.log("Executing command:", " ".join(commands))  # Print the full command

        try:
            # Run the Perforce sync command using communicate() to avoid deadlock
            with subprocess.Popen(commands, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) as process:
                stdout, stderr = process.communicate()
                
                # Log stdout line by line
                if stdout:
                    for line in stdout.splitlines():
                        Common.info(f"(p4 sync) {line.strip()}")
                
                # Log stderr line by line
                if stderr:
                    for line in stderr.splitlines():
                        Common.log_stderr(f"(p4 sync) {line.strip()}")

            if process.returncode != 0:
                Common.log(f"Perforce sync completed with errors(returncode: {process.returncode}), ignoring.")
            else:
                Common.log("Perforce sync completed successfully.")
 
        except subprocess.TimeoutExpired:
            Common.log(f"[ERROR] The Perforce sync command timed out.")
            sys.exit(1)

        except subprocess.CalledProcessError as e:
            Common.log(f"[ERROR] syncing Perforce files: {e.stderr}")
            sys.exit(1)

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
