''' 

    Nuke v13 accsyn compute app script.

    Finds and executes Nuke by building a commandline out of 'item'(frame number)
    and parameters provided.

    Changelog:

        * v1r4; (Henrik Norin, 22.11.11) Report progress frame number/uri within a bucket. Url encoded arguments support.
        * v1r3; (Henrik Norin, 22.06.27) Make sure not all lines in files have slash conversions.
        * v1r2; (Henrik Norin) Fixed bug where p_executable_rel was not found.
        * v1r1; (Henrik Norin) First version.

    This software is provided "as is" - the author and distributor can not be held 
    responsible for any damage caused by executing this script in any means.

    Author: Henrik Norin, accsyn / HDR AB

'''

import os
import sys
import logging
import traceback
import socket
import datetime
import threading
import time

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
    __revision__ = 4  # Will be automatically increased each publish

    # App configuration
    #
    # IMPORTANT NOTE:
    #   This section defines app behaviour and should not be refactored or moved away from the
    # enclosing START/END markers. Read into memory by backend at start and publish. See Common.py
    # for setting and parameter descriptions.
    #

    # -- APP CONFIG START --

    SETTINGS = {
        "items": True,
        "default_range": "1001-1100",
        "default_bucketsize": 5,
        "filename_extensions": ".nk",
        "binary_filename_extensions": "",
    }

    PARAMETERS = {"mapped_share_paths": [], "arguments": ["-txV"], "input_conversion": "auto"}

    # -- APP CONFIG END --

    NUKE_VERSION = '13'

    def __init__(self, argv):
        super(App, self).__init__(argv)
        self.date_finish_expire = None

    @staticmethod
    def get_path_version_name():
        p = os.path.realpath(__file__)
        parent = os.path.dirname(p)
        return (os.path.dirname(parent), os.path.basename(parent), os.path.splitext(os.path.basename(p))[0])

    @staticmethod
    def version():
        (unused_cp, cv, cn) = Common.get_path_version_name()
        (unused_p, v, n) = App.get_path_version_name()
        Common.info(
            '   accsyn compute app "{0}" v{1}-{2}(common: v{3}-{4}) '.format(
                n, v, App.__revision__, cv, Common.__revision__
            )
        )

    @staticmethod
    def usage():
        (unused_p, unused_v, name) = App.get_path_version_name()
        Common.info('')
        Common.info('   Usage: python %s {--probe | <path_json_data>}' % (name))
        Common.info('')
        Common.info('       --probe           Check app existance and version.')
        Common.info('')
        Common.info(
            '       <path_json_data>  Execute app on data provided in the JSON and'
            ' ACCSYN_xxx environment variables.'
        )
        Common.info('')

    def probe(self):
        '''(Optional) Do nothing if found, raise exception otherwise.'''
        exe = self.get_executable()
        assert os.path.exists(exe), "'{}' does not exist!".format(exe)
        # Check if correct version here
        return True

    def convert_path(self, p, envs=None):
        '''
        (Override) Called with translated path, during input conversion, before written to file.
        Suitable for turning Windows backslashes to forward slashes with Nuke.
        '''
        return p.replace('\\', '/')

    def get_envs(self):
        '''(Optional) Get dynamic environment variables'''
        result = {}
        return result

    def get_executable(self, preferred_nuke_version=None):
        '''(REQUIRED) Return path to executable as string'''

        def find_executable(p_base, prefix):
            if os.path.exists(p_base):
                candidates = []
                for fn in os.listdir(p_base):
                    if fn.startswith(prefix):
                        candidates.append(fn)
                if 0 < len(candidates):
                    if preferred_nuke_version and preferred_nuke_version in candidates:
                        dirname = preferred_nuke_version
                    else:
                        dirname = sorted(candidates)[-1]
                    p_app = os.path.join(p_base, dirname)
                    p_executable_rel = None
                    # Find executable
                    if Common.is_mac():
                        p_executable_rel = os.path.join('{0}.app'.format(dirname), 'Contents', 'MacOS')
                        p_search_executable = os.path.join(p_app, p_executable_rel)
                    else:
                        p_search_executable = p_app
                    for fn in os.listdir(p_search_executable):
                        if fn.lower().startswith(prefix.lower()):
                            if Common.is_win() and not fn.lower().endswith('.exe'):
                                continue
                            p_executable_rel = (
                                '{0}{1}'.format(p_executable_rel, os.sep) if p_executable_rel else ""
                            ) + fn
                            break
                    return p_app, p_executable_rel
                else:
                    raise Exception('No {0} application version found on system!'.format(prefix))
            else:
                raise Exception('Application base directory "{0}" not found on system!'.format(p_base))

        # Use highest version
        p_base = p_app = None
        if Common.is_lin():
            p_base = '/usr/local'
        elif Common.is_mac():
            p_base = '/Applications'
        elif Common.is_win():
            p_base = 'C:\\Program Files'
        p_executable_rel = None
        if p_base:
            p_app, p_executable_rel = find_executable(p_base, 'Nuke{0}'.format(App.NUKE_VERSION))
        if p_executable_rel is None:
            raise Exception('Nuke executable not found, looked in {0}!'.format(p_app))
        if p_app:
            return os.path.join(p_app, p_executable_rel)
        else:
            raise Exception('Nuke not supported on this platform!')

    def get_commandline(self, item):
        '''(REQUIRED) Return command line as a string array'''

        args = []
        if 'parameters' in self.get_compute():
            parameters = self.get_compute()['parameters']

            if 0 < len(parameters.get('arguments') or ''):
                arguments = parameters['arguments']
                if 0 < len(arguments):
                    args.extend(Common.build_arguments(arguments))
        if self.item and self.item != 'all':
            # Add range
            start = end = self.item
            if -1 < self.item.find('-'):
                parts = self.item.split('-')
                start = parts[0]
                end = parts[1]
            args.extend(['-F', '%s-%s' % (start, end)])

        input_path = self.normalize_path(self.data['compute']['input'])
        args.extend([input_path])
        # Find out preffered nuke version from script, expect:
        #   #! C:/Program Files/Nuke10.0v6/nuke-10.0.6.dll -nx
        #   version 10.0 v6
        #   define_window_layout_xml {<?xml version="1.0" encoding="UTF-8"?>
        preferred_nuke_version = None
        with open(input_path, 'r') as f_input:
            for line in f_input:
                if line.startswith('version '):
                    #  version 10.0 v6
                    preferred_nuke_version = line[8:].replace(' ', '').strip()
                    Common.info('Parsed Nuke version: "%s"' % preferred_nuke_version)
                    break

        if Common.is_lin():
            retval = ['/bin/bash', '-c', self.get_executable(preferred_nuke_version=preferred_nuke_version)]
            retval.extend(args)
            return retval
        elif Common.is_mac():
            retval = [self.get_executable(preferred_nuke_version=preferred_nuke_version)]
            retval.extend(args)
            return retval
        elif Common.is_win():
            retval = [self.get_executable(preferred_nuke_version=preferred_nuke_version)]
            retval.extend(args)
            return retval

        raise Exception('This OS is not recognized by this accsyn app!')

    def process_output(self, stdout, stderr):
        '''
        Sift through stdout/stderr and take action, return exitcode instead of None if should
         abort.

        Nuke example output:

        Loading C:/Program Files/Nuke13.1v2/plugins/Text.dll!Loading C:/Program Files/Nuke13.1v2/plugins/jpegWriter.dll
        Loading C:/Program Files/Nuke13.1v2/plugins/movWriter.dll![16:26:40 W. Europe Standard Time]
        Read nuke script: J://ZZZ-DATA/farm/tempsaves/pat/sc045/2830/compositing/nk/pat_sc045_2830_compositing_v0002_cp_20221111_162546.nk

        Writing J:/pat/sc045/2830/compositing/render/pat_sc045_2830_compositing_v0001/sequence/pat_sc045_2830_compositing_v0001.01010.exr took 3.16 seconds

        Frame 1010 (2 of 5)!!!Writing J:/pat/sc045/2830/compositing/render/pat_sc045_2830_compositing_v0001/sequence/pat_sc045_2830_compositing_v0001.01011.exr .1.3.6.8!Writing J:/pat/sc045/2830/compositing/render/pat_sc045_2830_compositing_v0001/sequence/pat_sc045_2830_compositing_v0001.01011.exr took 3.31 seconds!Frame 1011 (3 of 5)!!.9!Writing J:/pat/sc045/2830/compositing/render/pat_sc045_2830_compositing_v0001/sequence/pat_sc045_2830_compositing_v0001.01012.exr .1.2.5.7.9!Writing J:/pat/sc045/2830/compositing/render/pat_sc045_2830_compositing_v0001/sequence/pat_sc045_2830_compositing_v0001.01012.exr took 3.32 seconds!Frame 1012 (4 of 5)!!!Writing J:/pat/sc045/2830/compositing/render/pat_sc045_2830_compositing_v0001/sequence/pat_sc045_2830_compositing_v0001.01013.exr .2.4.7.9!Writing J:/pat/sc045/2830/compositing/render/pat_sc045_2830_compositing_v0001/sequence/pat_sc045_2830_compositing_v0001.01013.exr took 3.55 seconds!Frame 1013 (5 of 5)!!Total render time: 21 seconds!(2022-11-11 16:27:04)

        '''
        sys.stdout.flush()

        if -1 < stdout.find('Writing') and -1 < stdout.find('took'):
            idx = stdout.rfind('/')
            if idx == -1:
                stdout.rfind('\\')
            if 1 < idx:
                frame_number = Common.parse_number(stdout[idx : stdout.rfind('took')])
                if frame_number is not None:
                    self.task_started(frame_number)

        def check_stuck_render():
            while self.executing:
                time.sleep(1.0)
                if (
                    self.executing
                    and not self.date_finish_expire is None
                    and self.date_finish_expire < datetime.datetime.now()
                ):
                    Common.warning('Nuke finished but still running (hung?), finishing up.')
                    self.exitcode_force = 0
                    self.kill()
                    break  # We are done

        ''' Nuke might stuck on finished render, handle this. '''
        if -1 < (stdout + stderr).find('Total render time:'):
            Common.info('Finished Nuke render will expire in 5s...')
            self.date_finish_expire = datetime.datetime.now() + datetime.timedelta(seconds=5)
            thread = threading.Thread(target=check_stuck_render)
            thread.start()


if __name__ == '__main__':
    App.version()
    if '--help' in sys.argv:
        App.usage()
    else:
        # Common.set_debug(True)
        try:
            app = App(sys.argv)
            if '--probe' in sys.argv:
                app.probe()
            else:
                app.load()  # Load data
                app.execute()  # Run
        except:
            App.warning(traceback.format_exc())
            App.usage()
            sys.exit(1)
