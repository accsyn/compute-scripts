"""

    allente/annex media export accsyn compute engine script.

    Exports input media to output folder, with dependencies, defined by the Allente/Annex media export format

    Changelog:

        * v1r2; (Henrik, 24.07.13) Initial version
        * v1r1; (Henrik, 24.06.22) Initial version

    This software is provided "as is" - the author and distributor can not be held
    responsible for any damage caused by executing this script in any means.

    Author: Henrik Norin, HDR AB

"""
import subprocess
import os
import sys
import traceback
import time

import xml.etree.ElementTree as ET

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
    sys.stderr.write('[DEBUG] Sys path: {}'.format(sys.path))
    raise


class Engine(Common):
    __revision__ = 1  # Increment this after each update

    # Engine configuration
    # IMPORTANT NOTE:
    #   This section defines engine behaviour and should not be refactored or moved
    #   away from the enclosing START/END markers. Read into memory by backend at
    #   launch and publish of new engine.
    # -- ENGINE CONFIG START --

    SETTINGS = {
        "items": False,
        "multiple_inputs": False,
        "filename_extensions": ".mov,.mp4,.mxf",
        "binary": True,
        "default_output_path": "__EXPORTS__",
        "type": "exporter",
        "color": "253,96,0",
        "vendor": "accsyn"
    }

    PARAMETERS = {
        "input_conversion": "never"
    }

    ENVS = {}

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
            '   accsyn compute engine "{}" v{}-{}(common: v{}-{}) '.format(n, v, Engine.__revision__, cv, Common.__revision__)
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

    @staticmethod
    def add_app_data(el, **kwargs):
        app_data = ET.SubElement(el, 'AMS')
        if 'App' not in kwargs:
            kwargs['App'] = "MOD"
        for key in kwargs:
            app_data.set(key, str(kwargs[key]))

    @staticmethod
    def align_country(country):
        return country.upper() if country.lower() != 'en' else 'uk'

    @staticmethod
    def format_date(date):
        """ Return date string on Zulu format"""
        return date.strftime('%Y-%m-%dT%H:%M:%SZ')

    @staticmethod
    def copy_and_checksum(input_path, output_path):

        if not os.path.exists(os.path.dirname(output_path)):
            Common.log(f"Creating: {os.path.dirname(output_path)}")
            os.makedirs(os.path.dirname(output_path))

        start = time.time()
        Common.log(f"Copying '{input_path}' => '{output_path}'...")

        exitcode = subprocess.call(["rsync", "-rtv", '--progress', input_path, output_path])
        if exitcode != 0:
            raise Exception(f"Failed to copy media '{input_path}' to '{output_path}', check permissions!")

        Common.log(f"Copy took {time.time() - start:.2f}s")

        # Run MD5 checksum on the output file by running subprocess and then parsing the output
        start = time.time()
        Common.log(f"Calculating checksum for '{output_path}'...")
        md5sum = subprocess.check_output(["md5sum", output_path]).decode("utf-8").split()[0]

        if not md5sum:
            raise Exception(f"Failed to calculate checksum for '{output_path}'!")

        Common.log(f"Checksum for '{output_path}': {md5sum} (took {time.time() - start:.2f}s)")

        return md5sum

    def fetch_path_from_deps(self, path):
        if not path:
            return None
        dependencies = self.get_compute().get('dependencies', [])
        for dep in dependencies:
            if dep.lower().endswith(path.lower()):
                return self.normalize_path(dep)
        return None

    @staticmethod
    def add_media_asset(el, asset_class, asset_type, asset_name, checksum, **kwargs):
        Common.info(f"Adding asset {asset_name}(class: {asset_class}, type: {asset_type}) to metadata...")
        asset = ET.SubElement(el, 'Asset')
        metadata = ET.SubElement(asset, 'Metadata')
        ams = ET.SubElement(metadata, 'AMS')
        ams.set('Asset_Class', asset_class)
        Engine.add_app_data(metadata, Name="Type", App="MOD", Value=asset_type)
        Engine.add_app_data(metadata, Name="Content_CheckSum", App="MOD", Value=checksum)
        for key in kwargs:
            Engine.add_app_data(metadata, App="MOD", Name=key, Value=kwargs[key])

        content = ET.SubElement(metadata, 'Content')
        content.set("Value", asset_name)

    def _execute(self, item, additional_envs=None):

        # Create the root element
        root = ET.Element('ADI')

        if "parameters" not in self.get_compute():
            raise Exception("No parameters for engine")

        parameters = self.get_compute()["parameters"]

        if not parameters.get("customer", {}):
            raise Exception("No customer specified")
        customer = parameters["customer"]

        if not parameters.get("title", {}):
            raise Exception("No title specified")
        title = parameters["title"]

        input_path = self.normalize_path(self.get_input())
        if not os.path.exists(input_path):
            raise Exception("Input media not found @ {}!".format(input_path))

        if not self.get_compute().get("output", ""):
            raise Exception("No media output provided!")
        output_path = self.get_compute()["output"]

        Common.log(f"Exporting title {title['name']}, media {self.get_input()} for customer '{customer}' using Allente exporter > {output_path}...")

        # Create package/customer definition
        metadata = ET.SubElement(root, 'Metadata')

        ams = ET.SubElement(metadata, 'AMS')
        ams.set('Asset_Class', 'package')
        asset_id = str(parameters.get('sequence', 1))
        ams.set('Asset_ID', asset_id)
        product = 'SVOD'
        if len(parameters.get("export_type","")) > 0:
            product = parameters['export_type']
        ams.set('Product', product)
        ams.set('Provider_ID', parameters['workspace'])

        app_data = ET.SubElement(metadata, 'App_Data')
        app_data.set('App', 'MOD')
        app_data.set('Name', 'Metadata_Spec_Version')
        app_data.set('Value', 'CableLabsVOD1.1')

        # Create title definition

        asset = ET.SubElement(root, 'Asset')

        # Create asset metadata element
        metadata = ET.SubElement(asset, 'Metadata')

        # Create the ams metadata element
        ams = ET.SubElement(metadata, 'AMS')
        ams.set('Asset_Class', 'title')

        Engine.add_app_data(metadata, Name="Audience", Value=title.get('audience', "General"))
        Engine.add_app_data(metadata, Name="NO_Authority_ID", Value="123456")
        if title.get('imdb', ""):
            Engine.add_app_data(metadata, Name="IMDb_ID", Value=title['imdb'])
        Engine.add_app_data(metadata, Name="Title", Value=title['name'], Language="eng")
        if title.get('short_summary', ""):
            Engine.add_app_data(metadata, Name="Summary_Short", Value=title['short_summary'], Language="eng")
        if title.get('long_summary', ""):
            Engine.add_app_data(metadata, Name="Summary_Long", Value=title['long_summary'], Language="eng")
        if title.get('directors', ""):
            for director in title['directors'].split("\n"):
                Engine.add_app_data(metadata, Name="Director", Value=director)
        if title.get('actors', ""):
            for actor in title['actors'].split("\n"):
                Engine.add_app_data(metadata, Name="Actors", Value=actor)

        country = "en"
        if title.get('country', {}):
            country = title['country']['short']
        Engine.add_app_data(metadata, Name="Country_of_Origin", Value=Engine.align_country(country))
        if title.get('year', ""):
            Engine.add_app_data(metadata, Name="Year", Value=title['year'])
        if title.get('run_time', ""):
            Engine.add_app_data(metadata, Name="Run_Time", Value=title['run_time'])
        if title.get('rating', ""):
            Engine.add_app_data(metadata, Name="Rating", Value=title['rating'], Region="UK")
        if title.get('genre', ""):
            Engine.add_app_data(metadata, Name="Genre", Value=title['genre'])
        if title.get('keywords', ""):
            for keyword in title['keywords'].split("\n"):
                Engine.add_app_data(metadata, Name="Keyword", Value=keyword)

        # Provide region specific title metadata
        for region in parameters.get("regions", []):
            region_code = region['region']['value'].lower()
            region_code_short = region['region']['short'].upper()
            if region.get('title', ""):
                Engine.add_app_data(metadata, Name="Title", Value=region['title'], Language=region_code)
            if region.get('short_summary', ""):
                Engine.add_app_data(metadata, Name="Summary_Short", Value=region['short_summary'], Language=region_code)
            if region.get('long_summary', ""):
                Engine.add_app_data(metadata, Name="Summary_Long", Value=region['long_summary'], Language=region_code)
            if region.get('rating', ""):
                Engine.add_app_data(metadata, Name="Rating", Value=region['rating'], Region=region_code_short)
            if region.get('lic_start', ""):
                Engine.add_app_data(metadata, Name="Licensing_Window_Start", Value=Engine.format_date(region['lic_start']), Region=region_code_short)
            if region.get('lic_end', ""):
                Engine.add_app_data(metadata, Name="Licensing_Window_End", Value=Engine.format_date(region['lic_end']), Region=region_code_short)
            if region.get('est_lic_start', ""):
                Engine.add_app_data(metadata, Name="EST_Licensing_Window_Start", Value=Engine.format_date(region['lic_start']), Region=region_code_short)
            if region.get('est_lic_end', ""):
                Engine.add_app_data(metadata, Name="EST_Licensing_Window_End", Value=Engine.format_date(region['lic_end']), Region=region_code_short)
            if region.get('price', ""):
                Engine.add_app_data(metadata, Name="EST_Suggested_Price", Value=region['price'], Region=region_code_short)

        # Copy and checksum main media

        media_output_path = os.path.join(output_path, f"{title['code']}_movie{os.path.splitext(input_path)[1]}")
        checksum = Engine.copy_and_checksum(input_path, media_output_path)
        Engine.add_media_asset(root, "movie", "movie", os.path.basename(media_output_path), checksum)

        if parameters.get('trailer'):
            trailer_path = self.fetch_path_from_deps(parameters['trailer']['path'])
            if not trailer_path:
                raise Exception("Trailer media dependency not provided!")
            if not os.path.exists(trailer_path):
                raise Exception(f"Trailer media dependency not found @ {trailer_path}!")

            trailer_output_path = os.path.join(output_path, f"{title['code']}_preview{os.path.splitext(trailer_path)[1]}")
            checksum = Engine.copy_and_checksum(trailer_path, trailer_output_path)
            Engine.add_media_asset(root, "preview", "preview", os.path.basename(trailer_output_path), checksum)

        # Provide region specific content
        for region in parameters.get("regions", []):
            region_code = region['region']['value'].lower()
            region_code_short = region['region']['short'].upper()

            Common.log(f"Processing region {region_code} content...")

            if region.get('subtitles'):
                subtitles_path = self.fetch_path_from_deps(region['subtitles']['path'])
                if not subtitles_path:
                    Common.warning("Subtitles media dependency not provided!")
                elif not os.path.exists(subtitles_path):
                    Common.warning(f"Subtitles media dependency not found @ {subtitles_path}!")
                else:
                    subtitles_output_path = os.path.join(output_path, f"{title['code']}_{region_code}{os.path.splitext(subtitles_path)[1]}")
                    checksum = Engine.copy_and_checksum(subtitles_path, subtitles_output_path)
                    Engine.add_media_asset(root, "subtitle", "movie", os.path.basename(subtitles_output_path), checksum, Language=region_code)
            else:
                Common.warning("No subtitles provided for region")

            if region.get('trailer_subtitles'):
                trailer_subtitles_path = self.fetch_path_from_deps(region['trailer_subtitles']['path'])
                if not trailer_subtitles_path:
                    Common.warning("Trailer subtitles media dependency not provided!")
                elif not os.path.exists(trailer_subtitles_path):
                    Common.warning(f"Trailer subtitles media dependency not found @ {trailer_subtitles_path}!")
                else:
                    trailer_subtitles_output_path = os.path.join(output_path, f"{title['code']}_preview_{region_code}{os.path.splitext(trailer_subtitles_path)[1]}")
                    checksum = Engine.copy_and_checksum(trailer_subtitles_path, trailer_subtitles_output_path)
                    Engine.add_media_asset(root, "subtitle", "preview", os.path.basename(trailer_subtitles_output_path), checksum, Language=region_code)
            else:
                Common.warning("No trailer subtitles provided for region")

            if region.get('poster_portrait'):
                poster_portrait_path = self.fetch_path_from_deps(region['poster_portrait']['path'])
                if not poster_portrait_path:
                    Common.warning("Portrait poster media dependency not provided!")
                elif not os.path.exists(poster_portrait_path):
                    Common.warning(f"Portrait poster media dependency not found @ {poster_portrait_path}!")
                else:
                    poster_portrait_output_path = os.path.join(output_path, f"{title['code']}_movport_{region_code}{os.path.splitext(poster_portrait_path)[1]}")
                    checksum = Engine.copy_and_checksum(poster_portrait_path, poster_portrait_output_path)
                    Engine.add_media_asset(root, "poster", "DTH_MOVIE_PORTRAIT", os.path.basename(poster_portrait_output_path), checksum, Language=region_code)
            else:
                Common.warning("No portrait poster provided for region")

            if region.get('poster_landscape'):
                poster_landscape_path = self.fetch_path_from_deps(region['poster_landscape']['path'])
                if not poster_landscape_path:
                    Common.warning("Landscape poster media dependency not provided!")
                elif not os.path.exists(poster_landscape_path):
                    Common.warning(f"Landscape poster media dependency not found @ {poster_landscape_path}!")
                else:
                    poster_landscape_output_path = os.path.join(output_path, f"{title['code']}_movland_{region_code}{os.path.splitext(poster_landscape_path)[1]}")
                    checksum = Engine.copy_and_checksum(poster_landscape_path, poster_landscape_output_path)
                    Engine.add_media_asset(root, "poster", "DTH_MOVIE_LANDSCAPE", os.path.basename(poster_landscape_output_path), checksum, Language=region_code)
            else:
                Common.warning("No landscape poster provided for region")

            if region.get('poster_shot'):
                poster_shot_path = self.fetch_path_from_deps(region['poster_shot']['path'])
                if not poster_shot_path:
                    Common.warning("Shot poster media dependency not provided!")
                elif not os.path.exists(poster_shot_path):
                    Common.warning(f"Shot poster media dependency not found @ {poster_shot_path}!")
                else:
                    poster_shot_output_path = os.path.join(output_path, f"{title['code']}_movshot_{region_code}{os.path.splitext(poster_shot_path)[1]}")
                    checksum = Engine.copy_and_checksum(poster_shot_path, poster_shot_output_path)
                    Engine.add_media_asset(root, "poster", "DTH_MOVIE_SHOT", os.path.basename(poster_shot_output_path), checksum, Language=region_code)
            else:
                Common.warning("No shot poster provided for region")

        # Write the XML to file
        xml_output_path = os.path.join(output_path, f"{title['code']}_metadata.xml")
        tree = ET.ElementTree(root)

        Common.log(f"Writing metadata to '{xml_output_path}'")
        ET.indent(tree, space="\t", level=0)
        tree.write(xml_output_path, method='xml', encoding='utf-8', xml_declaration=True)

        Common.log("Allente/Canal Digital export done!")


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