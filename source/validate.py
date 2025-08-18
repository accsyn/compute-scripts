"""

    File/media validation accsyn compute engine script.

    Calculates checksum of input file/media using specified method. Validates the checksum were applicable (PKL/DCP).

    Changelog:

        * v1r1; [Henrik Norin, 25.08.12] Initial version

    This software is provided "as is" - the author and distributor can not be held
    responsible for any damage caused by executing this script in any means.

    Author: Henrik Norin, HDR AB

"""
import traceback
from functools import partial
import sys
import os
import re
import base64
import hashlib
import time
from pathlib import Path
import xml.etree.ElementTree as ET

VALIDATION_METHOD_CHECKSUM_MD5 = "checksum-md5"
VALIDATION_METHOD_CHECKSUM_SHA1 = "checksum-sha1"
VALIDATION_METHOD_CHECKSUM_SHA256 = "checksum-sha256"
VALIDATION_METHOD_PKL_VERIFY = "pkl-verify"


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
    """Accsyn compute engine for validating files/media."""

    __revision__ = 1  # Increment this after each update

    # Engine configuration
    # IMPORTANT NOTE:
    #   This section defines engine behaviour and should not be refactored or moved
    #   away from the enclosing START/END markers. Read into memory by backend at
    #   launch and publish of new engine.
    # -- ENGINE CONFIG START --

    SETTINGS = {
        "items": False,
        "multiple_inputs": True,
        "type": "validation",
        "color": "78,78,78",
        "vendor": ""
    }

    PARAMETERS = {"method": "checksum-md5", "input_conversion": "never"}

    ENVS = {}

    # -- ENGINE CONFIG END --

    def __init__(self, argv):
        super(Engine, self).__init__(argv)
        self._working_path = None
        # Global counters
        self._total_bytes_processed = 0
        self._total_files_processed = 0
        self._largest_pkl_asset_checksum = None
        self._largest_pkl_asset_size = -1


    @staticmethod
    def get_path_version_name():
        p = os.path.realpath(__file__)
        parent = os.path.dirname(p)
        return os.path.dirname(parent), os.path.basename(parent), os.path.splitext(os.path.basename(p))[0]

    @staticmethod
    def usage():
        (unused_cp, cv, cn) = Common.get_path_version_name()
        (unused_p, v, n) = Engine.get_path_version_name()
        Common.log(
            '   accsyn compute engine "{}" v{}-{}(common: v{}-{}) '.format(n, v, Engine.__revision__, cv,
                                                                           Common.__revision__)
        )
        Common.log('')
        Common.log('   Usage: python %s {--probe|<path_json_data>}' % n)
        Common.log('')
        Common.log('       --probe           Have engine check if it is found and' ' of correct version.')
        Common.log('')
        Common.log(
            '       <path_json_data>  Execute engine on data provided in '
            'the JSON and ACCSYN_xxx environment variables.'
        )
        Common.log('')

    # Standard checksum calculation methods

    def calculate_checksum(self, method, item, input_path):
        """Calculate and print *input_file* checksum using *method* (md5, sha1, sha256). Prints % progress every 10s."""
        hash_funcs = {
            VALIDATION_METHOD_CHECKSUM_MD5: hashlib.md5,
            VALIDATION_METHOD_CHECKSUM_SHA1: hashlib.sha1,
            VALIDATION_METHOD_CHECKSUM_SHA256: hashlib.sha256,
        }

        if method not in hash_funcs:
            raise Exception("Unsupported checksum method: {}".format(method))

        if not os.path.isfile(input_path):
            Common.log(f"[ERROR] Input file not found: {input_path}")
            return False

        file_size = os.path.getsize(input_path)
        hasher = hash_funcs[method]()
        chunk_size = 1024 * 1024  # 1MB
        read_bytes = 0
        start_time = time.time()
        last_print = time.time()

        algorithm = method.split('-')[-1]
        Common.log(f"Calculating {algorithm} checksum for {input_path}(task: {item}) using {chunk_size}b chunks.")

        with open(input_path, "rb") as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                hasher.update(chunk)
                read_bytes += len(chunk)
                now = time.time()
                if now - last_print >= 10:
                    percent = (read_bytes / file_size) * 100 if file_size else 0
                    Common.log(f"Progress: {percent:.2f}%")
                    last_print = now

        checksum = hasher.hexdigest()
        Common.log(f"{algorithm.upper()} checksum: {checksum}")
        # Also output checksum to stdout for further processing
        print("""{"taskchecksum":true,"uri":"%s","algorithm":"%s","result":"%s"}""" % (item, algorithm, checksum))
        end_time = time.time()
        elapsed = end_time - start_time
        speed = file_size / elapsed if elapsed > 0 else 0
        Common.log("Checksum calculation time: {:.2f} seconds, speed: {:.2f} bytes/sec".format(elapsed, speed))

        return True

    # PKL verification method

    def localname(self, tag: str):
        if "}" in tag:
            return tag.rsplit("}", 1)[1]
        return tag

    def parse_xml(self, path: Path):
        try:
            return ET.parse(path).getroot()
        except ET.ParseError as e:
            Common.log(f"*** XML parse error *** {path} : {e}")
            return None

    def find_assetmap_for_dir(self, d: Path):
        for p in d.iterdir():
            if p.is_file() and re.search(r"assetmap", p.name, flags=re.IGNORECASE):
                return p
        for p in d.glob("*.xml"):
            root = self.parse_xml(p)
            if root is not None and self.localname(root.tag).lower() in ("assetmap", "assetmap2"):
                return p
        return None

    def build_assetmap_id_to_path(self, assetmap_path: Path):
        mapping = {}
        root = self.parse_xml(assetmap_path)
        if root is None:
            return mapping
        for asset in root.iter():
            if self.localname(asset.tag).lower() != "asset":
                continue
            _id = None
            _path = None
            for child in asset.iter():
                lname = self.localname(child.tag).lower()
                if lname == "id" and _id is None and (child.text or "").strip():
                    _id = child.text.strip()
                elif lname == "path" and _path is None and (child.text or "").strip():
                    _path = child.text.strip()
            if _id and _path:
                mapping[_id] = _path
        return mapping

    def calc_sha1_base64(self, path: Path):
        """Calculate SHA-1 digest and return as Base64 string."""
        h = hashlib.sha1()
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                h.update(chunk)
        return base64.b64encode(h.digest()).decode("ascii")

    def verify_asset(self, currentdir: Path, originalfilename: str, asset_id: str, size: int,
                     expected_hash: str, assetmap_index: dict[str, str]):

        if not originalfilename:
            rel = assetmap_index.get(asset_id)
            if rel:
                originalfilename = rel
        if not originalfilename:
            Common.log(f"[ERROR] *** WARNING MISSING FILENAME FOR ASSET {asset_id} ***")
            return 1

        candidate = (currentdir / originalfilename).resolve()
        if not candidate.exists():
            alt = (currentdir.parent / originalfilename).resolve()
            if alt.exists():
                candidate = alt

        if not candidate.exists():
            Common.log(f"[ERROR] *** WARNING MISSING FILE *** {originalfilename} *** WARNING MISSING FILE ***")
            return 1

        if size is not None:
            actual_size = candidate.stat().st_size
            if actual_size != size:
                Common.log(f"[ERROR] *** WARNING INCORRECT SIZE *** {candidate} (expected {size}, got {actual_size}) ***")
                return 1

        # Count for stats
        self._total_files_processed += 1
        self._total_bytes_processed += candidate.stat().st_size

        Common.log(f"File {candidate.name} is: ")
        actual = self.calc_sha1_base64(candidate)
        if actual != expected_hash:
            Common.log(f"NOT OK")
            Common.log(f"Hash from PKL:   {expected_hash}")
            Common.log(f"Calculated hash: {actual}")
            return 1
        else:
            Common.log(f"OK ")
            if self._largest_pkl_asset_size == -1 or candidate.stat().st_size > self._largest_pkl_asset_size:
                self._largest_pkl_asset_checksum = actual
                self._largest_pkl_asset_size = candidate.stat().st_size
            return 0

    def read_pkl_and_verify(self, pkl_path: Path):
        currentdir = pkl_path.parent
        Common.log(f"\nChecking PKL: {pkl_path.name}")
        root = self.parse_xml(pkl_path)
        if root is None:
            return 1
        assetmap_path = self.find_assetmap_for_dir(currentdir)
        assetmap_index = self.build_assetmap_id_to_path(assetmap_path) if assetmap_path else {}
        errors = 0
        annotation_shown = False
        for asset in root.iter():
            asset_tag_name = self.localname(asset.tag)
            if asset_tag_name.lower() != "asset":
                Common.info(f"Skipping {asset_tag_name} - not an asset")
                continue
            asset_id = None
            expected_hash = None
            size = None
            originalfilename = None
            for child in asset:
                lname = self.localname(child.tag).lower()
                text = (child.text or "").strip()
                if not text:
                    continue
                if lname == "annotationtext" and not annotation_shown:
                    Common.log(f"   Annotation: {text}")
                    #annotation_shown = True
                elif lname == "id":
                    asset_id = text
                elif lname == "hash":
                    expected_hash = text
                elif lname == "size":
                    try:
                        size = int(text)
                    except ValueError:
                        size = None
                elif lname == "originalfilename":
                    originalfilename = text
            if expected_hash:
                errors += self.verify_asset(
                    currentdir=currentdir,
                    originalfilename=originalfilename,
                    asset_id=asset_id or "",
                    size=size,
                    expected_hash=expected_hash,
                    assetmap_index=assetmap_index,
                )
            else:
                Common.info(f"   Skipping asset {asset_tag_name} - no hash!")
        return errors

    def is_pkl(self, xml_path: Path):
        root = self.parse_xml(xml_path)
        if root is None:
            return False
        return self.localname(root.tag).lower() == "packinglist"

    def human_size(self, num_bytes):
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if num_bytes < 1024:
                return f"{num_bytes:.2f} {unit}"
            num_bytes /= 1024
        return f"{num_bytes:.2f} PB"

    def verify_pkl(self, item, input_path):

        root_dir = Path(input_path)
        if not root_dir.is_dir():
            Common.log(f"*** Input path is not a directory: {input_path} ***")
            return False

        Common.log("Checking PKL xml:s in {0}".format(str(root_dir)[str(root_dir).find("/storage/")+1:]))

        pkl_files: list[Path] = []
        for p in root_dir.rglob("*.xml"):
            try:
                if self.is_pkl(p):
                    pkl_files.append(p)
            except Exception:
                continue

        start_time = time.time()

        total_errors = 0
        for pkl in pkl_files:
            total_errors += self.read_pkl_and_verify(pkl)

        elapsed = time.time() - start_time

        if total_errors > 0:
            Common.log(f"*** Hash check complete with {total_errors} errors ***")
        else:
            Common.log(f"*** Hash check complete with 0 errors ***")
            # Also output most relevant checksum to stdout for further processing
            print("""{"taskchecksum":true,"uri":"%s","algorithm":"sha-1","result":"%s"}""" % (item, self._largest_pkl_asset_checksum))
        
        # Print stats
        if self._total_files_processed > 0:
            speed = self._total_bytes_processed / elapsed if elapsed > 0 else 0
            Common.log(f"Processed {self._total_files_processed} files, "
                  f"{Common.str_file_size(self._total_bytes_processed)} in {elapsed:.2f} seconds @ "
                  f"({Common.str_file_size(speed)}/s)")

        return False if total_errors > 0 else True

    def _execute(self, item, additional_envs=None):
        """(Override) Execute the validation for the given item."""

        if "parameters" not in self.get_compute():
            raise Exception("No parameters for engine")

        parameters = self.get_compute()["parameters"]

        if not parameters.get("method", {}):
            raise Exception("No validation method specified in parameters")
        method = parameters["method"]

        if method not in [VALIDATION_METHOD_CHECKSUM_MD5, VALIDATION_METHOD_CHECKSUM_SHA1,
                          VALIDATION_METHOD_CHECKSUM_SHA256, VALIDATION_METHOD_PKL_VERIFY]:
            raise Exception(f"Unsupported validation method: {method}")

        input_path = self.get_input()

        if not os.path.exists(input_path):
            Common.log("[WARNING] Input file not found @ {}!".format(input_path))

        validators = {
            VALIDATION_METHOD_CHECKSUM_MD5: partial(self.calculate_checksum, VALIDATION_METHOD_CHECKSUM_MD5, item),
            VALIDATION_METHOD_CHECKSUM_SHA1: partial(self.calculate_checksum, VALIDATION_METHOD_CHECKSUM_SHA1, item),
            VALIDATION_METHOD_CHECKSUM_SHA256: partial(self.calculate_checksum, VALIDATION_METHOD_CHECKSUM_SHA256, item),
            VALIDATION_METHOD_PKL_VERIFY: partial(self.verify_pkl, item),
        }
        if method not in validators:
            raise Exception(f"Validation method '{method}' is not implemented!")

        validator = validators[method]
        if not validator(input_path):
            raise Exception(f"Validation failed for {input_path} using method {method}")


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
