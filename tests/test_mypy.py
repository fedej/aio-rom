import os
import os.path
import re
import sys
from pathlib import Path

from mypy import api
from mypy.test.config import test_temp_dir
from mypy.test.data import DataDrivenTestCase, DataSuite
from mypy.test.helpers import assert_string_arrays_equal

current_dir = Path(__file__).parent


class RomDataSuite(DataSuite):
    files = ["mypy-plugin.test"]

    def run_case(self, testcase: DataDrivenTestCase) -> None:
        assert testcase.old_cwd is not None, "test was not properly set up"
        mypy_cmdline = [
            "--show-traceback",
            "--no-silence-site-packages",
            "--no-error-summary",
            f"--config-file={str(current_dir)}/rom.ini",
        ]
        version = sys.version_info[:2]
        mypy_cmdline.append("--python-version={}".format(".".join(map(str, version))))
        program_text = "\n".join(testcase.input)
        flags = re.search("# flags: (.*)$", program_text, flags=re.MULTILINE)
        if flags:
            flag_list = flags.group(1).split()
            mypy_cmdline.extend(flag_list)
        # Write the program to a file.
        program_path = os.path.join(test_temp_dir, "main.py")
        mypy_cmdline.append(program_path)
        with open(program_path, "w") as file:
            for s in testcase.input:
                file.write("{}\n".format(s))
        output = []
        # Type check the program.
        out, err, returncode = api.run(mypy_cmdline)
        # split lines, remove newlines, and remove directory of test case
        for line in (out + err).splitlines():
            if line.startswith(test_temp_dir + os.sep):
                output.append(
                    line[len(test_temp_dir + os.sep) :]
                    .rstrip("\r\n")
                    .replace(".py", "")
                )
            else:
                output.append(line.rstrip("\r\n"))
        # Remove temp file.
        os.remove(program_path)
        assert_string_arrays_equal(
            testcase.output,
            output,
            "Invalid output ({}, line {})".format(testcase.file, testcase.line),
        )
