"""
Wrapper script to use when running Python packages via egg file through spark-submit.

Rename this script to the fully qualified package and module name you want to run.
The module should provide a ``main`` function.

Pass any additional arguments to the script.

Usage:

  spark-submit --py-files <LIST-OF-EGGS> <PACKAGE>.<MODULE>.py <MODULE_ARGS>
"""
import os
import importlib


def main():
    filename = os.path.basename(__file__)
    module = os.path.splitext(filename)[0]
    module = importlib.import_module(module)
    module.main()


if __name__ == '__main__':
    main()