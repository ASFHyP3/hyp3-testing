"""HyP3 plugin for testing processing"""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version(__name__)
except PackageNotFoundError:
    print('package is not installed!\n'
          'Install in editable/develop mode via (from the top of this repo):\n'
          '   pip install -e .\n'
          'Or, to just get the version number use:\n'
          '   python setup.py --version')
