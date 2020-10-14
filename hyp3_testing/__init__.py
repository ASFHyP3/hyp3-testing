"""HyP3 plugin for testing processing"""

from importlib.metadata import PackageNotFoundError, version

API_URL = 'https://hyp3-api.asf.alaska.edu/jobs'
API_TEST_URL = 'https://hyp3-test-api.asf.alaska.edu/jobs'
AUTH_URL = 'https://urs.earthdata.nasa.gov/oauth/authorize?response_type=code' \
           '&client_id=BO_n7nTIlMljdvU6kRRB3g&redirect_uri=https://auth.asf.alaska.edu/login'

try:
    __version__ = version(__name__)
except PackageNotFoundError:
    print('package is not installed!\n'
          'Install in editable/develop mode via (from the top of this repo):\n'
          '   pip install -e .\n'
          'Or, to just get the version number use:\n'
          '   python setup.py --version')
