import os
import json
import random
import string

import pytest
import requests
from jinja2 import Template

AUTH_URL = 'https://urs.earthdata.nasa.gov/oauth/authorize' \
           '?response_type=code&client_id=BO_n7nTIlMljdvU6kRRB3g' \
           '&redirect_uri=https://auth.asf.alaska.edu/login'


@pytest.fixture()
def golden_jobs():
    name = ''.join(random.choices(string.ascii_letters + string.digits, k=7))
    with open(os.path.join(os.path.dirname(__file__), 'golden.json')) as f:
        template = f.read()

    jobs_template = Template(template)
    jobs = jobs_template.render(name=name)

    return json.loads(jobs)


@pytest.fixture()
def hyp3_session():
    session = requests.Session()
    resp = session.get(AUTH_URL)
    resp.raise_for_status()
    return session
