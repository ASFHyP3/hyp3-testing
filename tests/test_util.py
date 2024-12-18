import pytest
from jinja2.exceptions import UndefinedError

from hyp3_testing import util


def test_get_job_name():
    name1 = util.generate_job_name()
    name2 = util.generate_job_name()

    assert name1 != name2


def test_render_template():
    with pytest.raises(UndefinedError):
        util.render_template('insar_gamma_golden.json.j2')

    util.render_template('insar_gamma_golden.json.j2', name='test')
