import json
import random
import string

from jinja2 import Environment, PackageLoader, StrictUndefined, select_autoescape


def get_job_name() -> str:
    hash_ = ''.join(random.choices(string.ascii_letters + string.digits, k=7))
    return f'hyp3-testing-{hash_}'


def get_environment() -> Environment:
    env = Environment(
        loader=PackageLoader('hyp3_testing', 'templates'),
        autoescape=select_autoescape(['html.j2', 'xml.j2']),
        undefined=StrictUndefined,
        trim_blocks=True,
        lstrip_blocks=True,
        keep_trailing_newline=True,
    )
    return env


def render_template(template: str, **kwargs) -> dict:
    env = get_environment()
    template = env.get_template(template)
    rendered = template.render(**kwargs)
    return json.loads(rendered)
