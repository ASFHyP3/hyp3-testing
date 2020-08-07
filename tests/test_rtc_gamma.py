import time
from pathlib import Path


def test_golden(helpers, hyp3_session):
    submission_payload = helpers.get_submission_payload(Path(__file__).resolve().parent / 'data' / 'rtc_gamma_golden.json')

    main_response = hyp3_session.post(url='https://hyp3-api.asf.alaska.edu/jobs', json=submission_payload)
    main_response.raise_for_status()

    develop_response = hyp3_session.post(url='https://hyp3-test-api.asf.alaska.edu/jobs', json=submission_payload)
    develop_response.raise_for_status()

    ii = 0
    main_succeeded = develop_succeeded = False
    main_update = develop_update = None
    while (ii := ii + 1) < 60:
        if not main_succeeded:
            main_update = helpers.get_jobs_update(main_response, hyp3_session)
            main_succeeded = helpers.jobs_succeeded(main_update)
        if not develop_succeeded:
            develop_update = helpers.get_jobs_update(develop_response, hyp3_session)
            develop_succeeded = helpers.jobs_succeeded(develop_update)

        if main_succeeded and develop_succeeded:
            break

        time.sleep(60)

    # TODO: download products from main
    # TODO: download products from develop

    # TODO: compare each product
