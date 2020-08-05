import os
import time


def test_golden(helpers, hyp3_session):
    post_body = helpers.create_jobs_post(os.path.join(os.path.dirname(__file__), 'data', 'rtc_gamma_golden.json'))

    # FIXME: url once customization released to master
    main_response = hyp3_session.post(url='https://hyp3-test-api.asf.alaska.edu/jobs', json=post_body)
    main_response.raise_for_status()

    # FIXME: Remove once customization released to master
    time.sleep(1)

    develop_response = hyp3_session.post(url='https://hyp3-test-api.asf.alaska.edu/jobs', json=post_body)
    develop_response.raise_for_status()

    ii = 0
    main_succeeded = False
    develop_succeeded = False
    while (ii := ii + 1) < 60:
        if not main_succeeded:
            main_succeeded = helpers.jobs_succeeded(main_response, hyp3_session)
        if not develop_succeeded:
            develop_succeeded = helpers.jobs_succeeded(develop_response, hyp3_session)

        if main_succeeded and develop_succeeded:
            break

        time.sleep(60)

    # TODO: download products from main
    # TODO: download products from develop

    # TODO: compare each product
