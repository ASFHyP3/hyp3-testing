import time


def job_succeeded(resp, hyp3_session):
    job = resp.json()
    requests_time = job['request_time']
    params = {
        'start': requests_time,
        'end': requests_time,
    }
    update = hyp3_session.get(resp.url, params=params)
    update.raise_for_status()

    status = update.json()['status_code']
    if status == 'FAILED':
        raise Exception('Job failed')
    return status == 'SUCCEEDED'


def check_if_finished(main_response, develop_response, hyp3_session):
    return job_succeeded(main_response, hyp3_session) and job_succeeded(develop_response, hyp3_session)


def test_golden(golden_jobs, hyp3_session):
    main_response = hyp3_session.post(url='https://hyp3-api.asf.alaska.edu/jobs', json=golden_jobs)
    main_response.raise_for_status()

    develop_response = hyp3_session.post(url='https://hyp3-test-api.asf.alaska.edu/jobs', json=golden_jobs)
    develop_response.raise_for_status()

    ii = 0
    while (ii := ii + 1) < 60 and not check_if_finished(main_response, develop_response, hyp3_session):
        time.sleep(60)

    # TODO: download products from main
    # TODO: download products from develop

    # TODO: compare each product
