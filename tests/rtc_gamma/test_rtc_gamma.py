import time


def jobs_succeeded(post_response, hyp3_session):
    pr = post_response.json()
    requests_time = pr['jobs'][0]['request_time']
    params = {
        'start': requests_time,
        'end': requests_time,
        'name': pr['jobs'][0]['name']
    }
    update = hyp3_session.get(post_response.url, json=params)
    update.raise_for_status()

    status = set()
    for job in update.json()['jobs']:
        status.add(job['status_code'])

    if 'FAILED' in status:
        raise Exception('Job failed')

    return {'SUCCEEDED'}.issuperset(status)



def test_golden(golden_jobs, hyp3_session):
    # FIXME: url once customization released to master
    main_response = hyp3_session.post(url='https://hyp3-test-api.asf.alaska.edu/jobs', json=golden_jobs)
    main_response.raise_for_status()

    # FIXME: Remove once customization released to master
    time.sleep(1)

    develop_response = hyp3_session.post(url='https://hyp3-test-api.asf.alaska.edu/jobs', json=golden_jobs)
    develop_response.raise_for_status()

    ii = 0
    main_succeeded = False
    develop_succeeded = False
    while (ii := ii + 1) < 60:
        if not main_succeeded:
            main_succeeded = jobs_succeeded(main_response, hyp3_session)
        if not develop_succeeded:
            develop_succeeded = jobs_succeeded(develop_response, hyp3_session)

        if main_succeeded and develop_succeeded:
            break

        time.sleep(60)

    # TODO: download products from main
    # TODO: download products from develop

    # TODO: compare each product
