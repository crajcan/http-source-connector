test:
    bats ./tests/error-stream-test.bats

cloud_e2e_test:
	bats ./tests/cloud-http-get-test.bats
	bats ./tests/cloud-http-post-test.bats
	bats ./tests/cloud-http-get-header-test.bats

test_fluvio_install:
	fluvio version
	fluvio topic list
	fluvio topic create foobar
	sleep 3
	echo foo | fluvio produce foobar
	fluvio consume foobar -B -d
