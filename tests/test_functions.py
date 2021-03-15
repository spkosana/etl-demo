from code.helper.functions import get_date_formats, generate_data


def test_get_date_formats():
    """
    test : validating the date formates are populating as per the function 
    """
    actual = get_date_formats(
        start_date="2020-01-01", end_date="2020-02-01", freq="MS", sep="_"
    )
    expected = ["2020_01_01", "2020_02_01"]
    assert actual == expected


def test_generate_data():
    """
    test : validating the generate function is populating data as expected 
    """
    n = 2
    date_of_service = "2020-01-01"
    actual_data = generate_data(n=n, date_of_service=date_of_service)
    actual_len = len(actual_data[list(actual_data.keys())[0]])
    expected_len = n
    assert actual_len == expected_len
