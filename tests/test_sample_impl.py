import pytest
import datetime
from pyspark.sql import SparkSession

from src.sample_impl import *

AGENCY_EVENTS_PATH = 'tests/data/pp_events'
VENDOR_EVENTS_PATH = 'tests/data/vendor_events'

@pytest.fixture(scope="session")
def spark(request):
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("partOne") \
        .getOrCreate()
    request.addfinalizer(lambda: spark.stop())
    return spark


def test_load_agency_events(spark):
    agency_events = load_agency_events(spark, AGENCY_EVENTS_PATH)
    assert(agency_events.schema == AGENCY_EVENT_SCHEMA)
    assert(agency_events.count() == 1203)


def test_load_vendor_events(spark):
    vendor_events = load_vendor_events(spark, VENDOR_EVENTS_PATH)
    assert(vendor_events.schema == VENDOR_EVENT_SCHEMA)
    assert(vendor_events.count() == 1208)


def _check_enriched_event(enriched_events, expected):
    ppid_matches = (enriched_events
                    .filter(enriched_events.ppid == expected['ppid'])
                    )
    assert(ppid_matches.count() == 1)
    assert(ppid_matches.first().asDict() == expected)


def test_enrich_events(spark):
    enriched_events = enrich_events(spark,
                                    AGENCY_EVENTS_PATH,
                                    VENDOR_EVENTS_PATH)

    # Should conform to desired schema
    assert(enriched_events.schema == ENRICHED_EVENT_SCHEMA)
    # Should produce an EnrichedEvent for each agency event
    assert(enriched_events.count() == 1203)
    expected_enriched_events = [
        # vendor_id from the most recent eligible vendor_event
        {
            'ppid': 6,
            'marker': '6a',
            'vendorId': '6b',
            'ts': datetime.datetime(2018, 1, 2, 0, 0)
        },
        {
            'ppid': 65,
            'marker': '65a',
            'vendorId': '65c',
            'ts': datetime.datetime(2018, 1, 2, 0, 0)
        },
        {
            'ppid': 234,
            'marker': '234a',
            'vendorId': '234b',
            'ts': datetime.datetime(2018, 1, 2, 0, 0)
        },
        {
            'ppid': 1202,
            'marker': '1202a',
            'vendorId': '1202d',
            'ts': datetime.datetime(2018, 2, 8, 0, 0)
        },
        {
            'ppid': 1156,
            'marker': '1156a',
            'vendorId': '1156c',
            'ts': datetime.datetime(2018, 1, 2, 0, 0)
        },
        # no vendorId if one is not available
        {
            'ppid': 1201,
            'marker': '1201a',
            'vendorId': None,
            'ts': datetime.datetime(2018, 1, 1, 0, 0)
        },
        {
            'ppid': 1203,
            'marker': '1203a',
            'vendorId': None,
            'ts': datetime.datetime(2018, 2, 1, 0, 0)
        },
    ]

    for expected in expected_enriched_events:
        _check_enriched_event(enriched_events, expected)
