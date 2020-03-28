import requests
import json
import pandas as pd
import argparse

DATASETS = {
    'states_historical_data': {
        'url': 'https://covidtracking.com/api/states/daily'
        },
    'states_information': {
        'url': 'https://covidtracking.com/api/states/info'
        },
    'us_historical_data': {
        'url': 'https://covidtracking.com/api/us/daily'
        },
    'counties': {
        'url': 'https://covidtracking.com/api/counties'
        },
    'trackers_url': {
        'url': 'https://covidtracking.com/api/urls'
        },
    'press': {
        'url': 'https://covidtracking.com/api/urls'
        }
}

def get_argparser():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-o', '--output', required=True,
        help='Destination file to store the processed dataset.')

    return parser

def load_dataset(dataset_name):
    data_raw = requests.get(DATASETS[dataset_name]['url'])
    data_json = json.loads(data_raw.text)
    return pd.DataFrame(data_json)

def main():
    parser = get_argparser()
    args = parser.parse_args()
    
    df_states_hist_data = load_dataset('states_historical_data')
    df_states_info = load_dataset('states_information')
    df_us_hist_data = load_dataset('us_historical_data')
    df_counties = load_dataset('counties')
    df_trackers_url = load_dataset('trackers_url')
    df_press = load_dataset('press')
    
    df_states_hist_data.to_csv(f"{args.output}/states_hist_data.csv",
                               index=False, header=True)

if __name__ == "__main__":
    main()

"""
/api/states - States Current Values
These numbers are updated periodically throughout the day.

/api/states | CSV

state - State or territory postal code abbreviation.
positive - Total cumulative positive test results.
positiveScore - +1 for reporting positives reliably.
negative - Total cumulative negative test results.
negativeScore - +1 for reporting negatives sometimes.
negativeRegularScore - +1 for reporting negatives reliably.
commercialScore - +1 for reporting all commercial tests.
score - Total reporting quality score.
grade - Letter grade based on score.
totalTestResults - Calculated value (positive + negative) of total test results.
hospitalized - Total cumulative number of people hospitalized.
death - Total cumulative number of people that have died.
dateModified - ISO 8601 date of the time the data was last updated by the state.
dateChecked - ISO 8601 date of the time we last visited their website
total - DEPRECATED Will be removed in the future. (positive + negative + pending). Pending has been an unstable value and should not count in any totals.
/api/states/daily - States Historical Data
Entries saved each day at 4 pm ET.

/api/states/daily | CSV

state - State or territory postal code abbreviation.
positive - Total cumulative positive test results.
positiveIncrease - Increase from the day before.
negative - Total cumulative negative test results.
negativeIncrease - Increase from the day before.
pending - Tests that have been submitted to a lab but no results have been reported yet.
totalTestResults - Calculated value (positive + negative) of total test results.
totalTestResultsIncrease - Increase from the day before.
hospitalized - Total cumulative number of people hospitalized.
hospitalizedIncrease - Increase from the day before.
death - Total cumulative number of people that have died.
deathIncrease - Increase from the day before.
dateChecked - ISO 8601 date of the time we saved visited their website
total - DEPRECATED Will be removed in the future. (positive + negative + pending). Pending has been an unstable value and should not count in any totals.

/api/states/info - States Information
/api/states/info | CSV

state - State or territory postal code abbreviation.
name - Full state or territory name.
fips - Federal Information Processing Standard state code
covid19Site - Webpage dedicated to making results available to the public. More likely to contain numbers. We make regular screenshots of this URL.
covid19SiteSecondary - Typically more informational.
twitter - Twitter for the State Health Department
pui - Person Under Investigation; it is meant to capture positive, negative, and pending test results.
pum - Person Under Monitoring; we don’t collect these numbers as they are reported far less consistently across states
notes - Notes about the information available and how we collect or record it.
/api/us - US Current Values
These numbers are updated periodically throughout the day.

/api/us | CSV

positive - Total cumulative positive test results.
negative - Total cumulative negative test results.
totalTestResults - Calculated value (positive + negative) of total test results.
hospitalized - Total cumulative number of people hospitalized.
death - Total cumulative number of people that have died.
posNeg - DEPRECATED Renamed to totalTestResults.
total - DEPRECATED Will be removed in the future. (positive + negative + pending). Pending has been an unstable value and should not count in any totals.
/api/us/daily - US Historical Data
Entries saved each day at 4 pm ET.

/api/us/daily | CSV

dateChecked - ISO 8601 date of when these values were valid.
states - Quantity of states and territories that are reporting data.
positive - Total cumulative positive test results.
positiveIncrease - Increase from the day before.
negative - Total cumulative negative test results.
negativeIncrease - Increase from the day before.
hospitalized - Total cumulative number of people hospitalized.
hospitalizedIncrease - Increase from the day before.
death - Total cumulative number of people that have died.
deathIncrease - Increase from the day before.
pending - Tests that have been submitted to a lab but no results have been reported yet.
totalTestResults - Calculated value (positive + negative) of total test results.
totalTestResultsIncrease - Increase from the day before.
posNeg - DEPRECATED Renamed to totalTestResults.
total - DEPRECATED Will be removed in the future. (positive + negative + pending). Pending has been an unstable value and should not count in any totals.

/api/urls - Tracker URLs
/api/urls

name: State Name
stateId: State or territory postal code abbreviation.
url: String
kind: String
filter: String
headers: Object
navigate: String
options: Object
ssl_no_verify: Boolean
/api/cdc/daily - CDC Reported Tests
/api/cdc/daily | CSV

The table found on the CDC Testing in U.S. webpage is available via this endpoint. Please visit the CDC to learn more. Source

/api/cdc/daily | CSV

dateCollected
cdcLabs
usPubHealthLabs
dailyTotal
lag
"""
