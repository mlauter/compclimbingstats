import os
import json
import argparse
from pathlib import Path

from dotenv import dotenv_values
import requests
import IPython

DEFAULT_MIN_YEAR = 2012
DISCIPLINES = ["lead", "speed", "boulder", "combined", "boulder&lead"]
DEFAULT_DISCIPLINE = "lead"
# This could be an option at some point
LEAGUE = 'World Cups and World Championships'
ROUNDS = ['semi-final', 'final']
BASE_URL = "https://ifsc.results.info"
CONFIG = dotenv_values(".env")


class Extractor:
    def __init__(self, disciplines: list[str], outdir: str, start_year: int):
        """Extracts and stores ifsc result data in a local dir"""
        self.disciplines = disciplines
        self.outdir = outdir
        self.start_year = start_year

    def extract(self):
        """Extract result data from ifsc api and store it in local files"""

        index = self.__v1_api_request('/api/v1')
        index_json = index.json()

        # Filter seasons by year
        filtered_seasons_urls = [season['url'] for season in index_json['seasons']
                                 if int(season['name']) >= self.start_year]

        for url in filtered_seasons_urls:
            self.__extract_season(url)

    def __extract_season(self, season_url: str):
        """Given a season url, extract result data and store in files"""

        season = self.__v1_api_request(season_url)
        season_json = season.json()
        wc_league_season_id = int(
            [l for l in season_json['leagues'] if l['name'] == LEAGUE][0]['url'].split('/')[-1])

        events = season_json['events']

        # Filter to world cups/world champs
        wc_events = [
            event for event in events if event['league_season_id'] == wc_league_season_id]

        # Filter disciplines
        filtered_event_urls = [e['url'] for e in wc_events if any(
            d['kind'] in self.disciplines for d in e['disciplines'])]

        for url in filtered_event_urls:
            self.__extract_event(season_json['name'], url)

    def __extract_event(self, season_name: str, event_url: str):
        """Given an event url, extract result data and store in files"""

        event = self.__v1_api_request(event_url)
        event_json = event.json()

        for dcat in event_json['dcats']:
            # Still need to filter here because the overall event may have irrelevant disciplines
            if dcat['discipline_kind'] not in self.disciplines:
                continue

            cat_name = dcat['category_name'].lower()

            result_urls = [cat['result_url']
                           for cat in dcat['category_rounds'] if (cat['name'].lower() in ROUNDS and cat['status'] == 'finished')]

            for url in result_urls:
                self.__extract_result(
                    url, {'season': season_name, 'loc': event_json['location'], 'discipline': dcat['discipline_kind'], 'category': cat_name})

    def __extract_result(self, result_url: str, metadata):
        """Given a result url, extract result data and store in files"""

        result = self.__v1_api_request(result_url)
        result_json = result.json()
        round_name = result_json['round'].lower()
        result_json['metadata'] = dict(metadata, round=round_name)

        filename = f"{metadata['season']}_{metadata['loc']}_{metadata['discipline']}_{metadata['category']}_{round_name}.json"
        with open(os.path.join(self.outdir, filename), 'w', encoding='utf8') as f:
            json.dump(result_json, f)

    def __v1_api_request(self, path: str):
        """Request a resource from the ifsc results api"""
        return requests.get(BASE_URL + path,
                            headers=json.loads(CONFIG['INFO_API_HEADERS']), timeout=2.0)


def main():
    """Parse arguments and run extraction"""
    parser = argparse.ArgumentParser(description='Extract climbing world cup result data from the ifsc api.',
                                     epilog="Not affiliated with or sanctioned by the ifsc. Note that you will need cookie and other header info from your browser to make successful requests.")
    parser.add_argument('-d', '--discipline', action='append', choices=DISCIPLINES,
                        help='The discipline for which you want to extract results. Pass this option multiple times to select multiple disciplines.')
    parser.add_argument('-o', '--outdir', nargs='?', default=Path.cwd().joinpath(
        'data'), help='The directory in which to output results.')
    parser.add_argument('-y', '--start-year', nargs='?', type=int, default=DEFAULT_MIN_YEAR,
                        help='The first year from which to start collecting results.')

    args = parser.parse_args()
    if args.discipline is None:
        args.discipline = [DEFAULT_DISCIPLINE]

    Extractor(args.discipline, args.outdir, args.start_year).extract()


if __name__ == '__main__':
    main()
