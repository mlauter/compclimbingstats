from __future__ import annotations
from enum import Enum
import requests, datetime, os, csv, getopt, sys
from dateutil import parser

# https://ifsc.egroupware.net/egw/ranking/json.php?comp=1637
# https://ifsc.egroupware.net/egw/ranking/json.php?year=2016
# dev only
# all comps have a WetID, and it appears to be monotonically increasing
# over time, but I'm not 100% sure
#import ipdb

class ScrapeType(Enum):
    INCREMENTAL = 0
    SINGLE      = 1
    FULL        = 2

class Scraper:
    YEAR     = "year"
    WETID    = "wet_id"
    MIN_YEAR = 2014
    BASE_URL = "https://ifsc.egroupware.net/egw/ranking/json.php"

    def __init__(self, data_dir: str) -> Scraper:
        """Scrapes and stores ifsc result data in a local dir"""
        self.data_dir    = data_dir
        self.results_dir = data_dir + "/results"
        self.cursor_f    = data_dir + "/cursor.csv"
        self.cursor      = {self.YEAR: 0, self.WETID: 0}

    def setup(self) -> Scraper:
        """Setup data directory and set cursor for incremental dumps"""
        if not os.path.exists(self.data_dir):
            os.mkdir(self.data_dir)

        if not os.path.exists(self.results_dir):
            os.mkdir(self.results_dir)

        try:
            with open(self.cursor_f, 'r') as fh:
                reader = csv.DictReader(fh, [self.YEAR, self.WETID])
                # read a single line
                for row in reader:
                    self.cursor = row
                    break

        except FileNotFoundError:
            # create the file
            open(self.cursor_f, 'w+').close()

        return self

    def scrape(self, s_type: ScrapeType, comp_id: int=None):
        if s_type == ScrapeType.INCREMENTAL:
            self._dump_incremental_results()
        elif s_type == ScrapeType.FULL:
            self._dump_all_results()
        elif s_type == ScrapeType.SINGLE:
            if comp_id == None:
                raise ValueError('Single comp dump requires a comp_id')

            self._get_one_result(comp_id)
        else:
            raise ValueError('Invalid scrape type {}'.format(s_type))

    def _dump_incremental_results(self):
        print("Beginning incremental dump...\n")
        year = max(self.cursor[self.YEAR], self.MIN_YEAR)
        self._dump_results_from(year, comp_id=self.cursor[self.WETID])

    def _dump_all_results(self):
        """Dump all results as far back as 2014"""
        print("Beginning full dump from {}...\n".format(self.MIN_YEAR))
        self._dump_results_from(self.MIN_YEAR)

    def _dump_results_from(self, year: int, comp_id: int=0):
        """Dump comp results from ifsc starting from given year

        Args:
        year (int): The starting year for dumping results
        comp_id (int, optional): A competition id. If provided,
             start dumping from this competition. Defaults to 0.

        Raises:
        requests.exceptions.HTTPError
        ValueError (invalid json)
        """

        for i in range(year, datetime.date.today().year + 1):
            payload = {'year': i}
            r = requests.get(self.BASE_URL, params=payload)

            # throw if non-200
            r.raise_for_status()

            json = r.json()

            for comp in json['competitions']:
                self._get_one_result(comp['WetId'], comp)

            # update cursor year
            self.cursor[self.YEAR] = i
            self.write_cursor()

    def _get_one_result(self, comp_id: int, meta: dict={}):
        # redo with https://ifsc.egroupware.net/egw/ranking/json.php?comp=7935&cat=6
        # gets full result set
        # first get just the comp, get the cats, then get the full results with the comp+cat request
        print("Dumping comp {}...\n".format(comp_id))
        payload = {'comp': comp_id}
        r = requests.get(self.BASE_URL, params=payload)

        # continue if no results for this comp
        if r.status_code != 200:
            print("No results found for comp {}".format(comp_id))
            return

        json = r.json()
        results = []

        comp_date = parser.parse(json['date']).date()
        # the spelling error is in the response
        for c in json['categorys']:
            # todo actually make incremental by comp? (need to sort)

            cat_id = c['GrpId']
            r2 = requests.get(self.BASE_URL, params={
                'comp': comp_id,
                'cat': cat_id,
            })

            for r in c['results']:
                result = {
                    'comp_id': comp_id,
                    'comp_name': json['name'],
                    'location': meta.get('host_nation', 'UNKNOWN'),
                    'date': comp_date,
                    'cat': c['name'],
                    'cat_id': c['GrpId'],
                    'cat_key': c['rkey'],
                    'rank': r['result_rank'],
                    'lastname': r['lastname'],
                    'firstname': r['firstname'],
                    'birthyear': r['birthyear'],
                    'nationality': r['nation'],
                }

                result = {k:str(v).strip() for k,v in result.items()}
                results.append(result)

        # write results
        filename = '{}_{}.csv'.format(comp_date, comp_id)
        path = os.path.join(self.data_dir, 'results', filename)
        with open(path, 'w+') as fh:
            writer = csv.DictWriter(fh, results[0].keys())
            writer.writeheader()
            writer.writerows(results)

        # update cursor
        self.cursor[self.WETID] = comp_id
        self.write_cursor()

    def write_cursor(self):
        with open(self.cursor_f, 'w') as fh:
            writer = csv.DictWriter(fh, [self.YEAR, self.WETID])
            writer.writerow(self.cursor)

def usage(code: int = 0, msg: string = ''):
    msg = msg + "\n" if msg != '' else msg
    print(
        '''{}Scrape ifsc results to download climbing world cup result data

Usage:
    python {} [-h] [( -c <comp_id> | -f )] [-o <out_dir_path>]

Options:
    -c <comp_id>         Dump results for a particular competition by WetId
    -f                   Dump full results (going back to 2015). If omitted only dump incremental results
    -o <out_dir_path>    Output dir for comp result data (Default: data/ from the directory where the command is run)
    -h | --help          Print this message

'''.format(msg, sys.argv[0]))
    sys.exit(code)

if __name__ == '__main__':
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hc:fo:", ["help"])
    except getopt.GetoptError as err:
        # print help information and exit
        usage(code=1, msg=err)

    data_dir = os.path.abspath('data')
    s_type   = ScrapeType.INCREMENTAL
    comp_id  = None

    for opt, val in opts:
        if opt == '-c':
            if s_type == ScrapeType.FULL:
                usage(code=1, msg='-c (single comp) and -f (full dump) are mutually exclusive')

            s_type  = ScrapeType.SINGLE
            comp_id = val
        elif opt == '-f':
            if s_type == ScrapeType.SINGLE:
                usage(code=1, msg='-c (single comp) and -f (full dump) are mutually exclusive')

            s_type = ScrapeType.FULL
        elif opt == '-o':
            data_dir = os.path.abspath(val)
        elif opt in ('-h', '--help'):
            usage()
        else:
            msg = 'Unrecognized option {}'.format(opt)
            usage(code=1, msg=msg)

    s = Scraper(data_dir).setup().scrape(s_type, comp_id=comp_id)
