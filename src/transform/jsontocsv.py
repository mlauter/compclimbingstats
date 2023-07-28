import argparse
import json
import csv
from pathlib import Path


def transform_file(filename: str, outdir: str):
    """
    Transform json files to csvs
    """
    with open(filename, 'r', encoding='utf8') as datafile:
        raw = datafile.read()

    result_json = json.loads(raw)
    common_cols = {'fullname': result_json['event'], **result_json['metadata']}

    # drop flag_url
    rankings = [{k: v for k, v in d.items() if k != 'flag_url'}
                for d in result_json['ranking']]

    # strip score field and add in common_cols
    rankings = [{**common_cols, **rank, 'score': rank['score'].strip()}
                for rank in rankings]

    headers = rankings[0].keys()
    out_filename = Path(outdir).joinpath(
        Path(filename).stem).with_suffix('.csv')

    with open(out_filename, 'w', encoding='utf8') as output_file:
        dict_writer = csv.DictWriter(output_file, headers)
        dict_writer.writeheader()
        dict_writer.writerows(rankings)


def transform(datadir: str, outdir: str):
    """
    Loop over the json files in the input dir
    """
    for filename in Path(datadir).glob('*.json'):
        transform_file(filename, outdir)


def main():
    """
    Transform ifsc result json to csv
    """

    parser = argparse.ArgumentParser(description='Transform ifsc result json files output by the extract script to csv.',
                                     epilog="Not affiliated with or sanctioned by the ifsc.")
    parser.add_argument('-i', '--inputdir', nargs='?', default=Path.cwd().joinpath(
        'data'), help='The directory where your json files are located.')
    parser.add_argument('-o', '--outdir', nargs='?', default=Path.cwd().joinpath(
        'data'), help='The directory in which to output csvs.')

    args = parser.parse_args()
    transform(args.inputdir, args.outdir)


if __name__ == '__main__':
    main()
