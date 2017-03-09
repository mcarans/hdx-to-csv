import ckanapi
from os.path import join, expanduser
import logging

from flatten_json import flatten_json
from pandas import DataFrame
from pandas.io.json import json_normalize

logger = logging.getLogger(__name__)


def main():
    with open(join(expanduser('~'), '.hdxkey'), 'rt') as f:
        apikey = f.read().replace('\n', '')

        remoteckan = ckanapi.RemoteCKAN('https://data.humdata.org/',
                                        apikey=apikey,
                                        user_agent='hdx-to-csv')
        df = DataFrame()
        start = 0
        total_rows = 10000
        for page in range(total_rows // 1000 + 1):
            data = dict()
            pagetimes1000 = page * 1000
            data['offset'] = start + pagetimes1000
            rows_left = total_rows - pagetimes1000
            rows = min(rows_left, 1000)
            data['limit'] = rows
            result = remoteckan.call_action('current_package_list_with_resources', data)
            if result:
                no_results = len(result)
                #flat = flatten_json(result)
                norm = json_normalize(result)
                df = df.append(norm)
                if no_results < rows:
                    break
            else:
                logger.debug(result)
        df.to_csv('datasets.csv', encoding='utf-8', index=False, date_format='%Y-%m-%d', float_format='%.0f')

if __name__ == "__main__":
    main()