import scrapy
from bs4 import BeautifulSoup
import logging
import dateutil.parser
import datetime
import re

from ..items import MoviesItem
from scrapy.exceptions import CloseSpider


class BoxOfficeSpider(scrapy.Spider):
    name = "boxoffice"
    start_urls = [
        'https://www.boxofficemojo.com/title/',
    ]
    handle_httpstatus_list = [404, 503]
    consecutive_err = 0
    custom_settings = {'JOBDIR': 'boxoffice_mojo'}  # persistence of resumed job

    def __init__(self, *args, **kwargs):
        super(BoxOfficeSpider, self).__init__(*args, **kwargs)
        self.imdb_ids = kwargs.pop('imdb_ids', [])

    def start_requests(self):
        for imdb_id in self.imdb_ids:
            imdb_id = str(imdb_id)
            url = self.start_urls[0] + imdb_id + '/credits/'
            yield scrapy.Request(url=url, callback=self.parse, meta={'imdb_id': imdb_id})

    def parse(self, response):
        self.logger.info(f'Now parsing: {response.request.url}')
        if response.status == 200:
            self.consecutive_err = 0
        elif response.status in self.handle_httpstatus_list:
            self.consecutive_err += 1
            if self.consecutive_err == 10:
                # stop spider on condition
                raise CloseSpider('NUmber of error received from server has exceed')
            return None

        soup = BeautifulSoup(response.text, 'html.parser')
        try:
            general_info = soup.find_all('div', {'class': 'a-section a-spacing-none'})
            # self.logger.info(general_info)
            general_info = [mon.get_text('@', strip=True).replace('(', '').replace(')', '').split('@') for mon in
                            general_info]
            self.logger.info(general_info)

            # get Title, year and tagline
            title_year_tagline = general_info[0]
            for i in range(0, 3):
                if i >= len(title_year_tagline):
                    title_year_tagline.append(None)

            title = title_year_tagline[0]
            year = title_year_tagline[1]
            # sometimes tagline my be divided with other html tags, so merge it
            tagline = title_year_tagline[2]
            if tagline and len(title_year_tagline) > 3:
                tagline = ''.join([_tagline for _tagline in title_year_tagline[2:] if _tagline is not None])

            # get money
            domestic = BoxOfficeSpider._find_money(general_info, 'Domestic ')
            international = BoxOfficeSpider._find_money(general_info, 'International ')
            worldwide = BoxOfficeSpider._find_money(general_info, 'Worldwide')
            budget = BoxOfficeSpider._find_money(general_info, 'Budget')

            # get picture rating(mpaa), runtime and genre
            distributor = BoxOfficeSpider._find_info(general_info, 'Domestic Distributor')
            release_date = BoxOfficeSpider._find_info(general_info, 'Earliest Release Date')
            if release_date:  # convert human readable date format to standard format
                release_date = release_date.split('\n')[0]
                release_date = dateutil.parser.parse(release_date).strftime('%Y-%m-%d')

            # mpaa = BoxOfficeSpider._find_info(general_info, 'MPAA')
            run_time = BoxOfficeSpider._find_info(general_info, 'Running Time')
            if run_time:  # convert duration to minutes
                run_time = BoxOfficeSpider._convert_runtime(run_time)

            genres = BoxOfficeSpider._find_info(general_info, 'Genres')
            if genres:
                genres = genres.replace('\n', '').split()
                genres = ', '.join(genres)

            # get crews
            writer, director, producer, composer, cinematographer = None, None, None, None, None
            crews = soup.find('table', {"id": "principalCrew"})
            if crews:
                crews = BoxOfficeSpider._table_data_text(crews)
                writer = BoxOfficeSpider._find_crew('Writer', crews)
                director = BoxOfficeSpider._find_crew('Director', crews)
                producer = BoxOfficeSpider._find_crew('Producer', crews)
                composer = BoxOfficeSpider._find_crew('Composer', crews)
                cinematographer = BoxOfficeSpider._find_crew('Cinematographer', crews)

            # get main actors
            casts = soup.find('table', {"id": "principalCast"})
            if casts:
                casts = BoxOfficeSpider._table_data_text(casts)
                casts = ', '.join([actor[0] for actor in casts][1:]) if len(casts) > 0 else None

            # set movies item
            movies_item = MoviesItem()
            movies_item['title'] = title
            movies_item['date'] = release_date
            movies_item['tagline'] = tagline
            movies_item['genres'] = genres
            movies_item['runtime'] = run_time
            movies_item['revenue'] = worldwide
            movies_item['budget'] = budget
            movies_item['director'] = director
            movies_item['production_companies'] = distributor
            movies_item['cast'] = casts
            movies_item['imdb_id'] = response.meta.get('imdb_id')

            yield movies_item

        except Exception as err:
            self.logger.error((err))
            self.logger.error(f'URL:{response.request.url}')
            raise

    @staticmethod
    def _find_money(list_, name):
        output = None
        for sublist in list_:
            if name in sublist and sublist[-1] != '–':
                output = int(sublist[-1].replace("$", "").replace(",", ""))

        return output

    @staticmethod
    def _find_info(list_, name):
        """
        Searches through a list and returns a specific value from
        sublist that contains given name
        """
        output = None
        for sublist in list_:
            if name in sublist:
                if len(sublist) > 1:
                    output = sublist[1]

        return output

    @staticmethod
    def _convert_runtime(runtime):
        """
        Convert human readable duration format to minutes
        returns given string if nothing is found
        """
        regex = re.compile(r'[-]?((?P<hours>\d+?)\s?hr)?\s?[-]?((?P<minutes>\d+?)\s?min)?')
        parts = regex.match(runtime)
        if not parts:
            return runtime
        parts = parts.groupdict()
        time_params = {}
        for (name, param) in parts.items():
            if param:
                time_params[name] = int(param)
        return int(datetime.timedelta(**time_params).total_seconds() / 60)

    @staticmethod
    def _find_crew(role, crews):
        """
        Finds values with given role and concat with comma
        returns None if nothing is found
        """
        output = [crew[0] for crew in crews if len(crew) > 1 and crew[1] == role]
        return ', '.join(output) if output else None

    @staticmethod
    def _table_data_text(table):
        """
        Searches through <tr> (table rows) and inner <td> (table data) tags.
        Returns a list of rows with inner columns.
        """

        def row_get_data_text(tr, coltag='td'):  # td (data) or th (header)
            return [td.get_text(strip=True) for td in tr.find_all(coltag)]

        rows = []
        trs = table.find_all('tr')
        header_row = row_get_data_text(trs[0], 'th')
        if header_row:  # if there is a header row include first
            rows.append(header_row)
            trs = trs[1:]
        for tr in trs:  # for every other table rows
            rows.append(row_get_data_text(tr, 'td'))  # data row

        return rows


