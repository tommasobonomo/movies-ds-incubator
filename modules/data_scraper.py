from bs4 import BeautifulSoup
import requests
import re
import pandas as pd

def tableDataText(table):
    """Searches through <tr> (table rows) and inner <td> (table data) tags.
    Returns a list of rows with inner columns.
    """
    def rowgetDataText(tr, coltag='td'): # td (data) or th (header)
        return [td.get_text(strip=True) for td in tr.find_all(coltag)]
    rows = []
    trs = table.find_all('tr')
    headerow = rowgetDataText(trs[0], 'th')
    if headerow: # if there is a header row include first
        rows.append(headerow)
        trs = trs[1:]
    for tr in trs: # for every table row
        rows.append(rowgetDataText(tr, 'td') ) # data row
    return rows

def get_essential(role, data):
    '''finds first value with given role
    returns None if nothing is found'''
    output = None
    for row in data:
        if row[1] == role and output == None:
            output = row[0]
        else:
            continue
    return output

def find_money(list_, name):
    '''searches through a list and returns a numerical value from
    sublist that contains given name'''

    output = None
    for sublist in list_:
        if name in sublist and sublist[-1] != '–':
            output = sublist[-1].replace("$", "").replace(",", "")
    return output


def find_info(list_, name):
    '''searches through a list and returns a specific value from
    sublist that contains given name'''
    output = None
    for sublist in list_:
        if name in sublist:
            output = sublist[1]

    return output


def get_bom_data(imdb_codes):
    '''searches through boxofficemojo.com pages and stores essential movie info
    input: imdb movie codes in a format tt0000000
    return: Dataframe with movie info'''

    movies_bom = [[
        'movie_id', 'title', 'year', 'tagline', 'mpaa', 'release_date', 'run_time', 'distributor', 'director',
        'writer', 'producer', 'composer', 'cinematographer', 'main_actor_1', 'main_actor_2', 'main_actor_3',
        'main_actor_4', 'budget', 'domestic', 'international', 'worldwide', 'genre_1',
        'genre_2', 'genre_3', 'genre_4', 'html'
    ]]

    for imdb_code in imdb_codes:

        try:

            movie_id = str(imdb_code)

            html = 'https://www.boxofficemojo.com/title/' + movie_id + '/credits/'
            html_page = requests.get(html)
            soup = BeautifulSoup(html_page.text, 'html.parser')

            general_info = soup.find_all('div', {'class': 'a-section a-spacing-none'})
            general_info = [mon.get_text('@', strip=True).replace('(', '').replace(')', '').split('@') for mon in
                            general_info]

            # get Title year tagline
            title_year_trivia = general_info[0]
            for i in range(0, 3):
                if i >= len(title_year_trivia):
                    title_year_trivia.append(None)

            title = title_year_trivia[0]
            year = title_year_trivia[1]
            tagline = title_year_trivia[2]

            # get money
            domestic = find_money(general_info, 'Domestic ')
            international = find_money(general_info, 'International ')
            worldwide = find_money(general_info, 'Worldwide')
            budget = find_money(general_info, 'Budget')

            # get picture rating(mpaa), runtime and genre
            distributor = find_info(general_info, 'Domestic Distributor')
            release_date = find_info(general_info, 'Earliest Release Date')
            if release_date:
                release_date = release_date.split(',')
                release_date = release_date[0]

            mpaa = find_info(general_info, 'MPAA')
            run_time = find_info(general_info, 'Running Time')
            genres = find_info(general_info, 'Genres')

            if genres:
                genres = genres.replace('\n', '').split()
            else:
                genres = []

            for i in range(0, 4):
                if i >= len(genres):
                    genres.append(None)

            genre_1 = genres[0]
            genre_2 = genres[1]
            genre_3 = genres[2]
            genre_4 = genres[3]

            # get crew
            crew = soup.find('table', {"id": "principalCrew"})
            essential = tableDataText(crew)
            writer = get_essential('Writer', essential)
            director = get_essential('Director', essential)
            producer = get_essential('Producer', essential)
            composer = get_essential('Composer', essential)
            cinematographer = get_essential('Cinematographer', essential)

            # get main actors
            cast = tableDataText(soup.find('table', {"id": "principalCast"}))
            cast = [actor[0] for actor in cast]

            for i in range(0, 5):
                if i >= len(cast):
                    cast.append(None)

            main_actor_1 = cast[1]
            main_actor_2 = cast[2]
            main_actor_3 = cast[3]
            main_actor_4 = cast[4]

            movies_bom.append([movie_id, title, year, tagline, mpaa, release_date, run_time, distributor,
                               director, writer, producer, composer, cinematographer, main_actor_1, main_actor_2,
                               main_actor_3, main_actor_4, budget, domestic, international, worldwide, genre_1,
                               genre_2, genre_3, genre_4, html
                               ])
        except:
            continue

    return movies_