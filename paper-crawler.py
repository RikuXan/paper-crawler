import os
import csv
import shutil

import requests
import urllib.parse
from bs4 import BeautifulSoup

papers_folder = 'papers'
global_csv_file_name = 'papers.csv'
csv_columns = ['Title', 'Authors', 'WebLink', 'PaperFile', 'AbstractFile']
global_csv_columns = ['Title', 'Authors', 'WebLink', 'PaperFile', 'AbstractFile', 'Source', 'Year']

pages_to_crawl = [{'type': 'icml_jmlr_hosted',
                   'name': 'ICML',
                   'data': [{'year': '2015',
                             'link': 'http://jmlr.org/proceedings/papers/v37/'},
                            {'year': '2014',
                             'link': 'http://jmlr.org/proceedings/papers/v32/'},
                            {'year': '2013',
                             'link': 'http://jmlr.org/proceedings/papers/v28/'}]},
                  {'type': 'jmlr_volume',
                   'name': 'JMLR volume',
                   'data': [{'year': '2015',
                             'link': 'http://jmlr.csail.mit.edu/papers/v16'},
                            {'year': '2014',
                             'link': 'http://jmlr.csail.mit.edu/papers/v15'},
                            {'year': '2013',
                             'link': 'http://jmlr.csail.mit.edu/papers/v14'},
                            {'year': '2012',
                             'link': 'http://jmlr.csail.mit.edu/papers/v13'},
                            {'year': '2011',
                             'link': 'http://jmlr.csail.mit.edu/papers/v12'},
                            {'year': '2010',
                             'link': 'http://jmlr.csail.mit.edu/papers/v11'},
                            {'year': '2009',
                             'link': 'http://jmlr.csail.mit.edu/papers/v10'},
                            {'year': '2008',
                             'link': 'http://jmlr.csail.mit.edu/papers/v9'},
                            {'year': '2007',
                             'link': 'http://jmlr.csail.mit.edu/papers/v8'},
                            {'year': '2006',
                             'link': 'http://jmlr.csail.mit.edu/papers/v7'}]}]


def get_paper_list(page_soup, page_type):
    if page_type == 'icml_jmlr_hosted':
        return page_soup.find_all('div', {'class': 'paper'})[:2]
    elif page_type == 'jmlr_volume':
        return page_soup.find('div', id='content').find_all('dl')[:2]


def get_paper_data(paper, page_type, page_address):
    title = ''
    authors = ''
    web_link =''
    paper_file = ''
    pdf_link = ''
    abstract_data = ''
    source = ''
    year = ''

    if page_type == 'icml_jmlr_hosted':
        paper_page_soup = BeautifulSoup(requests.get(urllib.parse.urljoin(page_address, paper.find('a', text='abs').attrs['href'])).text, 'html.parser')

        title = paper.find('p', {'class': 'title'}).text
        authors = paper.find('span', {'class': 'authors'}).text.replace('\t', '').replace('\n', '').replace(',', ', ')
        web_link = urllib.parse.urljoin(page_address, paper.find('a', text='abs').attrs['href'])
        paper_file = paper.find('a', text='pdf').attrs['href'].split('/')[-1]
        pdf_link = urllib.parse.urljoin(page_address, paper_page_soup.find('a', text='Download PDF').attrs['href'])
        abstract_data = paper_page_soup.find('div', id='abstract').text.strip()

    elif page_type == 'jmlr_volume':
        paper_page_soup = BeautifulSoup(requests.get(urllib.parse.urljoin(page_address, paper.find('a', text='abs').attrs['href'])).text, 'html.parser')

        title = paper.find('dt').next_element.strip()
        authors = paper.find('dd').find('i').text
        web_link = urllib.parse.urljoin(page_address, paper.find('a', text='abs').attrs['href'])
        paper_file = paper.find('a', text='pdf').attrs['href'].split('/')[-1]
        pdf_link = urllib.parse.urljoin(page_address, paper_page_soup.find('a', text='pdf').attrs['href'])
        abstract_tag = paper.find('h3')
        while abstract_tag.next_sibling.name != 'font':
            abstract_data += abstract_tag.next_sibling
            abstract_tag = abstract_tag.next_sibling
        abstract_data = abstract_data

    return {'title': title, 'authors': authors, 'web_link': web_link, 'paper_file': paper_file, 'pdf_link': pdf_link, 'abstract_data': abstract_data, 'source': source, 'year': year}

# Clear existing data
shutil.rmtree(papers_folder, ignore_errors=True)
if os.path.isfile(global_csv_file_name):
    os.remove(global_csv_file_name)

with open(global_csv_file_name, 'w') as global_csv_file:
    global_csv_writer = csv.writer(global_csv_file, delimiter=',', lineterminator='\n', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    global_csv_writer.writerow(global_csv_columns)

    for page_group in pages_to_crawl:
        # Create folder for page group
        page_group_folder_name = papers_folder + '/' + page_group['name'].replace(' ', '')
        os.makedirs(page_group_folder_name, exist_ok=True)

        for page in page_group['data'][:2]:
            # Extract and parse page data
            page_data = requests.get(page['link']).text
            page_soup = BeautifulSoup(page_data, 'html.parser')
            paper_list = get_paper_list(page_soup, page_group['type'])

            # Extract paper data
            for paper in paper_list:
                paper_data = get_paper_data(paper, page_group['type'], page['link'])

                # Create folder for year if it doesn't exist yet
                paper_folder = page_group_folder_name + '/' + page['year'] if 'year' in page else paper_data['year']
                os.makedirs(paper_folder, exist_ok=True)

                # Download paper and write it to disk (change filename if it already exists)
                paper_file_name = paper_folder + '/' + paper_data['paper_file']
                while os.path.isfile(paper_file_name):
                    paper_data['paper_file'] += '_1'
                    paper_file_name = paper_folder + paper_data['paper_file']
                with open(paper_file_name, 'wb') as paper_file:
                    paper_file.write(requests.get(paper_data['pdf_link']).content)

                # Download abstract and write it to disk
                abstract_file_name = paper_file_name.replace('.pdf', '.abs')
                with open(abstract_file_name, 'wb') as abstract_file:
                    abstract_file.write(paper_data['abstract_data'].encode('utf-8'))

                # Put data in csv file
                global_csv_writer.writerow([paper_data['title'],
                                            paper_data['authors'],
                                            paper_data['web_link'],
                                            paper_file_name,
                                            abstract_file_name,
                                            page_group['name'] if page_group['name'] != '' else paper_data['source'],
                                            page['year'] if page['year'] != '' else paper_data['year']])

# nips_pages =