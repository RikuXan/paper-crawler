import os
import csv
import shutil
import sys
from urllib.parse import urljoin
from itertools import islice
from re import sub

import requests
from bs4 import BeautifulSoup

papers_folder = 'papers'
csv_file_name = 'papers.csv'
csv_columns = ['Title', 'Authors', 'WebLink', 'PaperFile', 'AbstractFile', 'Source', 'Year']

pages_to_crawl = [{'type': 'icml_jmlr_hosted',
                   'name': 'ICML proceedings',
                   'data': [{'year': '2015',
                             'link': 'http://jmlr.org/proceedings/papers/v37/'},
                            {'year': '2014',
                             'link': 'http://jmlr.org/proceedings/papers/v32/'},
                            {'year': '2013',
                             'link': 'http://jmlr.org/proceedings/papers/v28/'}]},
                  {'type': 'icml2012',
                   'name': 'ICML proceedings',
                   'data': [{'year': '2012',
                             'link': 'http://icml.cc/2012/papers/'}]},
                  {'type': 'icml2011',
                   'name': 'ICML proceedings',
                   'data': [{'year': '2011',
                             'link': 'http://www.icml-2011.org/papers.php'}]},
                  {'type': 'icml2010',
                   'name': 'ICML proceedings',
                   'data': [{'year': '2010',
                             'link': 'http://icml2010.haifa.il.ibm.com/abstracts.html'}]},
                  {'type': 'icml2009',
                   'name': 'ICML proceedings',
                   'data': [{'year': '2009',
                             'link': 'http://www.machinelearning.org/archive/icml2009/abstracts.html'}]},
                  {'type': 'icml2008',
                   'name': 'ICML proceedings',
                   'data': [{'year': '2008',
                             'link': 'http://icml2008.cs.helsinki.fi/abstracts.shtml'}]},
                  {'type': 'icml2007',
                   'name': 'ICML proceedings',
                   'data': [{'year': '2007',
                             'link': 'http://oregonstate.edu/conferences/event/icml2007/paperlist.html'}]},
                  {'type': 'icml2006',
                   'name': 'ICML proceedings',
                   'data': [{'year': '2006',
                             'link': 'http://www.autonlab.org/icml2006/technical/accepted.html'}]},
                  {'type': 'icml2005',
                   'name': 'ICML proceedings',
                   'data': [{'year': '2005',
                             'link': 'http://www.machinelearning.org/icml2005_proc.html'}]},
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
                             'link': 'http://jmlr.csail.mit.edu/papers/v7'},
                            {'year': '2005',
                             'link': 'http://jmlr.csail.mit.edu/papers/v6'}]},
                  {'type': 'papers_nips',
                   'name': 'NIPS proceedings',
                   'data': [{'year': '2015',
                             'link': 'http://papers.nips.cc/book/advances-in-neural-information-processing-systems-28-2015'},
                            {'year': '2014',
                             'link': 'http://papers.nips.cc/book/advances-in-neural-information-processing-systems-27-2014'},
                            {'year': '2013',
                             'link': 'http://papers.nips.cc/book/advances-in-neural-information-processing-systems-26-2013'},
                            {'year': '2012',
                             'link': 'http://papers.nips.cc/book/advances-in-neural-information-processing-systems-25-2012'},
                            {'year': '2011',
                             'link': 'http://papers.nips.cc/book/advances-in-neural-information-processing-systems-24-2011'},
                            {'year': '2010',
                             'link': 'http://papers.nips.cc/book/advances-in-neural-information-processing-systems-23-2010'},
                            {'year': '2009',
                             'link': 'http://papers.nips.cc/book/advances-in-neural-information-processing-systems-22-2009'},
                            {'year': '2008',
                             'link': 'http://papers.nips.cc/book/advances-in-neural-information-processing-systems-21-2008'},
                            {'year': '2007',
                             'link': 'http://papers.nips.cc/book/advances-in-neural-information-processing-systems-20-2007'},
                            {'year': '2006',
                             'link': 'http://papers.nips.cc/book/advances-in-neural-information-processing-systems-19-2006'},
                            {'year': '2005',
                             'link': 'http://papers.nips.cc/book/advances-in-neural-information-processing-systems-18-2005'}]}]


def clean_text(text):
    return sub(r'[\s]+', r' ', text.strip())


def get_paper_list(page_soup, page_type):
    if page_type == 'icml_jmlr_hosted' or page_type == 'icml2012' or page_type == 'icml2010' \
            or page_type == 'icml2011' or page_type == 'icml2009' or page_type == 'icml2008'\
            or page_type == 'icml2007' or page_type == 'icml2005':
        return page_soup.find_all('div', {'class': 'paper'})
    elif page_type == 'icml2006':
        return page_soup.find_all('tr')[5:]
    elif page_type == 'jmlr_volume':
        return page_soup.find('div', id='content').find_all('dl')
    elif page_type == 'papers_nips':
        return page_soup.find('div', {'class': 'main-container'}).find_all('li')
    else:
        return []


def manipulate_page_html(page_html, page_type):
    if page_type == 'icml2011':
        # Insert div opening tags before every paper
        page_html = sub(r"(<a name='[0-9]+'>)", r'<div class="paper">\1', page_html)
        # Insert div closing tags after every paper end
        page_html = sub(r'(discuss<\/a>\]<\/p>)', r'\1</div>', page_html)

    elif page_type == 'icml2010':
        # Insert div opening tags before every paper
        page_html = sub(r'(<a name="[0-9]+">)', r'<div class="paper">\1', page_html)
        # Insert div closing tags after every paper end
        page_html = sub(r'(<hr><br \/>)', r'\1</div>', page_html)

    elif page_type == 'icml2009':
        # Remove stupidly placed <a name> tag in the beginning of the document
        page_html = page_html.replace('<a name="10">', '', 1)
        # Insert div opening tags before every paper
        page_html = sub(r'(<h3><a name="[0-9]+">)', r'<div class="paper">\1', page_html)
        # Insert div closing tags after every paper end
        page_html = sub(r'(Discussion<\/a>] \n<hr\/>)', r'\1</div>', page_html)

    elif page_type == 'icml2008':
        # Insert div opening tags before every paper
        page_html = sub(r'(<a name="[0-9]+">)', r'<div class="paper">\1', page_html)
        # Insert div closing tags after every paper end
        page_html = sub(r'(<\/p><hr>)', r'\1</div>', page_html)

    elif page_type == 'icml2007':
        # Insert div opening tags before every paper
        page_html = sub(r'(<tr class="header">[\s]*<td colspan="2">[\s]*<a name="[0-9]+">)', r'<div class="paper">\1', page_html)
        # Insert div closing tags before every opening tag, then remove the first one and add a final one at the table end
        page_html = sub(r'(<div class="paper">)', r'</div>\1', page_html)
        page_html = sub(r'<\/div>(<div class="paper">)', r'\1', page_html, 1)
        page_html = sub(r'(<br>)[\s]*(<\/table>)', r'\1</div>\2', page_html)

    elif page_type == 'icml2005':
        # Insert div opening tags before every paper
        page_html = sub(r'(<tr class="proc_2005_link">)', r'<div class="paper">\1', page_html)
        # Insert div closing tags before every opening tag, then remove the first one and add a final one at the table end
        page_html = sub(r'(<div class="paper">)', r'</div>\1', page_html)
        page_html = sub(r'<\/div>(<div class="paper">)', r'\1', page_html, 1)
        page_html = sub(r'(<\/table>)', r'</div>\1', page_html)

    elif page_type == 'jmlr_paper':
        # Surround abstract with a div for easier extraction
        page_html = sub(r'(<h3>Abstract<\/h3>)', '\1<div id="abstract">', page_html)
        page_html = sub(r'(<font color=)', r'</div>\1', page_html)

    return page_html


def get_paper_data(paper, page_type, page_address):
    title = ''
    authors = ''
    web_link = ''
    paper_file_name = ''
    pdf_link = ''
    abstract_data = ''
    source = ''
    year = ''

    if page_type == 'icml_jmlr_hosted':
        # Manual UTF-8 decode, because requests erroneously thinks the page is ISO-8559-1 encoded
        paper_page_soup = BeautifulSoup(requests.get(urljoin(page_address, paper.find('a', text='abs').attrs['href'])).content.decode('utf-8'), 'html.parser')

        title = paper_page_soup.find('h1').text
        authors = paper_page_soup.find('div', id='authors').text
        web_link = urljoin(page_address, paper.find('a', text='abs').attrs['href'])
        paper_file_name = paper.find('a', text='pdf').attrs['href'].split('/')[-1]
        pdf_link = urljoin(page_address, paper_page_soup.find('a', text='Download PDF').attrs['href'])
        abstract_data = paper_page_soup.find('div', id='abstract').text

    elif page_type == 'jmlr_volume':
        paper_page_soup = BeautifulSoup(manipulate_page_html(requests.get(urljoin(page_address, paper.find('a', text='abs').attrs['href'])).content.decode('utf-8'), 'jmlr_paper'), 'html.parser')

        title = paper_page_soup.find('h2').text
        authors = paper_page_soup.find('h2').find_next('i').text
        web_link = urljoin(page_address, paper.find('a', text='abs').attrs['href'])
        paper_file_name = paper.find('a', text='pdf').attrs['href'].split('/')[-1]
        pdf_link = urljoin(page_address, paper_page_soup.find('a', text='pdf').attrs['href'])
        abstract_data = paper_page_soup.find('div', id='abstract').text

    elif page_type == 'icml2012':
        title = paper.find('h2').text
        authors = paper.find('p', {'class': 'authors'}).next_element
        web_link = urljoin(page_address, paper.find('a', text='more on ArXiv').attrs['href']) if paper.find('a', text='more on ArXiv')is not None else page_address
        paper_file_name = paper.find('a', text='ICML version (pdf)').attrs['href'].split('/')[-1]
        pdf_link = urljoin(page_address, paper.find('a', text='ICML version (pdf)').attrs['href'])
        abstract_data = next(islice(paper.find('p', {'class': 'abstract'}).next_elements, 2, None))

    elif page_type == 'icml2011':
        title = paper.find('h3').text
        authors = paper.find('span', {'class': 'name'}).text
        web_link = page_address
        paper_file_name = paper.find('a', text='download').attrs['href'].split('/')[-1]
        pdf_link = urljoin(page_address, paper.find('a', text='download').attrs['href'])
        abstract_data = next(islice(paper.find('p').next_elements, 2, None))

    elif page_type == 'icml2010':
        title = paper.find('h3').text
        authors = sub(r' +\(.*?\)', '', sub(r' *; *', ', ', paper.find('em').text))
        web_link = page_address
        paper_file_name = paper.find('a', text='Full Paper').attrs['href'].split('/')[-1]
        pdf_link = urljoin(page_address, paper.find('a', text='Full Paper').attrs['href'])
        abstract_data = sub(r'[\s]+', ' ', paper.find('p', {'class': 'abstracts'}).text)

    elif page_type == 'icml2009':
        title = paper.find('h3').text
        authors = paper.find('i').text.replace(' and ', ', ')
        web_link = page_address
        paper_file_name = paper.find('a', text='Full paper').attrs['href'].split('/')[-1]
        pdf_link = urljoin(page_address, paper.find('a', text='Full paper').attrs['href'])
        abstract_data = paper.find('a', text='Full paper').find_previous('p').text

    elif page_type == 'icml2008':
        title = paper.find('h3').text
        authors = paper.find('i').text.replace(' and ', ', ')
        web_link = page_address
        paper_file_name = paper.find('a', text='Full paper').attrs['href'].split('/')[-1]
        pdf_link = urljoin(page_address, paper.find('a', text='Full paper').attrs['href'])
        abstract_data = paper.contents[8].text

    elif page_type == 'icml2007':
        paper_page_soup = BeautifulSoup(requests.get(urljoin(page_address, paper.find('a', text='[Abstract]').attrs['href'])).text, 'html.parser')

        title = paper_page_soup.find('table').contents[0].text
        authors = ', '.join([sub(r' - .*', r'', author) for author in paper_page_soup.find('table').contents[1].text.split('\n\n')])
        web_link = urljoin(page_address, paper.find('a', text='[Abstract]').attrs['href'])
        paper_file_name = paper.find('a', text='[Paper]').attrs['href'].split('/')[-1]
        pdf_link = urljoin(page_address, paper.find('a', text='[Paper]').attrs['href'])
        abstract_data = paper_page_soup.find('table').contents[2].text

    elif page_type == 'icml2006':
        title = paper.find('em').text
        authors = paper.contents[5].text
        web_link = page_address
        paper_file_name = paper.find('a').attrs['href'].split('/')[-1]
        pdf_link = urljoin(page_address, paper.find('a').attrs['href'])
        abstract_data = ''

    elif page_type == 'icml2005':
        title = paper.find('tr', {'class': 'proc_2005_link'}).find('a').text
        authors = paper.find('tr', {'class': 'proc_2005_authors'}).text
        web_link = page_address
        paper_file_name = paper.find('tr', {'class': 'proc_2005_link'}).find('a').attrs['href'].split('/')[-1]
        pdf_link = urljoin(page_address, paper.find('tr', {'class': 'proc_2005_link'}).find('a').attrs['href'])
        abstract_data = ''

    elif page_type == 'papers_nips':
        paper_page_soup = BeautifulSoup(requests.get(urljoin(page_address, paper.find('a').attrs['href'])).text, 'html.parser')

        title = paper_page_soup.find('h2', {'class': 'subtitle'}).text
        authors = ', '.join([author.text for author in paper_page_soup.find('ul', {'class': 'authors'}).find_all('li')])
        web_link = urljoin(page_address, paper.find('a').attrs['href'])
        paper_file_name = paper_page_soup.find('a', text='[PDF]').attrs['href'].split('/')[-1]
        pdf_link = urljoin(page_address, paper_page_soup.find('a', text='[PDF]').attrs['href'])
        abstract_data = paper_page_soup.find('p', {'class': 'abstract'}).text.replace('Abstract missing', '').replace('Abstract Missing', '')

    return {'title': clean_text(title), 'authors': clean_text(authors), 'web_link': web_link, 'paper_file_name': paper_file_name, 'pdf_link': pdf_link, 'abstract_data': clean_text(abstract_data), 'source': source, 'year': year}

# Clear existing data
shutil.rmtree(papers_folder, ignore_errors=True)
if os.path.isfile(csv_file_name):
    os.remove(csv_file_name)

with open(csv_file_name, 'w', newline='', encoding='utf-8') as csv_file:
    csv_writer = csv.writer(csv_file, delimiter=',', lineterminator='\n', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    csv_writer.writerow(csv_columns)

    for page_group in pages_to_crawl:
        # Create folder for page group
        page_group_folder_name = papers_folder + '/' + page_group['name'].replace(' ', '')
        os.makedirs(page_group_folder_name, exist_ok=True)

        for page in page_group['data']:
            if not (page_group['type'] == 'jmlr_volume' and page['year'] == '2014'):
                continue

            print('Crawling page ' + page_group['name'] + ' ' + page['year'], end='')
            sys.stdout.flush()

            # Extract and parse page data
            page_data = manipulate_page_html(requests.get(page['link']).text, page_group['type'])
            page_soup = BeautifulSoup(page_data, 'html.parser')
            paper_list = get_paper_list(page_soup, page_group['type'])[:5]

            # Extract paper data
            for index, paper in enumerate(paper_list):
                paper_data = get_paper_data(paper, page_group['type'], page['link'])

                # Create folder for year if it doesn't exist yet
                paper_folder = page_group_folder_name + '/' + page['year'] if 'year' in page else paper_data['year']
                os.makedirs(paper_folder, exist_ok=True)

                # Download paper and write it to disk (change filename if it already exists)
                paper_file_path = paper_folder + '/' + paper_data['paper_file_name']
                while os.path.isfile(paper_file_path):
                    paper_data['paper_file_name'] += '_1'
                    paper_file_path = paper_folder + paper_data['paper_file_name']
                #with open(paper_file_path, 'wb') as paper_file:
                   # paper_file.write(requests.get(paper_data['pdf_link']).content)

                # Download abstract and write it to disk
                abstract_file_path = paper_file_path.replace('.pdf', '.abs')
                with open(abstract_file_path, 'wb') as abstract_file:
                    abstract_file.write(paper_data['abstract_data'].encode('utf-8'))

                # Put data in csv file
                csv_writer.writerow([paper_data['title'],
                                     paper_data['authors'],
                                     paper_data['web_link'],
                                     paper_file_path,
                                     abstract_file_path,
                                     page_group['name'] if page_group['name'] != '' else paper_data['source'],
                                     page['year'] if page['year'] != '' else paper_data['year']])

                print('\rCrawling page ' + page_group['name'] + ' ' + page['year'] + ' (' + str(index + 1) + ' of ' + str(len(paper_list))+ ')', end='')

            print('\rFinished crawling page ' + page_group['name'] + ' ' + page['year'] + ' (' + str(len(paper_list)) + ' papers)')

# Problems:
# - NIPS has no abstracts 2005 - 2007 (text is "Abstract missing") -> Abstract rectifier
# - ICML has no abstracts 2005 - 2006  -> Abstract rectifier

# Testparse results:
# - Encoding errors:
#   - ICML 2013, muandet13.pdf, Author
#   - JMLR 2014, lember14a.pdf, Author, Abstract