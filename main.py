import asyncio
import sys
import aiohttp
from bs4 import BeautifulSoup
import aiosqlite

# For Windows, to avoid 'RuntimeError: Event loop is closed'
if sys.platform.startswith('win'):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

async def fetch(session, url):
    async with session.get(url) as response:
        return await response.text()


def get_text_safe(tag):
    return tag.get_text(strip=True) if tag else "Not Available"


def get_text_from_list_safe(tags, index):
    return tags[index].get_text(strip=True) if tags and len(tags) > index else "Not Available"


async def scrape_review(session, url):
    content = await fetch(session, url)
    soup = BeautifulSoup(content, 'html.parser')

    artist_name = get_text_safe(soup.find('div', class_='SplitScreenContentHeaderArtist-ftloCc'))
    album_name = get_text_safe(soup.find('h1',
                                         class_='BaseWrap-sc-gjQpdd BaseText-ewhhUZ SplitScreenContentHeaderHed-lcUSuI iUEiRd fnwdMb fTtZlw'))

    score_div = soup.find('div', class_='ScoreCircle-jAxRuP')
    score = get_text_safe(score_div.find('p')) if score_div else "Not Available"

    best_new_div = soup.find('p',
                             class_='BaseWrap-sc-gjQpdd BaseText-ewhhUZ BestNewMusicText-xXvIB iUEiRd hJnYqh hnZKay')
    best_new = get_text_safe(best_new_div) if best_new_div else "Not Available"

    release_year = get_text_safe(soup.find('time', class_='SplitScreenContentHeaderReleaseYear-UjuHP'))
    reviewer_name = get_text_safe(soup.find('a', class_='BylineLink-gEnFiw'))
    info_slices = soup.find_all('p',
                                class_="BaseWrap-sc-gjQpdd BaseText-ewhhUZ InfoSliceValue-tfmqg iUEiRd dcTQYO fkSlPp")
    genre = "Not Available" if len(info_slices) < 3 else get_text_from_list_safe(info_slices, 0)
    label = get_text_from_list_safe(info_slices, 1)
    review_date = get_text_from_list_safe(info_slices, 2)
    summary = get_text_safe(soup.find('div',
                                      class_='BaseWrap-sc-gjQpdd BaseText-ewhhUZ SplitScreenContentHeaderDekDown-csTFQR iUEiRd jqOMmZ MVQMg'))

    review_text_divs = soup.find_all('div', class_='BodyWrapper-kufPGa cmVAut body body__container article__body')
    full_review_text = " ".join([p.get_text(strip=True) for div in review_text_divs for p in div.find_all('p')])

    return (
    artist_name, album_name, score, release_year, reviewer_name, genre, label, review_date, summary, full_review_text,
    best_new)


async def scrape_page_reviews(session, db, page_number):
    reviews_url = f'https://pitchfork.com/reviews/albums/?page={page_number}'
    page_content = await fetch(session, reviews_url)
    soup = BeautifulSoup(page_content, 'html.parser')

    review_links = soup.find_all('a', class_='review__link')
    for link in review_links:
        review_url = f'https://pitchfork.com{link.get("href")}'
        review_data = await scrape_review(session, review_url)
        await db.execute('''INSERT INTO reviews (artist, album, score, year, reviewer, genre, label, review_date, summary, review, best_new) 
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', review_data)

    await db.commit()


async def main():
    async with aiosqlite.connect('pitchfork_reviews2.db') as db:
        await db.execute('''CREATE TABLE IF NOT EXISTS reviews 
                            (artist TEXT, album TEXT, score TEXT, year TEXT, reviewer TEXT, 
                             genre TEXT, label TEXT, review_date TEXT, summary TEXT, review TEXT, best_new TEXT)''')

        async with aiohttp.ClientSession() as session:
            tasks = [scrape_page_reviews(session, db, page_number) for page_number in range(1, 2200)]
            await asyncio.gather(*tasks)


asyncio.run(main())
