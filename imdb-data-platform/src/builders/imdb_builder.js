import fetch from 'node-fetch';
import * as cheerio from 'cheerio';
import { sleep, parseImdbRating } from '../utils/helpers.js';

const IMDB_BASE_URL = 'https://www.imdb.com';
const MAX_ITEMS_PER_LIST = 50;

export const STATIC_CHARTS_CONFIG = {
    popularMovies: { name: '热门电影', path: '/chart/moviemeter/' },
    topMovies: { name: '高分电影', path: '/chart/top/' },
    popularTV: { name: '热门剧集', path: '/chart/tvmeter/' },
    topTV: { name: '高分剧集', path: '/chart/toptv/' },
};

export const CURATED_LISTS_CONFIG = {
    topSciFiMovies: { name: '高分科幻电影', params: { title_type: 'feature', genres: 'sci-fi', user_rating: '8.0,10', num_votes: '25000,', sort: 'user_rating,desc' } },
    topHorrorMovies: { name: '高分恐怖电影', params: { title_type: 'feature', genres: 'horror', user_rating: '7.5,10', num_votes: '20000,', sort: 'user_rating,desc' } },
    recentPopularSeries: { name: '近期热门剧集 (2022-)', params: { title_type: 'tv_series', release_date: '2022-01-01,', sort: 'moviemeter,asc' } },
    topActionSeries: { name: '高分动作剧集', params: { title_type: 'tv_series', genres: 'action', user_rating: '8.2,10', num_votes: '15000,', sort: 'user_rating,desc' } },
    gemsFromAsia: { name: '亚洲之光', params: { title_type: 'feature,tv_series', countries: 'jp,kr,cn,hk,tw,in,th', user_rating: '8.0,10', num_votes: '5000,', sort: 'user_rating,desc' } },
    mindBenders: { name: '烧脑神作', params: { title_type: 'feature', keywords: 'mind-bender,time-travel,plot-twist', user_rating: '7.5,10', sort: 'user_rating,desc' } },
};

async function fetchImdbPage(path, queryParams = {}) {
    const url = new URL(`${IMDB_BASE_URL}${path}`);
    Object.entries(queryParams).forEach(([key, value]) => {
        if (value) url.searchParams.set(key, value);
    });
    
    console.log(`  Fetching IMDb page: ${url.toString()}`);
    const response = await fetch(url.toString(), {
        headers: {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9'
        }
    });
    if (!response.ok) throw new Error(`Failed to fetch IMDb page: ${response.statusText}`);
    return response.text();
}

function parseIntelligent(html) {
    const $ = cheerio.load(html);
    try {
        const scriptTag = $('script[type="application/ld+json"]').first().html();
        if (scriptTag) {
            const jsonData = JSON.parse(scriptTag);
            if (jsonData && jsonData.itemListElement) {
                console.log('    Parsing via JSON-LD (Success).');
                return jsonData.itemListElement.map(entry => ({
                    imdbId: entry.item.url?.match(/tt\d+/)?.[0],
                    title: entry.item.name,
                })).filter(item => item.imdbId && item.title);
            }
        }
    } catch (e) {
        console.warn('    JSON-LD parsing failed, falling back to HTML parsing.');
    }

    console.log('    Parsing via HTML tags.');
    const items = [];
    $('li.ipc-metadata-list-summary-item').each((_, element) => {
        const $item = $(element);
        const titleElement = $item.find('h3.ipc-title__text');
        const title = titleElement.text().replace(/^(\d+)\.\s*/, '').trim();
        const imdbId = $item.find('a.ipc-title-link-wrapper').attr('href')?.match(/tt\d+/)?.[0];
        if (imdbId && title) {
            items.push({ imdbId, title });
        }
    });
    return items;
}

export async function fetchImdbIdList(config) {
    let html;
    if (config.path) {
        html = await fetchImdbPage(config.path);
    } else if (config.params) {
        html = await fetchImdbPage('/search/title/', config.params);
    } else {
        return [];
    }
    const items = parseIntelligent(html);
    return items.slice(0, MAX_ITEMS_PER_LIST).map(item => item.imdbId);
}
