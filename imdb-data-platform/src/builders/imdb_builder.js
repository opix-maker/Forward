import fetch from 'node-fetch';
import * as cheerio from 'cheerio';
import { sleep } from '../utils/helpers.js';

const IMDB_BASE_URL = 'https://www.imdb.com';
const MAX_ITEMS_PER_LIST = 100; // 扩大抓取范围，为后续筛选提供充足数据

async function fetchImdbPage(params) {
    const url = new URL(`${IMDB_BASE_URL}/search/title/`);
    Object.entries(params).forEach(([key, value]) => {
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
                return jsonData.itemListElement.map(entry => entry.item.url?.match(/tt\d+/)?.[0]).filter(Boolean);
            }
        }
    } catch (e) { /* fallback */ }

    const items = [];
    $('li.ipc-metadata-list-summary-item').each((_, element) => {
        const imdbId = $(element).find('a.ipc-title-link-wrapper').attr('href')?.match(/tt\d+/)?.[0];
        if (imdbId) items.push(imdbId);
    });
    return items;
}

/**
 * 根据IMDb高级搜索参数，抓取一个IMDb ID列表
 * @param {object} params - IMDb高级搜索的参数
 * @returns {Array<string>} - IMDb ID 列表
 */
export async function fetchImdbIdList(params) {
    const html = await fetchImdbPage(params);
    const ids = parseIntelligent(html);
    return ids.slice(0, MAX_ITEMS_PER_LIST);
}
