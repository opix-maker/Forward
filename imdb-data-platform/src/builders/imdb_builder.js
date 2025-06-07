import fetch from 'node-fetch';
import * as cheerio from 'cheerio';
import { sleep, parseImdbRating } from '../utils/helpers.js';

const IMDB_BASE_URL = 'https://www.imdb.com';

// 封装的IMDb页面抓取函数
async function fetchImdbPage(path) {
    const url = `${IMDB_BASE_URL}${path}`;
    console.log(`  Fetching IMDb page: ${url}`);
    const response = await fetch(url, {
        headers: {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9'
        }
    });
    if (!response.ok) {
        throw new Error(`Failed to fetch IMDb page: ${response.statusText}`);
    }
    return response.text();
}

// 解析IMDb列表页的通用函数
function parseImdbChart(html) {
    const $ = cheerio.load(html);
    const items = [];
    $('.ipc-metadata-list-summary-item').each((_, element) => {
        const $item = $(element);
        const titleElement = $item.find('h3.ipc-title__text');
        const rank = parseInt(titleElement.text().split('.')[0], 10);
        const title = titleElement.text().split('.').slice(1).join('.').trim();
        
        const imdbId = $item.find('a.ipc-title-link-wrapper').attr('href')?.match(/tt\d+/)?.[0];
        
        const metadata = $item.find('div.ipc-title-metadata-item');
        const year = metadata.eq(0).text().trim();
        const duration = metadata.eq(1).text().trim();
        const rating = $item.find('span.ipc-rating-star').text().trim();

        if (imdbId && title) {
            items.push({
                imdbId,
                title,
                rank,
                year,
                duration,
                rating: parseImdbRating(rating),
            });
        }
    });
    return items;
}

// 定义要抓取的榜单
const chartsToBuild = {
    mostPopularMovies: {
        name: "热门电影",
        path: '/chart/moviemeter/',
        parser: parseImdbChart
    },
    topRatedMovies: {
        name: "高分电影",
        path: '/chart/top/',
        parser: parseImdbChart
    },
    mostPopularTV: {
        name: "热门剧集",
        path: '/chart/tvmeter/',
        parser: parseImdbChart
    },
    topRatedTV: {
        name: "高分剧集",
        path: '/chart/toptv/',
        parser: parseImdbChart
    }
    
};

/**
 * 主构建函数
 * @returns {Promise<object>}
 */
export async function buildImdbData() {
    console.log('Building IMDb data module...');
    const imdbData = {};

    for (const [key, chart] of Object.entries(chartsToBuild)) {
        try {
            console.log(`- Building chart: ${chart.name}`);
            const html = await fetchImdbPage(chart.path);
            const items = chart.parser(html);
            imdbData[key] = items;
            console.log(`  Found ${items.length} items for ${chart.name}.`);
            await sleep(1000); // 添加1秒的礼貌性等待
        } catch (error) {
            console.error(`  Failed to build chart ${chart.name}:`, error.message);
            imdbData[key] = []; // 如果失败，则返回空数组
        }
    }

    console.log('IMDb data module build finished.');
    return imdbData;
}
