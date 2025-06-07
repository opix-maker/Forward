import fetch from 'node-fetch';
import * as cheerio from 'cheerio';
import { sleep, parseImdbRating } from '../utils/helpers.js';

const IMDB_BASE_URL = 'https://www.imdb.com';

// 定义地区代码和高级搜索的对应关系
const REGIONS = {
    ALL: { name: '全球', countries: null }, // 总榜单
    US: { name: '美国', countries: 'us' },
    GB: { name: '英国', countries: 'gb' },
    JP: { name: '日本', countries: 'jp' },
    KR: { name: '韩国', countries: 'kr' },
    IN: { name: '印度', countries: 'in' },
    FR: { name: '法国', countries: 'fr' },
    DE: { name: '德国', countries: 'de' },
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
    if (!response.ok) {
        throw new Error(`Failed to fetch IMDb page: ${response.statusText}`);
    }
    return response.text();
}

function parseImdbChart(html) {
    const $ = cheerio.load(html);
    const items = [];
    // IMDb的列表项选择器可能会变化，这里使用一个更通用的
    $('li.ipc-metadata-list-summary-item').each((_, element) => {
        const $item = $(element);
        const titleElement = $item.find('h3.ipc-title__text');
        const rankText = titleElement.text().match(/^(\d+)\./);
        const rank = rankText ? parseInt(rankText[1], 10) : null;
        const title = titleElement.text().replace(/^(\d+)\.\s*/, '').trim();
        
        const imdbId = $item.find('a.ipc-title-link-wrapper').attr('href')?.match(/tt\d+/)?.[0];
        
        const metadataItems = $item.find('.ipc-title-metadata-item');
        const year = metadataItems.eq(0)?.text().trim();
        const rating = $item.find('span.ipc-rating-star').text().trim();

        if (imdbId && title) {
            items.push({
                imdbId,
                title,
                rank,
                year,
                rating: parseImdbRating(rating),
            });
        }
    });
    return items;
}

const CHARTS_CONFIG = {
    mostPopular: { name: '热门榜单', path: '/chart/moviemeter/' },
    topRated: { name: '高分榜单', path: '/chart/top/' },
};

export async function buildImdbData() {
    console.log('Building IMDb data module...');
    const imdbData = {};

    for (const [chartKey, chart] of Object.entries(CHARTS_CONFIG)) {
        imdbData[chartKey] = {};
        console.log(`- Building chart type: ${chart.name}`);
        
        for (const [regionKey, region] of Object.entries(REGIONS)) {
            try {
                console.log(`  - Building region: ${region.name}`);
                let items;
                if (regionKey === 'ALL') {
                    // 总榜单使用固定的chart路径
                    const html = await fetchImdbPage(chart.path);
                    items = parseImdbChart(html);
                } else {
                    // 地区榜单使用高级搜索
                    const html = await fetchImdbPage('/search/title/', {
                        'title_type': 'feature',
                        'countries': region.countries,
                        'sort': 'moviemeter,asc' // 按热度排序
                    });
                    items = parseImdbChart(html);
                }
                
                imdbData[chartKey][regionKey] = items.slice(0, 50); // 每个榜单只取前50
                console.log(`    Found ${items.length} items, taking top 50 for ${region.name}.`);
                await sleep(1000);
            } catch (error) {
                console.error(`    Failed to build ${chart.name} for region ${region.name}:`, error.message);
                imdbData[chartKey][regionKey] = [];
            }
        }
    }

    console.log('IMDb data module build finished.');
    return imdbData;
}
