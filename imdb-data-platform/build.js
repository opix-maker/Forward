import fs from 'fs/promises';
import * as cheerio from 'cheerio';
import { findByImdbId, getTmdbDetails } from './src/utils/tmdb_api.js';
import { analyzeAndTagItem } from './src/core/analyzer.js';

const IMDB_BASE_URL = 'https://www.imdb.com';
const MAX_ITEMS_PER_CRAWL = 250;
const MAX_CONCURRENT_ENRICHMENTS = 15;
const OUTPUT_DIR = './dist';

// --- 抓取任务矩阵：定义数据湖的源头 ---
const CRAWL_MATRIX = [
    // 核心榜单
    { path: '/chart/moviemeter/' }, { path: '/chart/top/' },
    { path: '/chart/tvmeter/' }, { path: '/chart/toptv/' },
    // 亚洲地区专项抓取 (确保数据多样性)
    { params: { countries: 'jp', title_type: 'feature,tv_series', sort: 'user_rating,desc' } },
    { params: { countries: 'kr', title_type: 'tv_series', sort: 'user_rating,desc' } },
    { params: { countries: 'cn,hk,tw', title_type: 'feature,tv_series', sort: 'user_rating,desc' } },
    // 核心类型专项抓取
    { params: { genres: 'sci-fi', sort: 'user_rating,desc' } },
    { params: { genres: 'horror', sort: 'user_rating,desc' } },
    { params: { genres: 'animation', sort: 'user_rating,desc' } },
];

// --- 数据切片矩阵：定义最终输出的JSON文件 ---
const BUILD_MATRIX = {
    official_popular_movies: { name: '热门电影', tags: ['type:movie'], sortBy: 'popularity' },
    official_top_movies: { name: '高分电影', tags: ['type:movie'], sortBy: 'vote_average' },
    official_popular_tv: { name: '热门剧集', tags: ['type:tv'], sortBy: 'popularity' },
    official_top_tv: { name: '高分剧集', tags: ['type:tv'], sortBy: 'vote_average' },
    
    jp_anime_top: { name: '高分日漫', tags: ['type:animation', 'country:jp'], sortBy: 'vote_average' },
    us_scifi_top: { name: '高分科幻美剧', tags: ['type:tv', 'genre:科幻', 'country:us'], sortBy: 'vote_average' },
    kr_tv_popular: { name: '热门韩剧', tags: ['type:tv', 'country:kr'], sortBy: 'popularity' },
    cn_movie_top: { name: '高分国产电影', tags: ['type:movie', 'lang:chinese'], sortBy: 'vote_average' },

    theme_cyberpunk: { name: '赛博朋克精选', tags: ['theme:cyberpunk'], sortBy: 'vote_average' },
    theme_zombie: { name: '僵尸末日', tags: ['theme:zombie'], sortBy: 'vote_average' },
    theme_wuxia: { name: '武侠世界', tags: ['theme:wuxia'], sortBy: 'vote_average' },
    theme_film_noir: { name: '黑色电影', tags: ['theme:film-noir'], sortBy: 'vote_average' },
};

async function fetchImdbPage(config) {
    const url = config.path ? new URL(`${IMDB_BASE_URL}${config.path}`) : new URL(`${IMDB_BASE_URL}/search/title/`);
    if (config.params) {
        Object.entries(config.params).forEach(([key, value]) => url.searchParams.set(key, value));
    }
    const response = await fetch(url.toString(), { headers: { 'Accept-Language': 'en-US,en' } });
    if (!response.ok) throw new Error(`Failed to fetch IMDb page: ${response.statusText}`);
    return response.text();
}

function parseIntelligent(html) {
    const $ = cheerio.load(html);
    const ids = new Set();
    $('li.ipc-metadata-list-summary-item a.ipc-title-link-wrapper').each((_, el) => {
        const imdbId = $(el).attr('href')?.match(/tt\d+/)?.[0];
        if (imdbId) ids.add(imdbId);
    });
    return Array.from(ids);
}

async function main() {
    console.log('Starting data lake build process v3.0...');
    const startTime = Date.now();
    const allImdbIds = new Set();

    try {
        console.log('\nPHASE 1: Crawling IMDb ID lists to build raw ID pool...');
        for (const task of CRAWL_MATRIX) {
            const ids = parseIntelligent(await fetchImdbPage(task));
            ids.slice(0, MAX_ITEMS_PER_CRAWL).forEach(id => allImdbIds.add(id));
        }
        console.log(`  Raw ID pool contains ${allImdbIds.size} unique items.`);

        console.log(`\nPHASE 2: Enriching all items to create data lake...`);
        const dataLake = [];
        const imdbIdArray = Array.from(allImdbIds);
        for (let i = 0; i < imdbIdArray.length; i += MAX_CONCURRENT_ENRICHMENTS) {
            const batch = imdbIdArray.slice(i, i + MAX_CONCURRENT_ENRICHMENTS);
            const promises = batch.map(id => findByImdbId(id).then(info => info ? getTmdbDetails(info.id, info.media_type) : null).then(details => analyzeAndTagItem(details)));
            const results = await Promise.allSettled(promises);
            results.forEach(r => r.status === 'fulfilled' && r.value && dataLake.push(r.value));
        }
        console.log(`  Data lake created with ${dataLake.length} fully analyzed items.`);

        console.log('\nPHASE 3: Slicing data lake into individual data marts...');
        await fs.rm(OUTPUT_DIR, { recursive: true, force: true }); // 清理旧数据
        await fs.mkdir(OUTPUT_DIR, { recursive: true });

        for (const [key, config] of Object.entries(BUILD_MATRIX)) {
            let listData = dataLake.filter(item => config.tags.every(tag => item.semantic_tags.includes(tag)));
            listData.sort((a, b) => (b[config.sortBy] || 0) - (a[config.sortBy] || 0));
            listData = listData.slice(0, 50);
            await fs.writeFile(`${OUTPUT_DIR}/${key}.json`, JSON.stringify(listData));
            console.log(`  Generated list: ${config.name} -> ${key}.json (${listData.length} items)`);
        }

        const index = {
            buildTimestamp: new Date().toISOString(),
            lists: Object.entries(BUILD_MATRIX).map(([id, { name }]) => ({ id, name })),
        };
        await fs.writeFile(`${OUTPUT_DIR}/index.json`, JSON.stringify(index));
        console.log(`\nSuccessfully wrote index file.`);

        const duration = (Date.now() - startTime) / 1000;
        console.log(`\n✅ Build process successful! Took ${duration.toFixed(2)} seconds.`);

    } catch (error) {
        console.error('\n❌ FATAL ERROR during build process:', error);
        process.exit(1);
    }
}

main();
