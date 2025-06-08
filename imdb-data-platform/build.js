import fs from 'fs/promises';
import * as cheerio from 'cheerio';
import { findByImdbId, getTmdbDetails } from './src/utils/tmdb_api.js';
import { analyzeAndTagItem } from './src/core/analyzer.js';

const IMDB_BASE_URL = 'https://www.imdb.com';
const MAX_ITEMS_PER_TASK = 250; // 每个抓取任务获取的IMDb ID数量
const MAX_CONCURRENT_ENRICHMENTS = 15;
const OUTPUT_FILE = './dist/database.json';

// --- 抓取任务矩阵 ---
const CRAWL_TASKS = [
    // 电影
    { title_type: 'feature', sort: 'moviemeter,asc', count: 250 },
    { title_type: 'feature', sort: 'user_rating,desc', num_votes: '25000,', count: 250 },
    { title_type: 'feature', genres: 'sci-fi', sort: 'user_rating,desc' },
    { title_type: 'feature', genres: 'horror', sort: 'user_rating,desc' },
    { title_type: 'feature', genres: 'action', sort: 'user_rating,desc' },
    { title_type: 'feature', countries: 'cn', sort: 'user_rating,desc' },
    // 剧集
    { title_type: 'tv_series,tv_miniseries', sort: 'moviemeter,asc', count: 250 },
    { title_type: 'tv_series,tv_miniseries', sort: 'user_rating,desc', num_votes: '10000,', count: 250 },
    { title_type: 'tv_series', countries: 'us', sort: 'user_rating,desc' },
    { title_type: 'tv_series', countries: 'kr', sort: 'user_rating,desc' },
    // 动画
    { title_type: 'feature,tv_series', genres: 'animation', countries: 'jp', sort: 'user_rating,desc' },
    { title_type: 'feature,tv_series', genres: 'animation', countries: 'cn', sort: 'user_rating,desc' },
];

async function fetchImdbPage(params) {
    const url = new URL(`${IMDB_BASE_URL}/search/title/`);
    Object.entries(params).forEach(([key, value]) => {
        if (value) url.searchParams.set(key, value);
    });
    const response = await fetch(url.toString(), { headers: { 'Accept-Language': 'en-US,en' } });
    if (!response.ok) throw new Error(`Failed to fetch IMDb page: ${response.statusText}`);
    return response.text();
}

function parseIntelligent(html) {
    const $ = cheerio.load(html);
    const ids = new Set();
    $('li.ipc-metadata-list-summary-item').each((_, element) => {
        const imdbId = $(element).find('a.ipc-title-link-wrapper').attr('href')?.match(/tt\d+/)?.[0];
        if (imdbId) ids.add(imdbId);
    });
    return Array.from(ids);
}

async function main() {
    console.log('Starting data lake build process...');
    const startTime = Date.now();
    const allImdbIds = new Set();

    try {
        console.log('\nPHASE 1: Crawling IMDb ID lists...');
        for (const params of CRAWL_TASKS) {
            console.log(`  Crawling with params: ${JSON.stringify(params)}`);
            const ids = await parseIntelligent(await fetchImdbPage(params));
            ids.slice(0, MAX_ITEMS_PER_TASK).forEach(id => allImdbIds.add(id));
            console.log(`    Found ${ids.length} IDs, pool size is now ${allImdbIds.size}.`);
        }

        console.log(`\nPHASE 2: Enriching ${allImdbIds.size} unique items with TMDB data...`);
        const dataLake = [];
        const imdbIdArray = Array.from(allImdbIds);

        for (let i = 0; i < imdbIdArray.length; i += MAX_CONCURRENT_ENRICHMENTS) {
            const batch = imdbIdArray.slice(i, i + MAX_CONCURRENT_ENRICHMENTS);
            const promises = batch.map(async (imdbId) => {
                const tmdbInfo = await findByImdbId(imdbId);
                if (!tmdbInfo) return null;
                const details = await getTmdbDetails(tmdbInfo.id, tmdbInfo.media_type);
                return analyzeAndTagItem(details);
            });
            const settledResults = await Promise.allSettled(promises);
            settledResults.forEach(result => {
                if (result.status === 'fulfilled' && result.value) dataLake.push(result.value);
            });
            console.log(`  Enriched batch ${i / MAX_CONCURRENT_ENRICHMENTS + 1} / ${Math.ceil(imdbIdArray.length / MAX_CONCURRENT_ENRICHMENTS)}`);
        }
        
        console.log(`\nPHASE 3: Writing data lake to ${OUTPUT_FILE}...`);
        const output = {
            buildTimestamp: new Date().toISOString(),
            database: dataLake,
        };
        const dir = './dist';
        await fs.mkdir(dir, { recursive: true });
        await fs.writeFile(OUTPUT_FILE, JSON.stringify(output));

        const duration = (Date.now() - startTime) / 1000;
        console.log(`\n✅ Build process successful! Data lake contains ${dataLake.length} items. Took ${duration.toFixed(2)} seconds.`);

    } catch (error) {
        console.error('\n❌ FATAL ERROR during build process:', error);
        process.exit(1);
    }
}

main();
