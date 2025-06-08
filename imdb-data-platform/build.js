import fs from 'fs/promises';
import path from 'path';
import fetch from 'node-fetch';
import { pipeline } from 'stream/promises';
import { createWriteStream, createReadStream } from 'fs';
import zlib from 'zlib';
import readline from 'readline';
import * as cheerio from 'cheerio'; // <--- 核心修复：从默认导入改为命名空间导入
import { findByImdbId, getTmdbDetails } from './src/utils/tmdb_api.js';
import { analyzeAndTagItem } from './src/core/analyzer.js';

const IMDB_BASE_URL = 'https://www.imdb.com';
const DATASET_DIR = './datasets';
const TEMP_DIR = './temp';
const OUTPUT_DIR = './dist';
const MAX_CONCURRENT_ENRICHMENTS = 20;
const DATA_LAKE_FILE = path.join(TEMP_DIR, 'datalake.jsonl');

const DATASETS = {
    basics: { url: 'https://datasets.imdbws.com/title.basics.tsv.gz', local: 'title.basics.tsv' },
    ratings: { url: 'https://datasets.imdbws.com/title.ratings.tsv.gz', local: 'title.ratings.tsv' },
};

const CRAWL_MATRIX = [
    { path: '/chart/moviemeter/' }, { path: '/chart/top/' }, { path: '/chart/tvmeter/' }, { path: '/chart/toptv/' },
    { params: { countries: 'jp', title_type: 'feature,tv_series', sort: 'user_rating,desc' } },
    { params: { countries: 'kr', title_type: 'feature,tv_series', sort: 'user_rating,desc' } },
    { params: { countries: 'cn,hk,tw', title_type: 'feature,tv_series', sort: 'user_rating,desc' } },
    { params: { genres: 'sci-fi', sort: 'user_rating,desc' } }, { params: { genres: 'horror', sort: 'user_rating,desc' } },
    { params: { genres: 'animation', sort: 'user_rating,desc' } },
];

const BUILD_MATRIX = {
    inTheaters: { name: '院线热映', derive: true, type: 'inTheaters' },
    weeklyTrending: { name: '本周热门', derive: true, type: 'weeklyTrending' },
    annualBest_2024: { name: '2024年度口碑榜', derive: true, type: 'annualBest', year: 2024 },
    annualBest_2023: { name: '2023年度口碑榜', derive: true, type: 'annualBest', year: 2023 },
    recentlyPopular: { name: '近期流行', derive: true, type: 'recentlyPopular' },
    theme_mindBenders: { name: '烧脑神作', tags: ['style:plot-twist'], sortBy: 'vote_average' },
    theme_hiddenGems: { name: '冷门高分', derive: true, type: 'hiddenGems' },
    seriesUniverse: { name: '系列宇宙', derive: true, type: 'seriesUniverse' },
    top_rated_movies: { name: '影史高分电影', tags: ['type:movie'], sortBy: 'vote_average', minVotes: 25000 },
    top_rated_tv: { name: '影史高分剧集', tags: ['type:tv'], sortBy: 'vote_average', minVotes: 10000 },
    top_rated_jp_anime: { name: '高分日系动画', tags: ['category:jp_anime'], sortBy: 'vote_average', minVotes: 1000 },
    popular_us_tv: { name: '热门美剧', tags: ['category:us-eu_tv'], sortBy: 'popularity' },
};


async function downloadAndUnzip(url, localPath) {
    const dir = path.dirname(localPath);
    await fs.mkdir(dir, { recursive: true });
    console.log(`  Downloading from official URL: ${url}`);
    const response = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36' } });
    if (!response.ok) {
        const errorBody = await response.text();
        throw new Error(`Failed to download ${url} - Status: ${response.status} ${response.statusText}. Body: ${errorBody}`);
    }
    const gunzip = zlib.createGunzip();
    const destination = createWriteStream(localPath);
    await pipeline(response.body, gunzip, destination);
    console.log(`  Download and unzip complete for ${path.basename(localPath)}.`);
}

async function processTsvByLine(filePath, processor) {
    const fileStream = createReadStream(filePath);
    const rl = readline.createInterface({ input: fileStream, crlfDelay: Infinity });
    let isFirstLine = true;
    for await (const line of rl) {
        if (isFirstLine) { isFirstLine = false; continue; }
        processor(line);
    }
}

async function buildAndEnrichDataLake() {
    console.log('\nPHASE 1: Building and Enriching Data Lake to Disk...');
    await fs.rm(TEMP_DIR, { recursive: true, force: true });
    await fs.mkdir(TEMP_DIR, { recursive: true });
    const writeStream = createWriteStream(DATA_LAKE_FILE, { flags: 'a' });

    const allImdbIds = new Set();
    for (const task of CRAWL_MATRIX) {
        const url = task.path ? `${IMDB_BASE_URL}${task.path}` : `https://www.imdb.com/search/title/?${new URLSearchParams(task.params)}`;
        const html = await fetch(url).then(res => res.text());
        const $ = cheerio.load(html);
        $('li.ipc-metadata-list-summary-item a.ipc-title-link-wrapper').each((_, el) => {
            const imdbId = $(el).attr('href')?.match(/tt\d+/)?.[0];
            if (imdbId) allImdbIds.add(imdbId);
        });
    }
    console.log(`  Crawled ${allImdbIds.size} unique IMDb IDs.`);

    const imdbIdArray = Array.from(allImdbIds);
    for (let i = 0; i < imdbIdArray.length; i += MAX_CONCURRENT_ENRICHMENTS) {
        const batch = imdbIdArray.slice(i, i + MAX_CONCURRENT_ENRICHMENTS);
        const promises = batch.map(id => findByImdbId(id).then(info => info ? getTmdbDetails(info.id, info.media_type) : null).then(details => analyzeAndTagItem(details)));
        const results = await Promise.allSettled(promises);
        
        let writeBuffer = '';
        results.forEach(r => {
            if (r.status === 'fulfilled' && r.value) {
                writeBuffer += JSON.stringify(r.value) + '\n';
            }
        });
        if (writeBuffer) {
            writeStream.write(writeBuffer);
        }
        console.log(`  Enriched and wrote batch ${i / MAX_CONCURRENT_ENRICHMENTS + 1} / ${Math.ceil(imdbIdArray.length / MAX_CONCURRENT_ENRICHMENTS)}`);
    }
    writeStream.end();
    console.log(`  Data Lake written to ${DATA_LAKE_FILE}`);
}

async function sliceDataMartsFromLake() {
    console.log('\nPHASE 2: Slicing Data Marts from Data Lake on Disk...');
    const listBuckets = Object.fromEntries(Object.keys(BUILD_MATRIX).map(key => [key, []]));
    const fullDatabase = [];
    const collections = new Map();

    await processTsvByLine(DATA_LAKE_FILE, (line) => {
        const item = JSON.parse(line);
        fullDatabase.push(item);

        if (item.belongs_to_collection) {
            const id = item.belongs_to_collection.id;
            if (!collections.has(id)) collections.set(id, { name: item.belongs_to_collection.name, items: [] });
            collections.get(id).items.push(item);
        }

        for (const [key, config] of Object.entries(BUILD_MATRIX)) {
            if (config.tags) {
                if (config.tags.every(tag => item.semantic_tags.includes(tag))) {
                    if (!config.minVotes || (item.vote_count || 0) >= config.minVotes) {
                        listBuckets[key].push(item);
                    }
                }
            }
        }
    });
    console.log(`  Finished reading data lake. Found ${fullDatabase.length} total items.`);

    const now = new Date();
    listBuckets.inTheaters = fullDatabase.filter(item => {
        const releaseDate = new Date(item.release_date);
        return item.semantic_tags.includes('type:movie') && releaseDate >= new Date(now.getTime() - 6 * 7 * 24 * 60 * 60 * 1000) && releaseDate <= new Date(now.getTime() + 4 * 7 * 24 * 60 * 60 * 1000);
    }).sort((a, b) => b.popularity - a.popularity);

    listBuckets.weeklyTrending = [...fullDatabase].sort((a, b) => b.popularity - a.popularity);
    listBuckets.recentlyPopular = fullDatabase.filter(item => new Date(item.release_date) >= new Date(now.getTime() - 180 * 24 * 60 * 60 * 1000)).sort((a, b) => b.popularity - a.popularity);
    listBuckets.annualBest_2024 = fullDatabase.filter(item => item.release_year === 2024 && item.vote_count > 1000).sort((a, b) => b.vote_average - a.vote_average);
    listBuckets.annualBest_2023 = fullDatabase.filter(item => item.release_year === 2023 && item.vote_count > 1000).sort((a, b) => b.vote_average - a.vote_average);
    listBuckets.theme_hiddenGems = fullDatabase.filter(item => item.vote_average > 8.0 && item.vote_count > 5000 && item.vote_count < 25000).sort((a, b) => b.vote_average - a.vote_average);
    listBuckets.seriesUniverse = Array.from(collections.values()).filter(c => c.items.length > 1).map(c => {
        c.items.sort((a, b) => new Date(a.release_date) - new Date(b.release_date));
        return c;
    });

    await fs.rm(OUTPUT_DIR, { recursive: true, force: true });
    await fs.mkdir(OUTPUT_DIR, { recursive: true });

    for (const [key, config] of Object.entries(BUILD_MATRIX)) {
        const bucket = listBuckets[key];
        if (config.sortBy) {
            bucket.sort((a, b) => (b[config.sortBy] || 0) - (a[config.sortBy] || 0));
        }
        const finalData = bucket.slice(0, config.limit || 50);
        await fs.writeFile(`${OUTPUT_DIR}/${key}.json`, JSON.stringify(finalData));
        console.log(`  Generated list: ${config.name} -> ${key}.json (${finalData.length} items)`);
    }
    
    const clientDatabase = fullDatabase.map(item => ({
        id: item.id, title: item.title, poster_path: item.poster_path,
        release_year: item.release_year, vote_average: item.vote_average,
        popularity: item.popularity, semantic_tags: item.semantic_tags,
    }));
    await fs.writeFile(`${OUTPUT_DIR}/client_database.json`, JSON.stringify(clientDatabase));
    console.log(`  Generated client-side database with ${clientDatabase.length} items.`);

    const index = { buildTimestamp: new Date().toISOString(), lists: Object.entries(BUILD_MATRIX).map(([id, { name }]) => ({ id, name })) };
    await fs.writeFile(path.join(OUTPUT_DIR, 'index.json'), JSON.stringify(index));
    console.log(`\nSuccessfully wrote index file.`);
}

async function main() {
    console.log('Starting IMDb Discovery Engine build process v5.1 (Final)...');
    const startTime = Date.now();
    try {
        await buildAndEnrichDataLake();
        await sliceDataMartsFromLake();
        const duration = (Date.now() - startTime) / 1000;
        console.log(`\n✅ Build process successful! Took ${duration.toFixed(2)} seconds.`);
    } catch (error) {
        console.error('\n❌ FATAL ERROR during build process:', error);
        process.exit(1);
    } finally {
        await fs.rm(TEMP_DIR, { recursive: true, force: true });
    }
}

main();
