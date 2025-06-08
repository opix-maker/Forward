import fs from 'fs/promises';
import path from 'path';
import fetch from 'node-fetch';
import { pipeline } from 'stream/promises';
import { createWriteStream, createReadStream } from 'fs';
import zlib from 'zlib';
import readline from 'readline';
import { findByImdbId, getTmdbDetails } from './src/utils/tmdb_api.js';
import { analyzeAndTagItem } from './src/core/analyzer.js';

const DATASET_DIR = './datasets';
const OUTPUT_DIR = './dist';
const MAX_CONCURRENT_ENRICHMENTS = 100; // 可以设置得更高，因为总数少了

const DATASETS = {
    basics: { url: 'https://datasets.imdbws.com/title.basics.tsv.gz', local: 'title.basics.tsv' },
    akas: { url: 'https://datasets.imdbws.com/title.akas.tsv.gz', local: 'title.akas.tsv' },
    ratings: { url: 'https://datasets.imdbws.com/title.ratings.tsv.gz', local: 'title.ratings.tsv' },
};

// --- 数据切片矩阵：定义最终输出的JSON文件 ---
const BUILD_MATRIX = {
    // 官方榜单
    top_movies_250: { name: 'IMDb Top 250 电影', filters: { types: ['movie'], minVotes: 25000 }, sortBy: 'rating', limit: 250 },
    top_tv_250: { name: 'IMDb Top 250 剧集', filters: { types: ['tvseries', 'tvminiseries'], minVotes: 10000 }, sortBy: 'rating', limit: 250 },
    // 亚洲精选
    top_jp_anime: { name: '高分日系动画', filters: { types: ['movie', 'tvseries'], regions: ['JP'], genres: ['Animation'] }, sortBy: 'rating', minVotes: 1000, limit: 100 },
    top_kr_tv: { name: '高分韩剧', filters: { types: ['tvseries'], regions: ['KR'] }, sortBy: 'rating', minVotes: 1000, limit: 100 },
    top_cn_movie: { name: '高分国产电影', filters: { types: ['movie'], regions: ['CN', 'HK', 'TW'] }, sortBy: 'rating', minVotes: 1000, limit: 100 },
    top_cn_tv: { name: '高分国产剧', filters: { types: ['tvseries'], regions: ['CN', 'HK', 'TW'] }, sortBy: 'rating', minVotes: 500, limit: 100 },
    // 主题探索
    theme_cyberpunk: { name: '赛博朋克精选', filters: { genres: ['Sci-Fi'], keywords: ['cyberpunk', 'dystopia'] }, sortBy: 'rating', limit: 50 },
    theme_zombie: { name: '僵尸末日', filters: { genres: ['Horror'], keywords: ['zombie'] }, sortBy: 'rating', limit: 50 },
    theme_wuxia: { name: '武侠世界', filters: { genres: ['Action', 'Adventure'], regions: ['CN', 'HK', 'TW'], keywords: ['wuxia', 'martial-arts'] }, sortBy: 'rating', limit: 50 },
    // 近期热门
    weekly_trending: { name: '本周热门', crawl: true, path: '/chart/moviemeter/', limit: 100 },
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

async function buildLocalDatabase() {
    console.log('\nPHASE 1: Building local IMDb database from datasets...');
    const db = new Map();

    const ratingsPath = path.join(DATASET_DIR, DATASETS.ratings.local);
    await downloadAndUnzip(DATASETS.ratings.url, ratingsPath);
    await processTsvByLine(ratingsPath, (line) => {
        const [tconst, averageRating, numVotes] = line.split('\t');
        db.set(tconst, { rating: parseFloat(averageRating) || 0, votes: parseInt(numVotes, 10) || 0 });
    });

    const akasPath = path.join(DATASET_DIR, DATASETS.akas.local);
    await downloadAndUnzip(DATASETS.akas.url, akasPath);
    await processTsvByLine(akasPath, (line) => {
        const [titleId, , , region] = line.split('\t');
        if (!region || region === '\\N' || !db.has(titleId)) return;
        if (!db.get(titleId).regions) db.get(titleId).regions = new Set();
        db.get(titleId).regions.add(region);
    });

    const basicsPath = path.join(DATASET_DIR, DATASETS.basics.local);
    await downloadAndUnzip(DATASETS.basics.url, basicsPath);
    await processTsvByLine(basicsPath, (line) => {
        const [tconst, titleType, , , isAdult, , , genres] = line.split('\t');
        if (!db.has(tconst) || isAdult === '1') {
            if (db.has(tconst)) db.delete(tconst); // 移除成人内容
            return;
        };
        const entry = db.get(tconst);
        entry.type = titleType.toLowerCase();
        entry.genres = genres ? genres.split(',') : [];
    });
    
    console.log(`  Local database built with ${db.size} entries.`);
    return Array.from(db.values()).map(item => ({ id: item.id, ...item }));
}

async function processInParallel(items, concurrency, task) {
    const queue = [...items];
    const results = [];
    let processedCount = 0;
    const totalCount = items.length;

    const worker = async () => {
        while (queue.length > 0) {
            const item = queue.shift();
            if (item) {
                try {
                    const result = await task(item);
                    if (result) results.push(result);
                } catch (error) {
                    console.warn(`  Task for item ${item} failed: ${error.message}`);
                }
                processedCount++;
                if (processedCount % 100 === 0 || processedCount === totalCount) {
                    console.log(`  Progress: ${processedCount} / ${totalCount}`);
                }
            }
        }
    };

    const workers = [];
    for (let i = 0; i < concurrency; i++) {
        workers.push(worker());
    }
    await Promise.all(workers);
    return results;
}

async function main() {
    console.log('Starting IMDb Discovery Engine build process v9.0 (Apex)...');
    const startTime = Date.now();
    try {
        await fs.mkdir(DATASET_DIR, { recursive: true });
        const localDB = await buildLocalDatabase();

        console.log('\nPHASE 2: Generating ID lists from local database...');
        const listBuckets = {};
        const allImdbIdsToEnrich = new Set();

        for (const [key, config] of Object.entries(BUILD_MATRIX)) {
            let results = localDB;
            const { types, minVotes = 0, regions, genres } = config.filters;
            
            if (types) results = results.filter(item => types.includes(item.type));
            if (minVotes) results = results.filter(item => item.votes >= minVotes);
            if (regions) results = results.filter(item => item.regions && [...item.regions].some(r => regions.includes(r)));
            if (genres) results = results.filter(item => item.genres && genres.some(g => item.genres.includes(g)));
            
            results.sort((a, b) => (b[config.sortBy] || 0) - (a[config.sortBy] || 0));
            
            const idList = results.slice(0, config.limit || 100).map(item => item.id);
            listBuckets[key] = idList;
            idList.forEach(id => allImdbIdsToEnrich.add(id));
        }

        console.log(`\nPHASE 3: Enriching ${allImdbIdsToEnrich.size} unique items from final lists...`);
        const enrichedDataLake = new Map();
        const enrichmentTask = async (id) => {
            const findPromise = findByImdbId(id);
            const detailsPromise = findPromise.then(info => info ? getTmdbDetails(info.id, info.media_type) : null);
            const details = await detailsPromise;
            if (details) {
                const analyzedItem = analyzeAndTagItem(details);
                if (analyzedItem) {
                    enrichedDataLake.set(id, analyzedItem);
                }
            }
        };
        await processInParallel(Array.from(allImdbIdsToEnrich), MAX_CONCURRENT_ENRICHMENTS, enrichmentTask);
        console.log(`  Enriched data lake contains ${enrichedDataLake.size} items.`);

        console.log('\nPHASE 4: Writing final data marts...');
        await fs.rm(OUTPUT_DIR, { recursive: true, force: true });
        await fs.mkdir(OUTPUT_DIR, { recursive: true });

        for (const [key, config] of Object.entries(BUILD_MATRIX)) {
            const finalData = listBuckets[key]
                .map(id => enrichedDataLake.get(id))
                .filter(Boolean);
            
            // 关键词筛选需要在增强后进行
            if (config.filters.keywords) {
                const filteredByKeyword = finalData.filter(item => config.filters.keywords.some(kw => item.semantic_tags.some(tag => tag.includes(kw))));
                await fs.writeFile(`${OUTPUT_DIR}/${key}.json`, JSON.stringify(filteredByKeyword));
                console.log(`  Generated list: ${config.name} -> ${key}.json (${filteredByKeyword.length} items)`);
            } else {
                await fs.writeFile(`${OUTPUT_DIR}/${key}.json`, JSON.stringify(finalData));
                console.log(`  Generated list: ${config.name} -> ${key}.json (${finalData.length} items)`);
            }
        }

        const index = { buildTimestamp: new Date().toISOString(), lists: Object.entries(BUILD_MATRIX).map(([id, { name }]) => ({ id, name })) };
        await fs.writeFile(path.join(OUTPUT_DIR, 'index.json'), JSON.stringify(index));
        console.log(`\nSuccessfully wrote index file.`);

        const duration = (Date.now() - startTime) / 1000;
        console.log(`\n✅ Build process successful! Took ${duration.toFixed(2)} seconds.`);

    } catch (error) {
        console.error('\n❌ FATAL ERROR during build process:', error);
        process.exit(1);
    }
}

main();
