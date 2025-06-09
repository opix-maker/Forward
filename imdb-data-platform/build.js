import fs from 'fs/promises';
import { constants as fsConstants } from 'fs';
import path from 'path';
import fetch from 'node-fetch';
import { pipeline } from 'stream/promises';
import { createWriteStream, createReadStream } from 'fs';
import zlib from 'zlib';
import readline from 'readline';
import { findByImdbId, getTmdbDetails } from './src/utils/tmdb_api.js'; 
import { analyzeAndTagItem } from './src/core/analyzer.js';

// --- CONFIGURATION ---
const MAX_CONCURRENT_ENRICHMENTS = 80;
const MAX_DOWNLOAD_AGE_MS = 20 * 60 * 60 * 1000; // 20 hours
const MIN_VOTES = 2500; 
const MIN_YEAR = 1990; 
const MIN_RATING = 6.0; 
const ALLOWED_TITLE_TYPES = new Set(['movie', 'tvSeries', 'tvMiniSeries', 'tvMovie']);
const CURRENT_YEAR = new Date().getFullYear();
const RECENT_YEAR_THRESHOLD = CURRENT_YEAR - 1;

// --- PATHS ---
const DATASET_DIR = './datasets';
const TEMP_DIR = './temp';
const FINAL_OUTPUT_DIR = './dist'; // 最终输出目录
const DATA_LAKE_FILE = path.join(TEMP_DIR, 'datalake.jsonl');

const DATASETS = {
    basics: { url: 'https://datasets.imdbws.com/title.basics.tsv.gz', local: 'title.basics.tsv' },
    ratings: { url: 'https://datasets.imdbws.com/title.ratings.tsv.gz', local: 'title.ratings.tsv' },
};

// --- 分片配置 ---
const REGIONS = ['all', 'region:chinese', 'region:us-eu', 'region:east-asia', 'country:cn', 'country:hk', 'country:tw', 'country:us', 'country:gb', 'country:jp', 'country:kr', 'country:fr', 'country:de', 'country:ca', 'country:au'];
const GENRES_AND_THEMES = ['genre:爱情', 'genre:冒险', 'genre:悬疑', 'genre:惊悚', 'genre:恐怖', 'genre:科幻', 'genre:奇幻', 'genre:动作', 'genre:喜剧', 'genre:剧情', 'genre:历史', 'genre:战争', 'genre:犯罪', 'theme:whodunit', 'theme:spy', 'theme:courtroom', 'theme:slice-of-life', 'theme:wuxia', 'theme:superhero', 'theme:cyberpunk', 'theme:space-opera', 'theme:time-travel', 'theme:post-apocalyptic', 'theme:mecha', 'theme:zombie', 'theme:monster', 'theme:ghost', 'theme:magic', 'theme:gangster', 'theme:film-noir', 'theme:serial-killer', 'theme:xianxia', 'theme:kaiju', 'theme:isekai'];
const YEARS = Array.from({length: CURRENT_YEAR - 1990 + 1}, (_, i) => 1990 + i).reverse();

// --- 辅助函数 ---
async function downloadAndUnzipWithCache(url, localPath, maxAgeMs) {
     const dir = path.dirname(localPath);
     await fs.mkdir(dir, { recursive: true });
    try {
        const stats = await fs.stat(localPath);
        if (Date.now() - stats.mtimeMs < maxAgeMs) {
            console.log(`  Cache hit for ${path.basename(localPath)}.`);
            return;
        }
    } catch (e) { /* no cache */ }
    console.log(`  Downloading from: ${url}`);
    const response = await fetch(url, { headers: { 'User-Agent': 'IMDb-Builder/1.0' } });
    if (!response.ok) throw new Error(`Failed to download ${url}: ${response.statusText}`);
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
        if (line && line.includes('\t')) processor(line);
    }
}

async function processInParallel(items, concurrency, task) {
    const queue = [...items]; let processedCount = 0; const totalCount = items.length;
    const worker = async () => {
        while (queue.length > 0) {
            const item = queue.shift();
            if (item) {
                await task(item);
                processedCount++;
                if (processedCount % 100 === 0 || processedCount === totalCount) {
                    process.stdout.write(`  Progress: ${processedCount} / ${totalCount} \r`);
                }
            }
        }
    };
    const workers = Array(concurrency).fill(null).map(() => worker());
    await Promise.all(workers);
    process.stdout.write('\n');
}

// --- 主要构建流程 ---

async function buildDataLake() {
    console.log('\nPHASE 1: Building indexes & Caching...');
    const ratingsIndex = new Map();
    const ratingsPath = path.join(DATASET_DIR, DATASETS.ratings.local);
    await downloadAndUnzipWithCache(DATASETS.ratings.url, ratingsPath, MAX_DOWNLOAD_AGE_MS);
    await processTsvByLine(ratingsPath, (line) => {
        const [tconst, averageRating, numVotes] = line.split('\t');
        const votes = parseInt(numVotes, 10) || 0; const rating = parseFloat(averageRating) || 0;
        if (votes >= MIN_VOTES && rating >= MIN_RATING) ratingsIndex.set(tconst, { rating, votes });
    });
    console.log(`  Ratings index built with ${ratingsIndex.size} items.`);

    console.log(`\nPHASE 2: Streaming basics & filtering...`);
    const idPool = new Set();
    const basicsPath = path.join(DATASET_DIR, DATASETS.basics.local);
    await downloadAndUnzipWithCache(DATASETS.basics.url, basicsPath, MAX_DOWNLOAD_AGE_MS);
    await processTsvByLine(basicsPath, (line) => {
        const [tconst, titleType, , , isAdult, startYear] = line.split('\t');
        const year = parseInt(startYear, 10);
        if (!tconst.startsWith('tt') || isAdult === '1' || !ALLOWED_TITLE_TYPES.has(titleType) || isNaN(year) || year < MIN_YEAR || !ratingsIndex.has(tconst)) return;
        idPool.add(tconst);
    });
    console.log(`  Filtered ID pool contains ${idPool.size} items to enrich.`);

    console.log(`\nPHASE 3: Enriching items via TMDB API...`);
    await fs.rm(TEMP_DIR, { recursive: true, force: true }); await fs.mkdir(TEMP_DIR, { recursive: true });
    const writeStream = createWriteStream(DATA_LAKE_FILE, { flags: 'a' });
    const enrichmentTask = async (id) => {
         try {
            const info = await findByImdbId(id);
            if (!info || !info.id || !info.media_type) return;
            const details = await getTmdbDetails(info.id, info.media_type);
            if (details) {
                const analyzedItem = analyzeAndTagItem(details);
                if (analyzedItem) {
                    writeStream.write(JSON.stringify(analyzedItem) + '\n');
                }
            }
        } catch (error) {
            console.warn(`  Skipping ID ${id} due to enrichment error: ${error.message}`);
        }
    };
    await processInParallel(Array.from(idPool), MAX_CONCURRENT_ENRICHMENTS, enrichmentTask);
    await new Promise(resolve => writeStream.end(resolve));
    console.log(`  Data Lake written to ${DATA_LAKE_FILE}`);
}

async function shardDatabase() {
    console.log('\nPHASE 4: Sharding database from data lake...');
    await fs.rm(FINAL_OUTPUT_DIR, { recursive: true, force: true });
    await fs.mkdir(FINAL_OUTPUT_DIR, { recursive: true });

    const database = [];
    const rl = readline.createInterface({ input: createReadStream(DATA_LAKE_FILE), crlfDelay: Infinity });
    for await (const line of rl) { if (line.trim()) database.push(JSON.parse(line)); }
    console.log(`  Loaded ${database.length} items from data lake.`);

    // --- 计算分数和预处理 ---
    const validForStats = database.filter(i => i.vote_count > 100);
    const totalRating = validForStats.reduce((sum, item) => sum + (item.vote_average || 0), 0);
    const GLOBAL_AVERAGE_RATING = validForStats.length > 0 ? totalRating / validForStats.length : 6.8;
    const sortedVotes = validForStats.map(i => i.vote_count).sort((a,b) => a - b);
    const MINIMUM_VOTES_THRESHOLD = sortedVotes[Math.floor(sortedVotes.length * 0.75)] || 500;
    console.log(`  Global Stats: AvgRating=${GLOBAL_AVERAGE_RATING.toFixed(2)}, MinVotes=${MINIMUM_VOTES_THRESHOLD}`);

    database.forEach(item => {
        const pop = item.popularity || 0;
        const year = item.release_year || 1970;
        const R = item.vote_average || 0;
        const v = item.vote_count || 0;
        const yearDiff = Math.max(0, CURRENT_YEAR - year);
        const bayesianRating = (v / (v + MINIMUM_VOTES_THRESHOLD)) * R + (MINIMUM_VOTES_THRESHOLD / (v + MINIMUM_VOTES_THRESHOLD)) * GLOBAL_AVERAGE_RATING;
        item.hotness_score = Math.log10(pop + 1) * (1 / Math.sqrt(yearDiff + 2)) * bayesianRating;
        item.default_order = pop;
        // 预处理 mediaType
        const isTV = item.semantic_tags.includes('type:tv');
        item.mediaType = isTV ? 'tv' : 'movie';
    });

    const writeShard = async (filePath, data) => {
        
        const fullPath = path.join(FINAL_OUTPUT_DIR, filePath);
        const dir = path.dirname(fullPath);
        
        await fs.mkdir(dir, { recursive: true });
        const minifiedData = data.map(item => ({
             id: item.id,
             p: item.poster_path, // poster_path -> p
             b: item.backdrop_path, // backdrop_path -> b
             t: item.title, // title -> t
             r: item.vote_average, // vote_average -> r
             y: item.release_year, // release_year -> y
             hs: parseFloat(item.hotness_score.toFixed(3)), // hotness_score -> hs
             d: parseFloat(item.default_order.toFixed(3)), // default_order -> d
             mt: item.mediaType, // mediaType -> mt
        }));
        
        await fs.writeFile(fullPath, JSON.stringify(minifiedData));
    };

    // --- Generate Shards ---
    console.log('  Generating shards...');
    // 1. 近期热门
    await writeShard('recent_hot.json', database.filter(i => i.release_year >= RECENT_YEAR_THRESHOLD));
    
    // 2. 按类型/主题
    for (const tag of GENRES_AND_THEMES) {
        const filename = tag.replace(':', '_') + '.json';
        await writeShard(path.join('by_tag', filename), database.filter(i => i.semantic_tags.includes(tag)));
    }
    
    // 3. 按年份
    for (const year of YEARS) {
        await writeShard(path.join('by_year', `${year}.json`), database.filter(i => i.release_year === year));
    }
    
    // 4. 按电影/剧集/动画 + 地区
    const types = [
        { name: 'movies', tags: ['type:movie'], exclude: [] },
        { name: 'tvseries', tags: ['type:tv'], exclude: ['type:animation'] }, // 日剧/日漫分离
        { name: 'anime', tags: ['type:animation'], exclude: [] },
    ];
    for (const type of types) {
        let baseData = database.filter(i => type.tags.every(t => i.semantic_tags.includes(t)) && !type.exclude.some(e => i.semantic_tags.includes(e)));
        for (const region of REGIONS) {
            let data = (region === 'all') ? baseData : baseData.filter(i => i.semantic_tags.includes(region));
            const filename = region.replace(':', '_') + '.json';
            await writeShard(path.join(type.name, filename), data);
        }
    }
    
    await fs.writeFile(path.join(FINAL_OUTPUT_DIR, 'manifest.json'), JSON.stringify({
        buildTimestamp: new Date().toISOString(),
        regions: REGIONS,
        tags: GENRES_AND_THEMES,
        years: YEARS,
    }));
    console.log(`  ✅ Sharding complete. Files written to ${FINAL_OUTPUT_DIR}`);
}

async function main() {
    console.log('Starting IMDb Sharded Build Process...');
    const startTime = Date.now();
    try {
        await buildDataLake();
        await shardDatabase();
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
