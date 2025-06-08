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
const TEMP_DIR = './temp';
const OUTPUT_DIR = './dist';
const MAX_CONCURRENT_ENRICHMENTS = 50; 
const DATA_LAKE_FILE = path.join(TEMP_DIR, 'datalake.jsonl');
const FINAL_DATABASE_FILE = path.join(OUTPUT_DIR, 'database.json');

const DATASETS = {
    basics: { url: 'https://datasets.imdbws.com/title.basics.tsv.gz', local: 'title.basics.tsv' },
    akas: { url: 'https://datasets.imdbws.com/title.akas.tsv.gz', local: 'title.akas.tsv' },
    ratings: { url: 'https://datasets.imdbws.com/title.ratings.tsv.gz', local: 'title.ratings.tsv' },
};

// --- 抓取任务矩阵 ---
const CRAWL_MATRIX = [
    // 核心榜单 
    { path: '/chart/moviemeter/', limit: 500 }, { path: '/chart/top/', limit: 500 },
    { path: '/chart/tvmeter/', limit: 500 }, { path: '/chart/toptv/', limit: 500 },
    // 亚洲地区专项
    { params: { countries: 'jp', title_type: 'feature,tv_series', sort: 'user_rating,desc' }, limit: 250 },
    { params: { countries: 'kr', title_type: 'feature,tv_series', sort: 'user_rating,desc' }, limit: 250 },
    { params: { countries: 'cn,hk,tw', title_type: 'feature,tv_series', sort: 'user_rating,desc' }, limit: 250 },
    { params: { countries: 'in', title_type: 'feature', sort: 'user_rating,desc' }, limit: 250 },
    // 欧美地区专项
    { params: { countries: 'us', title_type: 'feature,tv_series', sort: 'user_rating,desc' }, limit: 250 },
    { params: { countries: 'gb', title_type: 'feature,tv_series', sort: 'user_rating,desc' }, limit: 250 },
    // 核心类型专项
    { params: { genres: 'sci-fi', sort: 'user_rating,desc' }, limit: 250 },
    { params: { genres: 'horror', sort: 'user_rating,desc' }, limit: 250 },
    { params: { genres: 'animation', sort: 'user_rating,desc' }, limit: 250 },
    { params: { genres: 'comedy', sort: 'user_rating,desc' }, limit: 250 },
    { params: { genres: 'action', sort: 'user_rating,desc' }, limit: 250 },
    { params: { genres: 'documentary', sort: 'user_rating,desc' }, limit: 250 },
    { params: { genres: 'romance', sort: 'user_rating,desc' }, limit: 250 },
    { params: { genres: 'thriller', sort: 'user_rating,desc' }, limit: 250 },
];

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

async function processInParallel(items, concurrency, task) {
    const queue = [...items];
    let processedCount = 0;

    const worker = async () => {
        while (queue.length > 0) {
            const item = queue.shift();
            if (item) {
                await task(item);
                processedCount++;
                if (processedCount % 100 === 0 || processedCount === items.length) {
                    console.log(`  Progress: ${processedCount} / ${items.length}`);
                }
            }
        }
    };

    const workers = [];
    for (let i = 0; i < concurrency; i++) {
        workers.push(worker());
    }

    await Promise.all(workers);
}

async function buildAndEnrichToDisk() {
    console.log('\nPHASE 1: Building lightweight indexes...');
    const ratingsIndex = new Map();
    const akasIndex = new Map();

    const ratingsPath = path.join(DATASET_DIR, DATASETS.ratings.local);
    await downloadAndUnzip(DATASETS.ratings.url, ratingsPath);
    await processTsvByLine(ratingsPath, (line) => {
        const [tconst, averageRating, numVotes] = line.split('\t');
        ratingsIndex.set(tconst, { rating: parseFloat(averageRating) || 0, votes: parseInt(numVotes, 10) || 0 });
    });
    console.log(`  Ratings index built with ${ratingsIndex.size} entries.`);

    const akasPath = path.join(DATASET_DIR, DATASETS.akas.local);
    await downloadAndUnzip(DATASETS.akas.url, akasPath);
    await processTsvByLine(akasPath, (line) => {
        const [titleId, , , region] = line.split('\t');
        if (!region || region === '\\N') return;
        if (!akasIndex.has(titleId)) akasIndex.set(titleId, new Set());
        akasIndex.get(titleId).add(region);
    });
    console.log(`  Akas index built with ${akasIndex.size} entries.`);

    console.log('\nPHASE 2: Streaming basics and filtering to create ID pool...');
    const idPool = new Set();
    const basicsPath = path.join(DATASET_DIR, DATASETS.basics.local);
    await downloadAndUnzip(DATASETS.basics.url, basicsPath);
    await processTsvByLine(basicsPath, (line) => {
        const [tconst, titleType, , , isAdult, startYear] = line.split('\t');
        if (!tconst.startsWith('tt') || isAdult === '1') return;
        const ratingInfo = ratingsIndex.get(tconst);
        if (ratingInfo && ratingInfo.votes > 100 && parseInt(startYear, 10) > 1970) {
            idPool.add(tconst);
        }
    });
    console.log(`  Initial pool contains ${idPool.size} potentially interesting items.`);

    console.log(`\nPHASE 3: Enriching ${idPool.size} unique items via concurrent pipeline...`);
    await fs.rm(TEMP_DIR, { recursive: true, force: true });
    await fs.mkdir(TEMP_DIR, { recursive: true });
    const writeStream = createWriteStream(DATA_LAKE_FILE, { flags: 'a' });

    const enrichmentTask = async (id) => {
        const details = await findByImdbId(id)
            .then(info => info ? getTmdbDetails(info.id, info.media_type) : null)
            .then(details => analyzeAndTagItem(details));
        if (details) {
            writeStream.write(JSON.stringify(details) + '\n');
        }
    };

    await processInParallel(Array.from(idPool), MAX_CONCURRENT_ENRICHMENTS, enrichmentTask);

    await new Promise(resolve => writeStream.end(resolve));
    console.log(`  Data Lake written to ${DATA_LAKE_FILE}`);
}

async function assembleFinalDatabase() {
    console.log('\nPHASE 4: Assembling final database from data lake...');
    await fs.rm(OUTPUT_DIR, { recursive: true, force: true });
    await fs.mkdir(OUTPUT_DIR, { recursive: true });

    const writeStream = createWriteStream(FINAL_DATABASE_FILE);
    writeStream.write(`{"buildTimestamp":"${new Date().toISOString()}","database":[`);

    const rl = readline.createInterface({ input: createReadStream(DATA_LAKE_FILE), crlfDelay: Infinity });
    let firstLine = true;
    for await (const line of rl) {
        if (!line.trim()) continue;
        if (!firstLine) {
            writeStream.write(',');
        }
        writeStream.write(line);
        firstLine = false;
    }

    writeStream.write(']}');
    await new Promise(resolve => writeStream.end(resolve));
    console.log(`  Final database written to ${FINAL_DATABASE_FILE}`);
}

async function main() {
    console.log('Starting IMDb Discovery Engine build process v7.1 (Velocity)...');
    const startTime = Date.now();
    try {
        await buildAndEnrichToDisk();
        await assembleFinalDatabase();
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
