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
const MAX_CONCURRENT_ENRICHMENTS = 25; // 稍微提高并发
const DATA_LAKE_FILE = path.join(TEMP_DIR, 'datalake.jsonl');
const FINAL_DATABASE_FILE = path.join(OUTPUT_DIR, 'database.json');

const DATASETS = {
    basics: { url: 'https://datasets.imdbws.com/title.basics.tsv.gz', local: 'title.basics.tsv' },
    akas: { url: 'https://datasets.imdbws.com/title.akas.tsv.gz', local: 'title.akas.tsv' },
    ratings: { url: 'https://datasets.imdbws.com/title.ratings.tsv.gz', local: 'title.ratings.tsv' },
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

    const akasPath = path.join(DATASET_DIR, DATASETS.akas.local);
    await downloadAndUnzip(DATASETS.akas.url, akasPath);
    await processTsvByLine(akasPath, (line) => {
        const [titleId, , , region] = line.split('\t');
        if (!region || region === '\\N') return;
        if (!akasIndex.has(titleId)) akasIndex.set(titleId, new Set());
        akasIndex.get(titleId).add(region);
    });

    console.log('\nPHASE 2: Streaming basics, filtering, and enriching to disk...');
    await fs.rm(TEMP_DIR, { recursive: true, force: true });
    await fs.mkdir(TEMP_DIR, { recursive: true });
    const writeStream = createWriteStream(DATA_LAKE_FILE, { flags: 'a' });
    
    const idPool = [];
    const basicsPath = path.join(DATASET_DIR, DATASETS.basics.local);
    await downloadAndUnzip(DATASETS.basics.url, basicsPath);
    await processTsvByLine(basicsPath, (line) => {
        const [tconst, titleType, , , isAdult, startYear] = line.split('\t');
        if (!tconst.startsWith('tt') || isAdult === '1') return;
        const ratingInfo = ratingsIndex.get(tconst);
        // 预筛选，只保留有足够信息和关注度的条目
        if (ratingInfo && ratingInfo.votes > 100 && parseInt(startYear, 10) > 1960) {
            idPool.push(tconst);
        }
    });
    console.log(`  Initial pool contains ${idPool.length} potentially interesting items.`);

    for (let i = 0; i < idPool.length; i += MAX_CONCURRENT_ENRICHMENTS) {
        const batch = idPool.slice(i, i + MAX_CONCURRENT_ENRICHMENTS);
        const promises = batch.map(id => {
            const ratingInfo = ratingsIndex.get(id);
            const regionInfo = akasIndex.get(id);
            const itemShell = { id, ...ratingInfo, regions: regionInfo };
            return findByImdbId(id)
                .then(info => info ? getTmdbDetails(info.id, info.media_type) : null)
                .then(details => analyzeAndTagItem(details));
        });
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
        console.log(`  Enriched and wrote batch ${Math.ceil((i + batch.length) / MAX_CONCURRENT_ENRICHMENTS)} / ${Math.ceil(idPool.length / MAX_CONCURRENT_ENRICHMENTS)}`);
    }
    await new Promise(resolve => writeStream.end(resolve));
    console.log(`  Data Lake written to ${DATA_LAKE_FILE}`);
}

async function assembleFinalDatabase() {
    console.log('\nPHASE 3: Assembling final database from data lake...');
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
    console.log('Starting IMDb Discovery Engine build process v7.0 (Genesis)...');
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
