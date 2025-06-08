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
const MAX_CONCURRENT_ENRICHMENTS = 20;

const DATASETS = {
    basics: { url: 'https://datasets.imdbws.com/title.basics.tsv.gz', local: 'title.basics.tsv' },
    akas: { url: 'https://datasets.imdbws.com/title.akas.tsv.gz', local: 'title.akas.tsv' },
    ratings: { url: 'https://datasets.imdbws.com/title.ratings.tsv.gz', local: 'title.ratings.tsv' },
};

const BUILD_MATRIX = {
    official_top_movies: { name: '高分电影', filters: { types: ['movie'], minVotes: 25000 }, sortBy: 'rating', limit: 250 },
    official_top_tv: { name: '高分剧集', filters: { types: ['tvseries', 'tvminiseries'], minVotes: 10000 }, sortBy: 'rating', limit: 250 },
    jp_anime_top: { name: '高分日漫', filters: { types: ['movie', 'tvseries'], regions: ['JP'], genres: ['Animation'] }, sortBy: 'rating', minVotes: 1000, limit: 100 },
    kr_tv_top: { name: '高分韩剧', filters: { types: ['tvseries'], regions: ['KR'] }, sortBy: 'rating', minVotes: 1000, limit: 100 },
    cn_movie_top: { name: '高分国产电影', filters: { types: ['movie'], regions: ['CN', 'HK', 'TW'] }, sortBy: 'rating', minVotes: 1000, limit: 100 },
    cn_tv_top: { name: '高分国产剧', filters: { types: ['tvseries'], regions: ['CN', 'HK', 'TW'] }, sortBy: 'rating', minVotes: 500, limit: 100 },
    theme_cyberpunk: { name: '赛博朋克精选', filters: { genres: ['Sci-Fi'], keywords: ['cyberpunk', 'dystopia'] }, sortBy: 'rating', limit: 50 },
    theme_zombie: { name: '僵尸末日', filters: { genres: ['Horror'], keywords: ['zombie'] }, sortBy: 'rating', limit: 50 },
    theme_wuxia: { name: '武侠世界', filters: { genres: ['Action', 'Adventure'], regions: ['CN', 'HK', 'TW'], keywords: ['wuxia', 'martial-arts'] }, sortBy: 'rating', limit: 50 },
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

async function buildLightweightIndexes() {
    console.log('\nPHASE 1: Building lightweight indexes from datasets...');
    const ratingsIndex = new Map();
    const akasIndex = new Map();

    // 1. 加载 ratings 数据
    const ratingsPath = path.join(DATASET_DIR, DATASETS.ratings.local);
    await downloadAndUnzip(DATASETS.ratings.url, ratingsPath);
    console.log(`  Reading ${path.basename(ratingsPath)}...`);
    await processTsvByLine(ratingsPath, (line) => {
        const [tconst, averageRating, numVotes] = line.split('\t');
        ratingsIndex.set(tconst, {
            rating: parseFloat(averageRating) || 0,
            votes: parseInt(numVotes, 10) || 0,
        });
    });
    console.log(`  Ratings index built with ${ratingsIndex.size} entries.`);

    // 2. 加载 akas 数据
    const akasPath = path.join(DATASET_DIR, DATASETS.akas.local);
    await downloadAndUnzip(DATASETS.akas.url, akasPath);
    console.log(`  Reading ${path.basename(akasPath)}...`);
    await processTsvByLine(akasPath, (line) => {
        const [titleId, , , region] = line.split('\t');
        if (!region || region === '\\N') return;
        if (!akasIndex.has(titleId)) {
            akasIndex.set(titleId, new Set());
        }
        akasIndex.get(titleId).add(region);
    });
    console.log(`  Akas index built with ${akasIndex.size} entries.`);

    return { ratingsIndex, akasIndex };
}

async function main() {
    console.log('Starting IMDb Dataset Engine build process v5.0 (Memory Optimized)...');
    const startTime = Date.now();

    try {
        await fs.mkdir(DATASET_DIR, { recursive: true });
        const { ratingsIndex, akasIndex } = await buildLightweightIndexes();

        console.log('\nPHASE 2: Streaming and filtering basics dataset...');
        const listBuckets = Object.fromEntries(Object.keys(BUILD_MATRIX).map(key => [key, []]));
        const allImdbIdsToEnrich = new Set();

        const basicsPath = path.join(DATASET_DIR, DATASETS.basics.local);
        await downloadAndUnzip(DATASETS.basics.url, basicsPath);
        console.log(`  Reading and processing ${path.basename(basicsPath)} line by line...`);

        await processTsvByLine(basicsPath, (line) => {
            const [tconst, titleType, , , isAdult, , , genres] = line.split('\t');
            if (!tconst.startsWith('tt') || isAdult === '1') return;

            const ratingInfo = ratingsIndex.get(tconst) || { rating: 0, votes: 0 };
            const regionInfo = akasIndex.get(tconst) || new Set();
            const itemGenres = genres ? genres.split(',') : [];

            const item = {
                id: tconst,
                type: titleType.toLowerCase(),
                genres: itemGenres,
                regions: regionInfo,
                ...ratingInfo
            };

            for (const [key, config] of Object.entries(BUILD_MATRIX)) {
                const { types, minVotes = 0, regions, genres: filterGenres } = config.filters;
                if (types && !types.includes(item.type)) continue;
                if ((item.votes || 0) < minVotes) continue;
                if (regions && ![...item.regions].some(r => regions.includes(r))) continue;
                if (filterGenres && !filterGenres.some(g => item.genres.includes(g))) continue;
                
                listBuckets[key].push(item);
            }
        });
        console.log('  Finished processing basics dataset.');

        console.log('\nPHASE 3: Sorting, slicing, and preparing for enrichment...');
        for (const [key, config] of Object.entries(BUILD_MATRIX)) {
            const bucket = listBuckets[key];
            bucket.sort((a, b) => (b[config.sortBy] || 0) - (a[config.sortBy] || 0));
            listBuckets[key] = bucket.slice(0, config.limit || 100);
            listBuckets[key].forEach(item => allImdbIdsToEnrich.add(item.id));
        }

        console.log(`\nPHASE 4: Enriching ${allImdbIdsToEnrich.size} unique items with TMDB data...`);
        const enrichedDataLake = new Map();
        const imdbIdArray = Array.from(allImdbIdsToEnrich);
        for (let i = 0; i < imdbIdArray.length; i += MAX_CONCURRENT_ENRICHMENTS) {
            const batch = imdbIdArray.slice(i, i + MAX_CONCURRENT_ENRICHMENTS);
            const promises = batch.map(id => findByImdbId(id).then(info => info ? getTmdbDetails(info.id, info.media_type) : null).then(details => analyzeAndTagItem(details)));
            const results = await Promise.allSettled(promises);
            results.forEach(r => r.status === 'fulfilled' && r.value && enrichedDataLake.set(r.value.imdb_id, r.value));
        }
        console.log(`  Enriched data lake contains ${enrichedDataLake.size} items.`);

        console.log('\nPHASE 5: Writing final data marts...');
        await fs.rm(OUTPUT_DIR, { recursive: true, force: true });
        await fs.mkdir(OUTPUT_DIR, { recursive: true });

        for (const [key, config] of Object.entries(BUILD_MATRIX)) {
            let listData = listBuckets[key].map(item => enrichedDataLake.get(item.id)).filter(Boolean);
            if (config.filters.keywords) {
                listData = listData.filter(item => config.filters.keywords.some(kw => item.semantic_tags.some(tag => tag.includes(kw))));
            }
            const outputPath = path.join(OUTPUT_DIR, `${key}.json`);
            await fs.writeFile(outputPath, JSON.stringify(listData));
            console.log(`  Generated list: ${config.name} -> ${path.basename(outputPath)} (${listData.length} items)`);
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
