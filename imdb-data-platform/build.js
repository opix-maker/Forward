import fs from 'fs/promises';
import path from 'path';
import fetch from 'node-fetch';
import { pipeline } from 'stream/promises';
import { createWriteStream } from 'fs';
import zlib from 'zlib'; 
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
    const response = await fetch(url, {
        headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36' }
    });
    if (!response.ok) {
        const errorBody = await response.text();
        throw new Error(`Failed to download ${url} - Status: ${response.status} ${response.statusText}. Body: ${errorBody}`);
    }
    
    // ===================================================================
    //  核心修复：使用Node.js原生流管道进行下载和解压，不再依赖第三方库
    // ===================================================================
    const gunzip = zlib.createGunzip();
    const destination = createWriteStream(localPath);
    await pipeline(response.body, gunzip, destination);
    
    console.log(`  Download and unzip complete for ${path.basename(localPath)}.`);
}

async function loadDataset(name) {
    const config = DATASETS[name];
    const localPath = path.join(DATASET_DIR, config.local);
    try {
        await fs.access(localPath);
        console.log(`  Dataset '${name}' found locally.`);
    } catch {
        console.log(`  Dataset '${name}' not found. Downloading...`);
        await downloadAndUnzip(config.url, localPath);
    }
    console.log(`  Reading ${path.basename(localPath)}...`);
    return fs.readFile(localPath, 'utf-8');
}

async function buildLocalDatabase() {
    console.log('\nPHASE 1: Building local IMDb database from datasets...');
    const db = new Map();
    const basicsTsv = await loadDataset('basics');
    basicsTsv.split('\n').forEach(line => {
        const [tconst, titleType, primaryTitle, , isAdult, startYear, , genres] = line.split('\t');
        if (tconst === 'tconst' || !tconst.startsWith('tt') || isAdult === '1') return;
        db.set(tconst, { id: tconst, type: titleType.toLowerCase(), title: primaryTitle, year: parseInt(startYear, 10) || null, genres: genres ? genres.split(',') : [], regions: new Set() });
    });
    console.log(`  Processed ${db.size} basic title entries.`);
    const akasTsv = await loadDataset('akas');
    akasTsv.split('\n').forEach(line => {
        const [titleId, , , region] = line.split('\t');
        if (titleId === 'titleId' || !region || region === '\\N' || !db.has(titleId)) return;
        db.get(titleId).regions.add(region);
    });
    console.log(`  Enriched with regional (akas) data.`);
    const ratingsTsv = await loadDataset('ratings');
    ratingsTsv.split('\n').forEach(line => {
        const [tconst, averageRating, numVotes] = line.split('\t');
        if (tconst === 'tconst' || !db.has(tconst)) return;
        db.get(tconst).rating = parseFloat(averageRating) || 0;
        db.get(tconst).votes = parseInt(numVotes, 10) || 0;
    });
    console.log(`  Enriched with ratings data.`);
    return Array.from(db.values());
}

function queryDatabase(db, { types, minVotes = 0, regions, genres }) {
    return db.filter(item => {
        if (types && !types.includes(item.type)) return false;
        if (minVotes && (item.votes || 0) < minVotes) return false;
        if (regions && ![...item.regions].some(r => regions.includes(r))) return false;
        if (genres && !genres.some(g => item.genres.includes(g))) return false;
        return true;
    });
}

async function main() {
    console.log('Starting IMDb Dataset Engine build process v4.4 (Native Unzip)...');
    const startTime = Date.now();
    try {
        await fs.mkdir(DATASET_DIR, { recursive: true });
        const localDB = await buildLocalDatabase();
        console.log('\nPHASE 2: Querying database and enriching with TMDB...');
        const allImdbIdsToEnrich = new Set();
        const queryResults = {};
        for (const [key, config] of Object.entries(BUILD_MATRIX)) {
            let results = queryDatabase(localDB, config.filters);
            results.sort((a, b) => (b[config.sortBy] || 0) - (a[config.sortBy] || 0));
            queryResults[key] = results.slice(0, config.limit || 100);
            queryResults[key].forEach(item => allImdbIdsToEnrich.add(item.id));
        }
        const enrichedDataLake = new Map();
        const imdbIdArray = Array.from(allImdbIdsToEnrich);
        console.log(`  Found ${imdbIdArray.length} unique IMDb IDs to enrich...`);
        for (let i = 0; i < imdbIdArray.length; i += MAX_CONCURRENT_ENRICHMENTS) {
            const batch = imdbIdArray.slice(i, i + MAX_CONCURRENT_ENRICHMENTS);
            const promises = batch.map(id => findByImdbId(id).then(info => info ? getTmdbDetails(info.id, info.media_type) : null).then(details => analyzeAndTagItem(details)));
            const results = await Promise.allSettled(promises);
            results.forEach(r => r.status === 'fulfilled' && r.value && enrichedDataLake.set(r.value.imdb_id, r.value));
        }
        console.log(`  Enriched data lake contains ${enrichedDataLake.size} items.`);
        console.log('\nPHASE 3: Slicing data lake into final data marts...');
        await fs.rm(OUTPUT_DIR, { recursive: true, force: true });
        await fs.mkdir(OUTPUT_DIR, { recursive: true });
        for (const [key, config] of Object.entries(BUILD_MATRIX)) {
            let listData = queryResults[key].map(item => enrichedDataLake.get(item.id)).filter(Boolean);
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
