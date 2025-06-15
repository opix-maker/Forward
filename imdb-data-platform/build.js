import fs from 'fs/promises';
import { constants as fsConstants } from 'fs';
import path from 'path';
import fetch from 'node-fetch';
import { pipeline } from 'stream/promises';
import { createWriteStream, createReadStream } from 'fs';
import zlib from 'zlib';
import readline from 'readline';
import { getSupplementaryTmdbDetails } from './src/utils/tmdb_api.js'; 
import { analyzeAndTagItem } from './src/core/analyzer.js';

// --- 配置 ---
const MAX_CONCURRENT_ENRICHMENTS = 80; 
const MAX_CONCURRENT_FILE_IO = 64;     
const MAX_DOWNLOAD_AGE_MS = 20 * 60 * 60 * 1000; 
const DATA_LAKE_CACHE_AGE_MS = 20 * 60 * 60 * 1000;
const MIN_VOTES = 1000;   
const MIN_YEAR = 1990;     
const MIN_RATING = 6.0;    
const ALLOWED_TITLE_TYPES = new Set(['movie', 'tvSeries', 'tvMiniSeries', 'tvMovie']);
const CURRENT_YEAR = new Date().getFullYear();
const RECENT_YEAR_THRESHOLD = CURRENT_YEAR - 1;
const ITEMS_PER_PAGE = 30;
const SORT_CONFIG = { 'hs': 'hotness_score', 'r': 'vote_average', 'd': 'default_order' };
const DATASET_DIR = './datasets';
const TEMP_DIR = './temp';
const FINAL_OUTPUT_DIR = './dist';
const DATA_LAKE_FILE = path.join(TEMP_DIR, 'datalake.jsonl');
const DATASETS = { basics: { url: 'https://datasets.imdbws.com/title.basics.tsv.gz', local: 'title.basics.tsv' }, ratings: { url: 'https://datasets.imdbws.com/title.ratings.tsv.gz', local: 'title.ratings.tsv' }, akas: { url: 'https://datasets.imdbws.com/title.akas.tsv.gz', local: 'title.akas.tsv' }, };
const REGIONS = ['all', 'region:chinese', 'region:us-eu', 'region:east-asia', 'country:cn', 'country:hk', 'country:tw', 'country:us', 'country:gb', 'country:jp', 'country:kr', 'country:fr', 'country:de', 'country:ca', 'country:au'];
const GENRES_AND_THEMES = ['genre:爱情', 'genre:冒险', 'genre:悬疑', 'genre:惊悚', 'genre:恐怖', 'genre:科幻', 'genre:奇幻', 'genre:动作', 'genre:喜剧', 'genre:剧情', 'genre:历史', 'genre:战争', 'genre:犯罪', 'theme:whodunit', 'theme:spy', 'theme:courtroom', 'theme:slice-of-life', 'theme:wuxia', 'theme:superhero', 'theme:cyberpunk', 'theme:space-opera', 'theme:time-travel', 'theme:post-apocalyptic', 'theme:mecha', 'theme:zombie', 'theme:monster', 'theme:ghost', 'theme:magic', 'theme:gangster', 'theme:film-noir', 'theme:serial-killer', 'theme:xianxia', 'theme:kaiju', 'theme:isekai'];
const YEARS = Array.from({length: CURRENT_YEAR - 1990 + 1}, (_, i) => 1990 + i).reverse();

// --- 辅助函数 (保持不变) ---
async function isDataLakeCacheValid(filePath, maxAgeMs) { try { const stats = await fs.stat(filePath); const ageMs = Date.now() - stats.mtimeMs; console.log(`  > Data Lake file found. Age: ${(ageMs / 1000 / 60).toFixed(1)} minutes.`); return ageMs < maxAgeMs; } catch (e) { if (e.code === 'ENOENT') { console.log('  > Data Lake file not found.'); } else { console.error('  > Error checking Data Lake cache:', e.message); } return false; } }
async function downloadAndUnzipWithCache(url, localPath, maxAgeMs) { const dir = path.dirname(localPath); await fs.mkdir(dir, { recursive: true }); try { const stats = await fs.stat(localPath); if (Date.now() - stats.mtimeMs < maxAgeMs) { console.log(`  ✅ Cache hit for ${path.basename(localPath)}.`); return; } } catch (e) { /* no cache */ } console.log(`  ⏳ Downloading from: ${url}`); const response = await fetch(url, { headers: { 'User-Agent': 'IMDb-Builder/1.0' } }); if (!response.ok) throw new Error(`Failed to download ${url}: ${response.statusText}`); if (!response.body) throw new Error(`Response body is null for ${url}`); const gunzip = zlib.createGunzip(); const destination = createWriteStream(localPath); try { await pipeline(response.body, gunzip, destination); console.log(`  ✅ Download and unzip complete for ${path.basename(localPath)}.`); } catch (error) { console.error(`  ❌ Error during download/unzip for ${url}:`, error); await fs.unlink(localPath).catch(() => {}); throw error; } }
async function processTsvByLine(filePath, processor) { const fileStream = createReadStream(filePath); const rl = readline.createInterface({ input: fileStream, crlfDelay: Infinity }); let isFirstLine = true; let count = 0; for await (const line of rl) { if (isFirstLine) { isFirstLine = false; continue; } if (line && line.includes('\t')) { processor(line); count++; if (count % 2000000 === 0) { console.log(`    ... Processed ${count} lines...`); } } } console.log(`    ✅ Processed total ${count} lines from ${path.basename(filePath)}.`); }
async function processInParallel(items, concurrency, task) { const queue = [...items]; let processedCount = 0; const totalCount = items.length; let totalFiles = 0; const worker = async () => { while (queue.length > 0) { const item = queue.shift(); if (item) { const result = await task(item); processedCount++; if (typeof result === 'number') { totalFiles += result; } const updateFrequency = 500; if (processedCount % updateFrequency === 0 || processedCount === totalCount) { let progressLine = `  ... Progress: ${processedCount} / ${totalCount} (${(processedCount / totalCount * 100).toFixed(1)}%)`; if (totalFiles > 0) { progressLine += ` (Files generated: ${totalFiles})`; } console.log(progressLine); } } } }; const workers = Array(Math.min(concurrency, items.length)).fill(null).map(() => worker()); await Promise.all(workers); return totalFiles; }

// --- 阶段 1-3: 构建数据湖 ---
let idPool = new Map();
let akasIndex = new Map();
async function buildDataLake() {
    console.log('\n--- PHASE 1 & 2: Building Local IMDb Indexes ---');
    const ratingsIndex = new Map();
    const ratingsPath = path.join(DATASET_DIR, DATASETS.ratings.local);
    await downloadAndUnzipWithCache(DATASETS.ratings.url, ratingsPath, MAX_DOWNLOAD_AGE_MS);
    await processTsvByLine(ratingsPath, (line) => { const parts = line.split('\t'); const votes = parseInt(parts[2], 10) || 0; const rating = parseFloat(parts[1]) || 0; if (votes >= MIN_VOTES && rating >= MIN_RATING) { ratingsIndex.set(parts[0], { rating, votes }); } });
    
    idPool.clear();
    const basicsPath = path.join(DATASET_DIR, DATASETS.basics.local);
    await downloadAndUnzipWithCache(DATASETS.basics.url, basicsPath, MAX_DOWNLOAD_AGE_MS);
    await processTsvByLine(basicsPath, (line) => { const parts = line.split('\t'); const tconst = parts[0]; const year = parseInt(parts[5], 10); const genres = parts[8]; if (!tconst.startsWith('tt') || parts[4] === '1' || !ALLOWED_TITLE_TYPES.has(parts[1]) || isNaN(year) || year < MIN_YEAR || !ratingsIndex.has(tconst)) { return; } idPool.set(tconst, genres); });
    
    akasIndex.clear();
    const akasPath = path.join(DATASET_DIR, DATASETS.akas.local);
    await downloadAndUnzipWithCache(DATASETS.akas.url, akasPath, MAX_DOWNLOAD_AGE_MS);
    await processTsvByLine(akasPath, (line) => { const parts = line.split('\t'); const tconst = parts[0]; if (idPool.has(tconst)) { const region = parts[3] && parts[3] !== '\\N' ? parts[3].toLowerCase() : null; const language = parts[4] && parts[4] !== '\\N' ? parts[4].toLowerCase() : null; if (!akasIndex.has(tconst)) { akasIndex.set(tconst, { regions: new Set(), languages: new Set() }); } const entry = akasIndex.get(tconst); if (region) entry.regions.add(region); if (language) entry.languages.add(language); } });
    
    console.log(`  ✅ Local IMDb indexes built. Filtered ID pool: ${idPool.size}.`);
    
    console.log(`\n--- PHASE 3: Enriching with Supplementary TMDB Data (Concurrency: ${MAX_CONCURRENT_ENRICHMENTS}) ---`);
    await fs.rm(TEMP_DIR, { recursive: true, force: true }).catch(() => {}); 
    await fs.mkdir(TEMP_DIR, { recursive: true });
    const writeStream = createWriteStream(DATA_LAKE_FILE, { flags: 'a' });
    let apiErrorCount = 0;

    const enrichmentTask = async (imdbId) => {
         try {
            // 最终修复：调用获取补充性数据的函数
            const supplementaryDetails = await getSupplementaryTmdbDetails(imdbId);
            
            if (supplementaryDetails) {
                // 获取权威的 IMDb 数据
                const imdbAkasInfo = akasIndex.get(imdbId) || { regions: new Set(), languages: new Set() };
                const imdbGenresString = idPool.get(imdbId);
                
                // 传入补充性 TMDB 数据和权威的 IMDb 数据进行分析
                const analyzedItem = analyzeAndTagItem(supplementaryDetails, imdbAkasInfo, imdbGenresString);
                
                if (analyzedItem) {
                    writeStream.write(JSON.stringify(analyzedItem) + '\n'); 
                }
            }
        } catch (error) {
            apiErrorCount++;
            console.warn(`  Skipping ID ${imdbId} due to enrichment error: ${error.message}`);
        }
    };
    
    await processInParallel(Array.from(idPool.keys()), MAX_CONCURRENT_ENRICHMENTS, enrichmentTask);
    await new Promise(resolve => writeStream.end(resolve));
    console.log(`  ✅ Data Lake freshly built. Total items skipped due to errors: ${apiErrorCount}`);
}

// --- 阶段 4: 分片、排序和分页 ---
function minifyItem(item) { return { id: item.id, p: item.poster_path, b: item.backdrop_path, t: item.title, r: item.vote_average, y: item.release_year, rd: item.release_date, hs: parseFloat(item.hotness_score.toFixed(3)), d: parseFloat(item.default_order.toFixed(3)), mt: item.mediaType, o: item.overview }; }
const dirCreationPromises = new Map();
async function ensureDir(dir) { if (dirCreationPromises.has(dir)) { return dirCreationPromises.get(dir); } const promise = fs.mkdir(dir, { recursive: true }).catch(err => { if (err.code !== 'EEXIST') throw err; }); dirCreationPromises.set(dir, promise); return promise; }
async function processAndWriteSortedPaginatedShards(task) { const { basePath, data } = task; if (data.length === 0) return 0; const currentShardWritePromises = []; const metadata = { total_items: data.length, items_per_page: ITEMS_PER_PAGE, pages: {} }; for (const [sortPrefix, internalKey] of Object.entries(SORT_CONFIG)) { const sortedData = [...data].sort((a, b) => (b[internalKey] || 0) - (a[internalKey] || 0)); const numPages = Math.ceil(sortedData.length / ITEMS_PER_PAGE); metadata.pages[sortPrefix] = numPages; for (let page = 1; page <= numPages; page++) { const start = (page - 1) * ITEMS_PER_PAGE; const pageData = sortedData.slice(start, start + ITEMS_PER_PAGE); const minifiedPageData = pageData.map(minifyItem); const finalPath = path.join(FINAL_OUTPUT_DIR, basePath, `by_${sortPrefix}`, `page_${page}.json`); const dir = path.dirname(finalPath); const writePromise = ensureDir(dir).then(() => fs.writeFile(finalPath, JSON.stringify(minifiedPageData))); currentShardWritePromises.push(writePromise); } } const metaPath = path.join(FINAL_OUTPUT_DIR, basePath, 'meta.json'); const metaDir = path.dirname(metaPath); const metaWritePromise = ensureDir(metaDir).then(() => fs.writeFile(metaPath, JSON.stringify(metadata))); currentShardWritePromises.push(metaWritePromise); await Promise.all(currentShardWritePromises); return currentShardWritePromises.length; }
async function shardDatabase() {
    console.log(`\n--- PHASE 4: Sharding, Sorting, Paginating & Writing Files ---`);
    await fs.rm(FINAL_OUTPUT_DIR, { recursive: true, force: true }).catch(() => {});
    await fs.mkdir(FINAL_OUTPUT_DIR, { recursive: true });
    dirCreationPromises.clear();
    const database = [];
    try { await fs.access(DATA_LAKE_FILE, fsConstants.R_OK); } catch (e) { console.error(`\n❌ FATAL ERROR: Data Lake file ${DATA_LAKE_FILE} not found.`); throw new Error("Data Lake file missing."); }
    const rl = readline.createInterface({ input: createReadStream(DATA_LAKE_FILE), crlfDelay: Infinity });
    for await (const line of rl) { if (line.trim()) { database.push(JSON.parse(line)); } }
    if (database.length === 0) { console.log('  ⚠️ No items in database to process. Exiting Phase 4.'); return; }
    console.log(`  ✅ Loaded ${database.length} items. Calculating scores...`);
    const validForStats = database.filter(i => i.vote_count > 100);
    const totalRating = validForStats.reduce((sum, item) => sum + (item.vote_average || 0), 0);
    const GLOBAL_AVERAGE_RATING = validForStats.length > 0 ? totalRating / validForStats.length : 6.8;
    const sortedVotes = validForStats.map(i => i.vote_count).sort((a,b) => a - b);
    const MINIMUM_VOTES_THRESHOLD = sortedVotes[Math.floor(sortedVotes.length * 0.75)] || 500;
    database.forEach(item => { const pop = item.popularity || 0; const year = item.release_year || 1970; const R = item.vote_average || 0; const v = item.vote_count || 0; const yearDiff = Math.max(0, CURRENT_YEAR - year); const timeDecay = 1 / Math.sqrt(yearDiff + 2); const bayesianRating = (v / (v + MINIMUM_VOTES_THRESHOLD)) * R + (MINIMUM_VOTES_THRESHOLD / (v + MINIMUM_VOTES_THRESHOLD)) * GLOBAL_AVERAGE_RATING; item.hotness_score = Math.log10(pop + 1) * timeDecay * bayesianRating; item.default_order = pop; });
    const shardTasksDefinitions = []; const contentTypes = ['all', 'movie', 'tv', 'anime']; const definedRegions = REGIONS;
    const filterData = (baseData, type, region) => { let filtered = baseData; if (type !== 'all') { filtered = filtered.filter(i => i.mediaType === type); } if (region !== 'all') { filtered = filtered.filter(i => i.semantic_tags.includes(region)); } return filtered; };
    const addShardDefinition = (pathName, data) => { shardTasksDefinitions.push({ basePath: pathName, data: data }); };
    const recentHotBase = database.filter(i => i.release_year >= RECENT_YEAR_THRESHOLD);
    for (const type of contentTypes) { for (const region of definedRegions) { const data = filterData(recentHotBase, type, region); addShardDefinition(`hot/${type}/${region.replace(':', '_')}`, data); } }
    const allCategories = ['all', ...GENRES_AND_THEMES]; 
    for (const tag of allCategories) { const tagBaseData = (tag === 'all') ? database : database.filter(i => i.semantic_tags.includes(tag)); for (const type of contentTypes) { for (const region of definedRegions) { const data = filterData(tagBaseData, type, region); addShardDefinition(`tag/${tag.replace(':', '_')}/${type}/${region.replace(':', '_')}`, data); } } }
    const allYears = ['all', ...YEARS]; 
    for (const year of allYears) { const yearBaseData = (year === 'all') ? database : database.filter(i => i.release_year === year); for (const type of contentTypes) { for (const region of definedRegions) { const data = filterData(yearBaseData, type, region); addShardDefinition(`year/${year}/${type}/${region.replace(':', '_')}`, data); } } }
    const directTypes = [{ name: 'movies', mediaType: 'movie' }, { name: 'tvseries', mediaType: 'tv' }, { name: 'anime', mediaType: 'anime' }];
    for (const type of directTypes) { let baseData = database.filter(i => i.mediaType === type.mediaType); for (const region of definedRegions) { let data = (region === 'all') ? baseData : baseData.filter(i => i.semantic_tags.includes(region)); addShardDefinition(`${type.name}/${region.replace(':', '_')}`, data); } }
    console.log(`  Processing ${shardTasksDefinitions.length} shards...`);
    const totalFiles = await processInParallel(shardTasksDefinitions, MAX_CONCURRENT_FILE_IO, processAndWriteSortedPaginatedShards);
    await fs.writeFile(path.join(FINAL_OUTPUT_DIR, 'manifest.json'), JSON.stringify({ buildTimestamp: new Date().toISOString(), regions: REGIONS, tags: GENRES_AND_THEMES, years: YEARS, itemsPerPage: ITEMS_PER_PAGE, sortOptions: Object.keys(SORT_CONFIG), contentTypes: contentTypes, isPaginated: true }));
    console.log(`  ✅ Phase 4 complete. Total files: ${totalFiles}.`);
}

// --- 主执行入口 ---
async function main() {
    console.log('🎬 Starting IMDb Discovery Engine Build Process (v4 - IMDb First Principle)...');
    const startTime = Date.now();
    try {
        console.log('\n🔍 Checking Data Lake cache...');
        const cacheValid = await isDataLakeCacheValid(DATA_LAKE_FILE, DATA_LAKE_CACHE_AGE_MS);
        if (cacheValid) {
            console.log('✅ Cache hit! Using existing Data Lake. Skipping Phases 1-3.');
        } else {
            console.log('❌ Cache miss or stale. Starting full build...');
            await buildDataLake();
        }
        await shardDatabase(); 
        const duration = (Date.now() - startTime) / 1000;
        console.log(`\n🎉🎉🎉 Build process successful! 🎉🎉🎉`);
        console.log(`Total time: ${duration.toFixed(2)} seconds.`);
    } catch (error) {
        console.error('\n❌ FATAL ERROR during build process:', error.stack || error);
        process.exit(1); 
    }
}

main();
