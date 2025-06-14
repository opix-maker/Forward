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
const MAX_CONCURRENT_FILE_IO = 64;     

// --- 缓存设置 ---
const MAX_DOWNLOAD_AGE_MS = 20 * 60 * 60 * 1000; // 20 hours 
const DATA_LAKE_CACHE_AGE_MS = 20 * 60 * 60 * 1000; 

// --- 阈值和参数 ---
const MIN_VOTES = 1000; 
const MIN_YEAR = 1990; 
const MIN_RATING = 6.0; 
const ALLOWED_TITLE_TYPES = new Set(['movie', 'tvSeries', 'tvMiniSeries', 'tvMovie']);
const CURRENT_YEAR = new Date().getFullYear();
const RECENT_YEAR_THRESHOLD = CURRENT_YEAR - 1;
const ITEMS_PER_PAGE = 30;
const SORT_CONFIG = { 'hs': 'hotness_score', 'r': 'vote_average', 'd': 'default_order' };

// --- PATHS ---
const DATASET_DIR = './datasets';
const TEMP_DIR = './temp';
const FINAL_OUTPUT_DIR = './dist';
const DATA_LAKE_FILE = path.join(TEMP_DIR, 'datalake.jsonl'); // 我们的核心缓存文件

const DATASETS = {
    basics: { url: 'https://datasets.imdbws.com/title.basics.tsv.gz', local: 'title.basics.tsv' },
    ratings: { url: 'https://datasets.imdbws.com/title.ratings.tsv.gz', local: 'title.ratings.tsv' },
    akas: { url: 'https://datasets.imdbws.com/title.akas.tsv.gz', local: 'title.akas.tsv' },
};

// --- 分片配置 ---
const REGIONS = ['all', 'region:chinese', 'region:us-eu', 'region:east-asia', 'country:cn', 'country:hk', 'country:tw', 'country:us', 'country:gb', 'country:jp', 'country:kr', 'country:fr', 'country:de', 'country:ca', 'country:au'];
const GENRES_AND_THEMES = ['genre:爱情', 'genre:冒险', 'genre:悬疑', 'genre:惊悚', 'genre:恐怖', 'genre:科幻', 'genre:奇幻', 'genre:动作', 'genre:喜剧', 'genre:剧情', 'genre:历史', 'genre:战争', 'genre:犯罪', 'theme:whodunit', 'theme:spy', 'theme:courtroom', 'theme:slice-of-life', 'theme:wuxia', 'theme:superhero', 'theme:cyberpunk', 'theme:space-opera', 'theme:time-travel', 'theme:post-apocalyptic', 'theme:mecha', 'theme:zombie', 'theme:monster', 'theme:ghost', 'theme:magic', 'theme:gangster', 'theme:film-noir', 'theme:serial-killer', 'theme:xianxia', 'theme:kaiju', 'theme:isekai'];
const YEARS = Array.from({length: CURRENT_YEAR - 1990 + 1}, (_, i) => 1990 + i).reverse();


// --- 辅助函数 ---

// --- 缓存检查函数 ---
async function isDataLakeCacheValid(filePath, maxAgeMs) {
    try {
        const stats = await fs.stat(filePath);
        const ageMs = Date.now() - stats.mtimeMs;
        console.log(`  > Data Lake file found. Age: ${(ageMs / 1000 / 60).toFixed(1)} minutes.`);
        return ageMs < maxAgeMs;
    } catch (e) {
        if (e.code === 'ENOENT') {
            console.log('  > Data Lake file not found.');
        } else {
            console.error('  > Error checking Data Lake cache:', e.message);
        }
        return false; // 文件不存在或读取错误
    }
}

// --- 下载和解压 ---
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
   if (!response.body) throw new Error(`Response body is null for ${url}`);
   const gunzip = zlib.createGunzip();
   const destination = createWriteStream(localPath);
   try {
        await pipeline(response.body, gunzip, destination);
        console.log(`  Download and unzip complete for ${path.basename(localPath)}.`);
   } catch (error) {
        console.error(`  Error during download/unzip for ${url}:`, error);
        await fs.unlink(localPath).catch(() => {}); 
        throw error;
   }
}

// --- TSV 文件处理  ---
async function processTsvByLine(filePath, processor) {
   const fileStream = createReadStream(filePath);
   const rl = readline.createInterface({ input: fileStream, crlfDelay: Infinity });
   let isFirstLine = true;
   let count = 0;
   for await (const line of rl) {
       if (isFirstLine) { isFirstLine = false; continue; }
       if (line && line.includes('\t')) {
           processor(line);
           count++;
           if (count % 500000 === 0) {
                process.stdout.write(`    Processed ${count} lines...\r`);
           }
       }
   }
   process.stdout.write(`    Processed total ${count} lines from ${path.basename(filePath)}.\n`);
}

// --- 核心并发控制函数 ---
async function processInParallel(items, concurrency, task) {
   const queue = [...items]; 
   let processedCount = 0; 
   const totalCount = items.length;
   let totalFiles = 0; // 用于 Phase 4 记录文件数

   const worker = async () => {
       while (queue.length > 0) {
           const item = queue.shift();
           if (item) {
               const result = await task(item);
               processedCount++;

               if (typeof result === 'number') {
                   totalFiles += result;
               }

               // 调整进度更新频率，避免控制台输出混乱
               const updateFrequency = Math.max(1, Math.ceil(concurrency / 4));
               if (processedCount % updateFrequency === 0 || processedCount === totalCount) {
                   let progressLine = `  Progress: ${processedCount} / ${totalCount}`;
                   if (totalFiles > 0) {
                       progressLine += ` (Files written: ${totalFiles})`;
                   }
                   process.stdout.write(progressLine + ' \r');
               }
           }
       }
   };

   const workers = Array(Math.min(concurrency, items.length)).fill(null).map(() => worker());
   await Promise.all(workers);
   process.stdout.write('\n');
   return totalFiles;
}


// --- Phase 1-3: 构建数据湖 (仅在缓存失效时运行) ---
// 存储全局索引，以便在 Phase 3 使用
let idPool = new Map();
let akasIndex = new Map();

async function buildDataLake() {
    // --- Phase 1: 评分索引 ---
    console.log('\nPHASE 1: Building ratings index & Caching...');
    const ratingsIndex = new Map();
    const ratingsPath = path.join(DATASET_DIR, DATASETS.ratings.local);
    await downloadAndUnzipWithCache(DATASETS.ratings.url, ratingsPath, MAX_DOWNLOAD_AGE_MS);
    await processTsvByLine(ratingsPath, (line) => {
        const parts = line.split('\t');
        const votes = parseInt(parts[2], 10) || 0; const rating = parseFloat(parts[1]) || 0;
        if (votes >= MIN_VOTES && rating >= MIN_RATING) ratingsIndex.set(parts[0], { rating, votes });
    });
    console.log(`  Ratings index built with ${ratingsIndex.size} items.`);

    // --- Phase 2: ID 池 + IMDb 类型 ---
    console.log(`\nPHASE 2: Streaming basics, filtering, and extracting IMDb genres...`);
    idPool.clear(); // 使用全局变量
    const basicsPath = path.join(DATASET_DIR, DATASETS.basics.local);
    await downloadAndUnzipWithCache(DATASETS.basics.url, basicsPath, MAX_DOWNLOAD_AGE_MS);
    await processTsvByLine(basicsPath, (line) => {
        const parts = line.split('\t');
        const tconst = parts[0];
        const year = parseInt(parts[5], 10);
        const genres = parts[8];
        if (!tconst.startsWith('tt') || parts[4] === '1' || !ALLOWED_TITLE_TYPES.has(parts[1]) || isNaN(year) || year < MIN_YEAR || !ratingsIndex.has(tconst)) return;
        idPool.set(tconst, genres);
    });
    console.log(`  Filtered ID pool contains ${idPool.size} items.`);

    // --- Phase 2.5: AKAS 地区/语言索引 ---
    console.log(`\nPHASE 2.5: Building region/language index from AKAS...`);
    akasIndex.clear(); // 使用全局变量
    const akasPath = path.join(DATASET_DIR, DATASETS.akas.local);
    await downloadAndUnzipWithCache(DATASETS.akas.url, akasPath, MAX_DOWNLOAD_AGE_MS);
    await processTsvByLine(akasPath, (line) => {
        const parts = line.split('\t');
        const tconst = parts[0];
        if (idPool.has(tconst)) {
            const region = parts[3] && parts[3] !== '\\N' ? parts[3].toLowerCase() : null;
            const language = parts[4] && parts[4] !== '\\N' ? parts[4].toLowerCase() : null;
            if (!akasIndex.has(tconst)) akasIndex.set(tconst, { regions: new Set(), languages: new Set() });
            const entry = akasIndex.get(tconst);
            if (region) entry.regions.add(region);
            if (language) entry.languages.add(language);
        }
    });
    console.log(`  AKAS index built for ${akasIndex.size} items.`);

    // --- Phase 3: TMDB API 丰富 ---
    console.log(`\nPHASE 3: Enriching items via TMDB API (Concurrency: ${MAX_CONCURRENT_ENRICHMENTS})...`);
    
    // <<< 重要改动: 仅在执行 buildDataLake 时清理并创建 TEMP 目录
    await fs.rm(TEMP_DIR, { recursive: true, force: true }).catch(() => {}); 
    await fs.mkdir(TEMP_DIR, { recursive: true });

    const writeStream = createWriteStream(DATA_LAKE_FILE, { flags: 'a' });
    
    const enrichmentTask = async (imdbId) => {
         try {
            const info = await findByImdbId(imdbId);
            if (!info || !info.id || !info.media_type) return;
            const details = await getTmdbDetails(info.id, info.media_type);
            if (details) {
                const imdbAkasInfo = akasIndex.get(imdbId) || { regions: new Set(), languages: new Set() };
                const imdbGenresString = idPool.get(imdbId);
                const analyzedItem = analyzeAndTagItem(details, imdbAkasInfo, imdbGenresString);
                if (analyzedItem) {
                    // 同步写入以避免 JSONL 文件损坏
                    writeStream.write(JSON.stringify(analyzedItem) + '\n'); 
                }
            }
        } catch (error) {
            // console.warn(`  Skipping ID ${imdbId} due to enrichment error: ${error.message}`);
        }
    };

    // 使用并发控制函数
    await processInParallel(Array.from(idPool.keys()), MAX_CONCURRENT_ENRICHMENTS, enrichmentTask);
    await new Promise(resolve => writeStream.end(resolve));
    console.log(`  Data Lake freshly built and written to ${DATA_LAKE_FILE}`);
}


// --- Phase 4: 分片、排序和分页 ---

// 精简条目 
function minifyItem(item) {
    return {
        id: item.id,
        p: item.poster_path, b: item.backdrop_path, t: item.title, r: item.vote_average,
        y: item.release_year, rd: item.release_date,
        hs: parseFloat(item.hotness_score.toFixed(3)), d: parseFloat(item.default_order.toFixed(3)),
        mt: item.mediaType, o: item.overview
    };
}

// I/O 优化辅助 
const dirCreationPromises = new Map();
async function ensureDir(dir) {
    if (dirCreationPromises.has(dir)) return dirCreationPromises.get(dir);
    const promise = fs.mkdir(dir, { recursive: true }).catch(err => {
        if (err.code !== 'EEXIST') throw err; 
    });
    dirCreationPromises.set(dir, promise);
    return promise;
}

// 单个分片的处理函数 
async function processAndWriteSortedPaginatedShards(task) {
    const { basePath, data } = task;
    if (data.length === 0) return 0;

    const currentShardWritePromises = [];
    const metadata = { total_items: data.length, items_per_page: ITEMS_PER_PAGE, pages: {} };

    for (const [sortPrefix, internalKey] of Object.entries(SORT_CONFIG)) {
        const sortedData = [...data].sort((a, b) => (b[internalKey] || 0) - (a[internalKey] || 0));
        const numPages = Math.ceil(sortedData.length / ITEMS_PER_PAGE);
        metadata.pages[sortPrefix] = numPages;

        for (let page = 1; page <= numPages; page++) {
            const start = (page - 1) * ITEMS_PER_PAGE;
            const pageData = sortedData.slice(start, start + ITEMS_PER_PAGE);
            const minifiedPageData = pageData.map(minifyItem);
            
            const finalPath = path.join(FINAL_OUTPUT_DIR, basePath, `by_${sortPrefix}`, `page_${page}.json`);
            const dir = path.dirname(finalPath);
            
            const writePromise = ensureDir(dir).then(() => 
                fs.writeFile(finalPath, JSON.stringify(minifiedPageData))
            );
            currentShardWritePromises.push(writePromise);
        }
    }

    const metaPath = path.join(FINAL_OUTPUT_DIR, basePath, 'meta.json');
    const metaDir = path.dirname(metaPath);
    const metaWritePromise = ensureDir(metaDir).then(() => 
        fs.writeFile(metaPath, JSON.stringify(metadata))
    );
    currentShardWritePromises.push(metaWritePromise);

    await Promise.all(currentShardWritePromises);
    return currentShardWritePromises.length; 
}

// 数据库分片主函数 
async function shardDatabase() {
    console.log(`\nPHASE 4: Sharding, Sorting, and Paginating database (I/O Concurrency: ${MAX_CONCURRENT_FILE_IO})...`);
    await fs.rm(FINAL_OUTPUT_DIR, { recursive: true, force: true }).catch(() => {});
    await fs.mkdir(FINAL_OUTPUT_DIR, { recursive: true });
    dirCreationPromises.clear();

    // --- 加载数据湖 ---
    const database = [];
    // 确保文件存在
    try {
        await fs.access(DATA_LAKE_FILE, fsConstants.R_OK);
    } catch (e) {
        console.error(`\n❌ FATAL ERROR: Data Lake file ${DATA_LAKE_FILE} not found or not readable.`);
        throw new Error("Data Lake file missing.");
    }
    
    const rl = readline.createInterface({ input: createReadStream(DATA_LAKE_FILE), crlfDelay: Infinity });
    for await (const line of rl) { if (line.trim()) database.push(JSON.parse(line)); }
    console.log(`  Loaded ${database.length} items from data lake.`);

    // --- 计算分数  ---
    const validForStats = database.filter(i => i.vote_count > 100);
    const totalRating = validForStats.reduce((sum, item) => sum + (item.vote_average || 0), 0);
    const GLOBAL_AVERAGE_RATING = validForStats.length > 0 ? totalRating / validForStats.length : 6.8;
    const sortedVotes = validForStats.map(i => i.vote_count).sort((a,b) => a - b);
    const MINIMUM_VOTES_THRESHOLD = sortedVotes[Math.floor(sortedVotes.length * 0.75)] || 500;
    console.log(`  Global Stats: AvgRating=${GLOBAL_AVERAGE_RATING.toFixed(2)}, MinVotes=${MINIMUM_VOTES_THRESHOLD}`);

    database.forEach(item => {
        const pop = item.popularity || 0; const year = item.release_year || 1970;
        const R = item.vote_average || 0; const v = item.vote_count || 0;
        const yearDiff = Math.max(0, CURRENT_YEAR - year);
        const bayesianRating = (v / (v + MINIMUM_VOTES_THRESHOLD)) * R + (MINIMUM_VOTES_THRESHOLD / (v + MINIMUM_VOTES_THRESHOLD)) * GLOBAL_AVERAGE_RATING;
        item.hotness_score = Math.log10(pop + 1) * (1 / Math.sqrt(yearDiff + 2)) * bayesianRating;
        item.default_order = pop;
    });

    // --- 收集所有分片任务 ---
    console.log('  Collecting all shard tasks...');
    const shardTasksDefinitions = []; // { basePath, data }

    const contentTypes = ['all', 'movie', 'tv', 'anime'];
    const definedRegions = REGIONS; 

    const filterData = (baseData, type, region) => {
        let filtered = baseData;
        if (type !== 'all') filtered = filtered.filter(i => i.mediaType === type);
        if (region !== 'all') filtered = filtered.filter(i => i.semantic_tags.includes(region));
        return filtered;
    };

    const addShardDefinition = (pathName, data) => {
        shardTasksDefinitions.push({ basePath: pathName, data: data });
    };

    // 1. 近期热门
    const recentHotBase = database.filter(i => i.release_year >= RECENT_YEAR_THRESHOLD);
    for (const type of contentTypes) {
        for (const region of definedRegions) {
            const data = filterData(recentHotBase, type, region);
            const pathName = `hot/${type}/${region.replace(':', '_')}`;
            addShardDefinition(pathName, data);
        }
    }

    // 2. 按分类/主题
    const allCategories = ['all', ...GENRES_AND_THEMES]; 
    for (const tag of allCategories) {
        const tagBaseData = (tag === 'all') ? database : database.filter(i => i.semantic_tags.includes(tag));
        for (const type of contentTypes) {
            for (const region of definedRegions) {
                const data = filterData(tagBaseData, type, region);
                const pathName = `tag/${tag.replace(':', '_')}/${type}/${region.replace(':', '_')}`;
                addShardDefinition(pathName, data);
            }
        }
    }

    // 3. 按年份
    const allYears = ['all', ...YEARS]; 
    for (const year of allYears) {
        const yearBaseData = (year === 'all') ? database : database.filter(i => i.release_year === year);
        for (const type of contentTypes) {
             for (const region of definedRegions) {
                const data = filterData(yearBaseData, type, region);
                const pathName = `year/${year}/${type}/${region.replace(':', '_')}`;
                addShardDefinition(pathName, data);
             }
        }
    }

    // 4. 独立的电影/剧集/动画列表
    const directTypes = [{ name: 'movies', mediaType: 'movie' }, { name: 'tvseries', mediaType: 'tv' }, { name: 'anime', mediaType: 'anime' }];
    for (const type of directTypes) {
        let baseData = database.filter(i => i.mediaType === type.mediaType);
        for (const region of definedRegions) {
            let data = (region === 'all') ? baseData : baseData.filter(i => i.semantic_tags.includes(region));
            const pathName = `${type.name}/${region.replace(':', '_')}`;
            addShardDefinition(pathName, data);
        }
    }

    // --- 执行分片任务 (使用并发控制) ---
    console.log(`  Processing ${shardTasksDefinitions.length} shards with controlled concurrency...`);
    const totalFiles = await processInParallel(
        shardTasksDefinitions, 
        MAX_CONCURRENT_FILE_IO, 
        processAndWriteSortedPaginatedShards
    );

    // --- 生成 Manifest ---
    await fs.writeFile(path.join(FINAL_OUTPUT_DIR, 'manifest.json'), JSON.stringify({
        buildTimestamp: new Date().toISOString(),
        regions: REGIONS, tags: GENRES_AND_THEMES, years: YEARS,
        itemsPerPage: ITEMS_PER_PAGE, sortOptions: Object.keys(SORT_CONFIG), contentTypes: contentTypes
    }));
    
    console.log(`  ✅ Sharding, Sorting, and Paginating complete. Processed ${shardTasksDefinitions.length} shards and wrote ${totalFiles} files.`);
}


// --- 主函数  ---
async function main() {
    console.log('Starting IMDb Sharded Build Process (v2.5 - Caching & Concurrency Control)...');
    const startTime = Date.now();

    try {
        // --- 缓存检查 ---
        console.log('\nChecking Data Lake cache validity...');
        const cacheValid = await isDataLakeCacheValid(DATA_LAKE_FILE, DATA_LAKE_CACHE_AGE_MS);

        if (cacheValid) {
            console.log('✅ Cache hit! Using existing Data Lake. Skipping Phases 1-3.');
        } else {
            console.log('❌ Cache miss or stale. Starting full build (Phases 1-3). This may take ~30 minutes...');
            await buildDataLake(); // 仅在缓存无效时运行
        }

        // --- 总是运行 Phase 4 ---
        // 这将使用刚刚生成的或缓存中的 datalake.jsonl
        await shardDatabase(); 

        const duration = (Date.now() - startTime) / 1000;
        console.log(`\n✅ Build process successful! Took ${duration.toFixed(2)} seconds.`);

    } catch (error) {
        console.error('\n❌ FATAL ERROR during build process:', error.stack || error);
        process.exit(1);
    } finally {
        console.log(`Build finished. Cache file preserved at ${DATA_LAKE_FILE}`);
        console.log('To force a full rebuild, delete the "temp" directory manually.');
    }
}

main();
