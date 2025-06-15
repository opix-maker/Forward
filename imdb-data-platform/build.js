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

// --- 全局配置 ---
const MAX_CONCURRENT_ENRICHMENTS = 80; 
const MAX_CONCURRENT_FILE_IO = 64;     

// --- 缓存设置 ---
const MAX_DOWNLOAD_AGE_MS = 20 * 60 * 60 * 1000; 
const DATA_LAKE_CACHE_AGE_MS = 20 * 60 * 60 * 1000;

// --- 数据过滤阈值 ---
const MIN_VOTES = 1000;   
const MIN_YEAR = 1990;     
const MIN_RATING = 6.0;    
const ALLOWED_TITLE_TYPES = new Set(['movie', 'tvSeries', 'tvMiniSeries', 'tvMovie']);
const CURRENT_YEAR = new Date().getFullYear();
const RECENT_YEAR_THRESHOLD = CURRENT_YEAR - 1;

// --- 分页和排序 ---
const ITEMS_PER_PAGE = 30;
const SORT_CONFIG = { 
    'hs': 'hotness_score', // 热度分
    'r': 'vote_average',   // 评分
    'd': 'default_order'   // 默认（流行度）
};

// --- 文件路径 ---
const DATASET_DIR = './datasets';
const TEMP_DIR = './temp';
const FINAL_OUTPUT_DIR = './dist';
const DATA_LAKE_FILE = path.join(TEMP_DIR, 'datalake.jsonl'); // 存放 API 请求结果

const DATASETS = {
    basics: { url: 'https://datasets.imdbws.com/title.basics.tsv.gz', local: 'title.basics.tsv' },
    ratings: { url: 'https://datasets.imdbws.com/title.ratings.tsv.gz', local: 'title.ratings.tsv' },
    akas: { url: 'https://datasets.imdbws.com/title.akas.tsv.gz', local: 'title.akas.tsv' },
};

// --- 分片维度定义 ---
const REGIONS = ['all', 'region:chinese', 'region:us-eu', 'region:east-asia', 'country:cn', 'country:hk', 'country:tw', 'country:us', 'country:gb', 'country:jp', 'country:kr', 'country:fr', 'country:de', 'country:ca', 'country:au'];
const GENRES_AND_THEMES = ['genre:爱情', 'genre:冒险', 'genre:悬疑', 'genre:惊悚', 'genre:恐怖', 'genre:科幻', 'genre:奇幻', 'genre:动作', 'genre:喜剧', 'genre:剧情', 'genre:历史', 'genre:战争', 'genre:犯罪', 'theme:whodunit', 'theme:spy', 'theme:courtroom', 'theme:slice-of-life', 'theme:wuxia', 'theme:superhero', 'theme:cyberpunk', 'theme:space-opera', 'theme:time-travel', 'theme:post-apocalyptic', 'theme:mecha', 'theme:zombie', 'theme:monster', 'theme:ghost', 'theme:magic', 'theme:gangster', 'theme:film-noir', 'theme:serial-killer', 'theme:xianxia', 'theme:kaiju', 'theme:isekai'];
const YEARS = Array.from({length: CURRENT_YEAR - 1990 + 1}, (_, i) => 1990 + i).reverse();


// === 辅助函数 ===


// 检查数据湖缓存有效性
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
        return false;
    }
}

// 下载并解压文件
async function downloadAndUnzipWithCache(url, localPath, maxAgeMs) {
    const dir = path.dirname(localPath);
    await fs.mkdir(dir, { recursive: true });

   try {
       const stats = await fs.stat(localPath);
       if (Date.now() - stats.mtimeMs < maxAgeMs) {
           console.log(`  ✅ Cache hit for ${path.basename(localPath)}.`);
           return;
       }
   } catch (e) { /* no cache */ }

   console.log(`  ⏳ Downloading from: ${url}`);
   const response = await fetch(url, { headers: { 'User-Agent': 'IMDb-Builder/1.0' } });
   if (!response.ok) throw new Error(`Failed to download ${url}: ${response.statusText}`);
   if (!response.body) throw new Error(`Response body is null for ${url}`);

   const gunzip = zlib.createGunzip();
   const destination = createWriteStream(localPath);
   try {
        await pipeline(response.body, gunzip, destination);
        console.log(`  ✅ Download and unzip complete for ${path.basename(localPath)}.`);
   } catch (error) {
        console.error(`  ❌ Error during download/unzip for ${url}:`, error);
        await fs.unlink(localPath).catch(() => {}); 
        throw error;
   }
}

// 逐行处理 TSV 文件 
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
           if (count % 2000000 === 0) { 
                console.log(`    ... Processed ${count} lines...`);
           }
       }
   }
   console.log(`    ✅ Processed total ${count} lines from ${path.basename(filePath)}.`);
}

// 并行任务处理
async function processInParallel(items, concurrency, task) {
   const queue = [...items]; 
   let processedCount = 0; 
   const totalCount = items.length;
   let totalFiles = 0; // 仅用于 Phase 4

   const worker = async () => {
       while (queue.length > 0) {
           const item = queue.shift();
           if (item) {
               const result = await task(item);
               processedCount++;

               if (typeof result === 'number') {
                   totalFiles += result;
               }
               const updateFrequency = 500; 
               if (processedCount % updateFrequency === 0 || processedCount === totalCount) {
                   let progressLine = `  ... Progress: ${processedCount} / ${totalCount} (${(processedCount / totalCount * 100).toFixed(1)}%)`;
                   if (totalFiles > 0) {
                       progressLine += ` (Files generated: ${totalFiles})`;
                   }
                   console.log(progressLine); 
               }
           }
       }
   };

   const workers = Array(Math.min(concurrency, items.length)).fill(null).map(() => worker());
   await Promise.all(workers);
   return totalFiles; // 返回值对 Phase 4 有用
}



// === 阶段 1-3: 构建数据湖 ===

let idPool = new Map();
let akasIndex = new Map();

async function buildDataLake() {
    // --- Phase 1: 构建评分索引 ---
    console.log('\n--- PHASE 1: Building Ratings Index & Downloading Data ---');
    const ratingsIndex = new Map();
    const ratingsPath = path.join(DATASET_DIR, DATASETS.ratings.local);
    await downloadAndUnzipWithCache(DATASETS.ratings.url, ratingsPath, MAX_DOWNLOAD_AGE_MS);
    await processTsvByLine(ratingsPath, (line) => {
        const parts = line.split('\t');
        const votes = parseInt(parts[2], 10) || 0;
        const rating = parseFloat(parts[1]) || 0;
        if (votes >= MIN_VOTES && rating >= MIN_RATING) {
            ratingsIndex.set(parts[0], { rating, votes });
        }
    });
    console.log(`  ✅ Ratings index built. Total items: ${ratingsIndex.size}.`);

    // --- Phase 2: 过滤基础信息，构建 ID 池 ---
    console.log(`\n--- PHASE 2: Filtering Basics & Building ID Pool ---`);
    idPool.clear();
    const basicsPath = path.join(DATASET_DIR, DATASETS.basics.local);
    await downloadAndUnzipWithCache(DATASETS.basics.url, basicsPath, MAX_DOWNLOAD_AGE_MS);
    await processTsvByLine(basicsPath, (line) => {
        const parts = line.split('\t');
        const tconst = parts[0];
        const year = parseInt(parts[5], 10);
        const genres = parts[8];
        // 过滤条件：必须有评分，类型符合，年份符合，不是成人内容
        if (!tconst.startsWith('tt') || parts[4] === '1' || !ALLOWED_TITLE_TYPES.has(parts[1]) || isNaN(year) || year < MIN_YEAR || !ratingsIndex.has(tconst)) {
            return;
        }
        idPool.set(tconst, genres);
    });
    console.log(`  ✅ Filtered ID pool built. Total items: ${idPool.size}.`);

    // --- Phase 2.5: 构建地区/语言索引 ---
    console.log(`\n--- PHASE 2.5: Building AKAS Region/Language Index ---`);
    akasIndex.clear();
    const akasPath = path.join(DATASET_DIR, DATASETS.akas.local);
    await downloadAndUnzipWithCache(DATASETS.akas.url, akasPath, MAX_DOWNLOAD_AGE_MS);
    await processTsvByLine(akasPath, (line) => {
        const parts = line.split('\t');
        const tconst = parts[0];
        // 只处理 ID 池中的项目
        if (idPool.has(tconst)) {
            const region = parts[3] && parts[3] !== '\\N' ? parts[3].toLowerCase() : null;
            const language = parts[4] && parts[4] !== '\\N' ? parts[4].toLowerCase() : null;

            if (!akasIndex.has(tconst)) {
                akasIndex.set(tconst, { regions: new Set(), languages: new Set() });
            }
            const entry = akasIndex.get(tconst);
            if (region) entry.regions.add(region);
            if (language) entry.languages.add(language);
        }
    });
    console.log(`  ✅ AKAS index built. Items processed: ${akasIndex.size}.`);

    // --- Phase 3: 通过 TMDB API 丰富数据 ---
    console.log(`\n--- PHASE 3: Enriching Data via TMDB API (Concurrency: ${MAX_CONCURRENT_ENRICHMENTS}) ---`);
    console.log('  ⏳ This might take 20-40 minutes...');
    
    await fs.rm(TEMP_DIR, { recursive: true, force: true }).catch(() => {}); 
    await fs.mkdir(TEMP_DIR, { recursive: true });

    const writeStream = createWriteStream(DATA_LAKE_FILE, { flags: 'a' });
    
    let apiErrorCount = 0; 

    const enrichmentTask = async (imdbId) => {
         try {
            const info = await findByImdbId(imdbId);
            if (!info || !info.id || !info.media_type) return;

            const details = await getTmdbDetails(info.id, info.media_type);
            if (details) {
                const imdbAkasInfo = akasIndex.get(imdbId) || { regions: new Set(), languages: new Set() };
                const imdbGenresString = idPool.get(imdbId);
                // 分析和打标签
                const analyzedItem = analyzeAndTagItem(details, imdbAkasInfo, imdbGenresString);
                
                if (analyzedItem) {
                    // 写入 JSONL 文件
                    writeStream.write(JSON.stringify(analyzedItem) + '\n'); 
                }
            }
        } catch (error) {
            apiErrorCount++; // 只计数
            // console.warn(`  Skipping ID ${imdbId} due to enrichment error: ${error.message}`); // <<< 已移除！
        }
    };

    // 执行并行处理（使用精简日志版）
    await processInParallel(Array.from(idPool.keys()), MAX_CONCURRENT_ENRICHMENTS, enrichmentTask);
    
    // 确保所有数据写入文件
    await new Promise(resolve => writeStream.end(resolve));

    console.log(`  ✅ Data Lake freshly built and written to ${DATA_LAKE_FILE}`);
    console.log(`  ℹ️ Total items skipped due to API errors: ${apiErrorCount}`); // 打印错误摘要
}



// === 阶段 4: 分片、排序和分页 ===


// 精简数据条目，用于最终 JSON
function minifyItem(item) {
    return {
        id: item.id,                // TMDB ID
        p: item.poster_path,        // 海报
        b: item.backdrop_path,      // 背景图
        t: item.title,              // 标题
        r: item.vote_average,       // 评分
        y: item.release_year,       // 年份
        rd: item.release_date,      // 具体日期
        hs: parseFloat(item.hotness_score.toFixed(3)), // 热度分
        d: parseFloat(item.default_order.toFixed(3)),  // 默认排序
        mt: item.mediaType,         // 媒体类型
        o: item.overview            // 简介
    };
}

// 缓存目录创建 Promise，避免重复创建
const dirCreationPromises = new Map();
async function ensureDir(dir) {
    if (dirCreationPromises.has(dir)) {
        return dirCreationPromises.get(dir);
    }
    const promise = fs.mkdir(dir, { recursive: true }).catch(err => {
        if (err.code !== 'EEXIST') throw err; 
    });
    dirCreationPromises.set(dir, promise);
    return promise;
}

// 处理单个分片：排序、分页并写入文件 (架构保持：生成多个小文件)
async function processAndWriteSortedPaginatedShards(task) {
    const { basePath, data } = task;
    if (data.length === 0) return 0;

    const currentShardWritePromises = [];
    const metadata = { 
        total_items: data.length, 
        items_per_page: ITEMS_PER_PAGE, 
        pages: {} 
    };

    for (const [sortPrefix, internalKey] of Object.entries(SORT_CONFIG)) {
        // 1. 排序
        const sortedData = [...data].sort((a, b) => (b[internalKey] || 0) - (a[internalKey] || 0));

        const numPages = Math.ceil(sortedData.length / ITEMS_PER_PAGE);
        metadata.pages[sortPrefix] = numPages;

        // 2. 分页和写入
        for (let page = 1; page <= numPages; page++) {
            const start = (page - 1) * ITEMS_PER_PAGE;
            const pageData = sortedData.slice(start, start + ITEMS_PER_PAGE);
            const minifiedPageData = pageData.map(minifyItem);
            
            // 路径：dist/hot/all/region_chinese/by_hs/page_1.json
            const finalPath = path.join(FINAL_OUTPUT_DIR, basePath, `by_${sortPrefix}`, `page_${page}.json`);
            const dir = path.dirname(finalPath);
            
            // 异步写入文件
            const writePromise = ensureDir(dir).then(() => 
                fs.writeFile(finalPath, JSON.stringify(minifiedPageData))
            );
            currentShardWritePromises.push(writePromise);
        }
    }

    // 3. 写入元数据
    const metaPath = path.join(FINAL_OUTPUT_DIR, basePath, 'meta.json');
    const metaDir = path.dirname(metaPath);
    const metaWritePromise = ensureDir(metaDir).then(() => 
        fs.writeFile(metaPath, JSON.stringify(metadata))
    );
    currentShardWritePromises.push(metaWritePromise);

    // 等待该分片的所有文件写入完成
    await Promise.all(currentShardWritePromises);

    // 返回生成的文件数量
    return currentShardWritePromises.length; 
}


// 分片主逻辑
async function shardDatabase() {
    console.log(`\n--- PHASE 4: Sharding, Sorting, Paginating & Writing Files (I/O Concurrency: ${MAX_CONCURRENT_FILE_IO}) ---`);
    console.log('  ⏳ This will generate ~160k files...');
    await fs.rm(FINAL_OUTPUT_DIR, { recursive: true, force: true }).catch(() => {});
    await fs.mkdir(FINAL_OUTPUT_DIR, { recursive: true });
    dirCreationPromises.clear();

    // --- 4.1 加载数据湖 ---
    console.log('  Loading data lake from disk...');
    const database = [];
    try {
        await fs.access(DATA_LAKE_FILE, fsConstants.R_OK);
    } catch (e) {
        console.error(`\n❌ FATAL ERROR: Data Lake file ${DATA_LAKE_FILE} not found. Cannot proceed.`);
        throw new Error("Data Lake file missing.");
    }

    const rl = readline.createInterface({ input: createReadStream(DATA_LAKE_FILE), crlfDelay: Infinity });
    for await (const line of rl) {
        if (line.trim()) {
            database.push(JSON.parse(line));
        }
    }
    console.log(`  ✅ Loaded ${database.length} items from data lake.`);

    // --- 4.2 计算热度分数 ---
    console.log('  Calculating hotness and default scores...');
    const validForStats = database.filter(i => i.vote_count > 100); // 使用有足够票数的项目计算全局平均
    const totalRating = validForStats.reduce((sum, item) => sum + (item.vote_average || 0), 0);
    const GLOBAL_AVERAGE_RATING = validForStats.length > 0 ? totalRating / validForStats.length : 6.8;

    // 计算票数阈值 (75th percentile)
    const sortedVotes = validForStats.map(i => i.vote_count).sort((a,b) => a - b);
    const MINIMUM_VOTES_THRESHOLD = sortedVotes[Math.floor(sortedVotes.length * 0.75)] || 500;

    console.log(`  ℹ️ Global Stats: AvgRating=${GLOBAL_AVERAGE_RATING.toFixed(2)}, MinVotesThreshold=${MINIMUM_VOTES_THRESHOLD}`);

    database.forEach(item => {
        const pop = item.popularity || 0;
        const year = item.release_year || 1970;
        const R = item.vote_average || 0;
        const v = item.vote_count || 0;

        // 计算年份衰减
        const yearDiff = Math.max(0, CURRENT_YEAR - year);
        const timeDecay = 1 / Math.sqrt(yearDiff + 2);

        // 贝叶斯平均评分 (避免低票数高分刷榜)
        const bayesianRating = (v / (v + MINIMUM_VOTES_THRESHOLD)) * R + (MINIMUM_VOTES_THRESHOLD / (v + MINIMUM_VOTES_THRESHOLD)) * GLOBAL_AVERAGE_RATING;
        
        // 热度分 = log(流行度+1) * 时间衰减 * 贝叶斯评分
        item.hotness_score = Math.log10(pop + 1) * timeDecay * bayesianRating;
        item.default_order = pop; // 默认排序使用 TMDB 流行度
    });

    // --- 4.3 收集所有分片任务 ---
    console.log('  Collecting all shard definitions...');
    const shardTasksDefinitions = []; // 存储 { basePath, data }
    const contentTypes = ['all', 'movie', 'tv', 'anime'];
    const definedRegions = REGIONS; 

    // 过滤辅助函数
    const filterData = (baseData, type, region) => {
        let filtered = baseData;
        if (type !== 'all') {
            filtered = filtered.filter(i => i.mediaType === type);
        }
        if (region !== 'all') {
            filtered = filtered.filter(i => i.semantic_tags.includes(region));
        }
        return filtered;
    };

    const addShardDefinition = (pathName, data) => {
        shardTasksDefinitions.push({ basePath: pathName, data: data });
    };

    // --- 定义分片：近期热门、标签/类型、年份、直接类型 ---
    // 1. 近期热门 (hot/all/all, hot/movie/country_cn, ...)
    const recentHotBase = database.filter(i => i.release_year >= RECENT_YEAR_THRESHOLD);
    for (const type of contentTypes) {
        for (const region of definedRegions) {
            const data = filterData(recentHotBase, type, region);
            addShardDefinition(`hot/${type}/${region.replace(':', '_')}`, data);
        }
    }

    // 2. 按分类/主题 (tag/all/all/all, tag/genre_action/movie/country_us, ...)
    const allCategories = ['all', ...GENRES_AND_THEMES]; 
    for (const tag of allCategories) {
        const tagBaseData = (tag === 'all') ? database : database.filter(i => i.semantic_tags.includes(tag));
        for (const type of contentTypes) {
            for (const region of definedRegions) {
                const data = filterData(tagBaseData, type, region);
                addShardDefinition(`tag/${tag.replace(':', '_')}/${type}/${region.replace(':', '_')}`, data);
            }
        }
    }

    // 3. 按年份 (year/all/all/all, year/2023/movie/country_jp, ...)
    const allYears = ['all', ...YEARS]; 
    for (const year of allYears) {
        const yearBaseData = (year === 'all') ? database : database.filter(i => i.release_year === year);
        for (const type of contentTypes) {
             for (const region of definedRegions) {
                const data = filterData(yearBaseData, type, region);
                addShardDefinition(`year/${year}/${type}/${region.replace(':', '_')}`, data);
             }
        }
    }

    // 4. 独立的电影/剧集/动画列表 (movies/all, movies/country_cn, tvseries/all, ...)
    const directTypes = [{ name: 'movies', mediaType: 'movie' }, { name: 'tvseries', mediaType: 'tv' }, { name: 'anime', mediaType: 'anime' }];
    for (const type of directTypes) {
        let baseData = database.filter(i => i.mediaType === type.mediaType);
        for (const region of definedRegions) {
            let data = (region === 'all') ? baseData : baseData.filter(i => i.semantic_tags.includes(region));
            addShardDefinition(`${type.name}/${region.replace(':', '_')}`, data);
        }
    }

    // --- 4.4 并行执行分片任务 ---
    console.log(`  Processing ${shardTasksDefinitions.length} shards in parallel...`);
    const totalFiles = await processInParallel(
        shardTasksDefinitions, 
        MAX_CONCURRENT_FILE_IO, 
        processAndWriteSortedPaginatedShards // 这个函数会返回生成的文件数
    );

    // --- 4.5 生成 Manifest 文件 ---
    await fs.writeFile(path.join(FINAL_OUTPUT_DIR, 'manifest.json'), JSON.stringify({
        buildTimestamp: new Date().toISOString(),
        regions: REGIONS, 
        tags: GENRES_AND_THEMES, 
        years: YEARS,
        itemsPerPage: ITEMS_PER_PAGE, 
        sortOptions: Object.keys(SORT_CONFIG), 
        contentTypes: contentTypes,
        isPaginated: true // 告知客户端这是分页结构
    }));
    
    console.log(`  ✅ Phase 4 complete. Total shards processed: ${shardTasksDefinitions.length}. Total files generated: ${totalFiles}.`);
}



// === 主执行入口 ===

async function main() {
    console.log('🎬 Starting IMDb Discovery Engine Build Process (v2.7 - Reduced Logging)...');
    const startTime = Date.now();

    try {
        // --- 缓存检查 ---
        console.log('\n🔍 Checking Data Lake cache...');
        const cacheValid = await isDataLakeCacheValid(DATA_LAKE_FILE, DATA_LAKE_CACHE_AGE_MS);

        if (cacheValid) {
            console.log('✅ Cache hit! Using existing Data Lake. Skipping Phases 1-3.');
        } else {
            console.log('❌ Cache miss or stale. Starting full build (Phases 1-3)...');
            await buildDataLake();
        }

        // --- 运行 Phase 4 ---
        await shardDatabase(); 

        const duration = (Date.now() - startTime) / 1000;
        console.log(`\n🎉🎉🎉 Build process successful! 🎉🎉🎉`);
        console.log(`Total time: ${duration.toFixed(2)} seconds.`);

    } catch (error) {
        console.error('\n❌ FATAL ERROR during build process:', error.stack || error);
        // 如果构建失败，必须退出码非 0，让 GitHub Actions 知道失败了
        process.exit(1); 
    } finally {
        // 保留 TEMP_DIR 中的 datalake.jsonl 以供下次缓存
        console.log(`\nBuild finished. Cache file preserved at ${DATA_LAKE_FILE}`);
    }
}

main();
