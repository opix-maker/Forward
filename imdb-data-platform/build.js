import fs from 'fs/promises';
import { constants as fsConstants, createReadStream, createWriteStream } from 'fs'; 
import path from 'path';
import fetch from 'node-fetch';
import { pipeline } from 'stream/promises';
import zlib from 'zlib';
import readline from 'readline';
import pLimit from 'p-limit'; 
import { findByImdbId, getTmdbDetails } from './src/utils/tmdb_api.js'; 
import { analyzeAndTagItem } from './src/core/analyzer.js'; 

// --- CONFIGURATION ---
const MAX_CONCURRENT_ENRICHMENTS = 80;
const MAX_CONCURRENT_WRITES = 25; 
const MAX_DOWNLOAD_AGE_MS = 20 * 60 * 60 * 1000; 
const MIN_VOTES = 2500; 
const MIN_YEAR = 1990; 
const MIN_RATING = 6.0; 
const ALLOWED_TITLE_TYPES = new Set(['movie', 'tvSeries', 'tvMiniSeries', 'tvMovie']);
const CURRENT_YEAR = new Date().getFullYear();
const RECENT_YEAR_THRESHOLD = CURRENT_YEAR - 1;
const writeLimit = pLimit(MAX_CONCURRENT_WRITES); 

// --- PATHS ---
const DATASET_DIR = './datasets';
const TEMP_DIR = './temp'; 
const FINAL_OUTPUT_DIR = './dist'; 


const DATASETS = {
    basics: { url: 'https://datasets.imdbws.com/title.basics.tsv.gz', local: 'title.basics.tsv' },
    ratings: { url: 'https://datasets.imdbws.com/title.ratings.tsv.gz', local: 'title.ratings.tsv' },
};

// --- 分片配置 ---
const REGIONS = ['all', 'region:chinese', 'region:us-eu', 'region:east-asia', 'country:cn', 'country:hk', 'country:tw', 'country:us', 'country:gb', 'country:jp', 'country:kr', 'country:fr', 'country:de', 'country:ca', 'country:au'];
const GENRES_AND_THEMES = ['genre:爱情', 'genre:冒险', 'genre:悬疑', 'genre:惊悚', 'genre:恐怖', 'genre:科幻', 'genre:奇幻', 'genre:动作', 'genre:喜剧', 'genre:剧情', 'genre:历史', 'genre:战争', 'genre:犯罪', 'theme:whodunit', 'theme:spy', 'theme:courtroom', 'theme:slice-of-life', 'theme:wuxia', 'theme:superhero', 'theme:cyberpunk', 'theme:space-opera', 'theme:time-travel', 'theme:post-apocalyptic', 'theme:mecha', 'theme:zombie', 'theme:monster', 'theme:ghost', 'theme:magic', 'theme:gangster', 'theme:film-noir', 'theme:serial-killer', 'theme:xianxia', 'theme:kaiju', 'theme:isekai'];
const YEARS = Array.from({length: CURRENT_YEAR - 1990 + 1}, (_, i) => 1990 + i).reverse();
const VALID_YEARS_SET = new Set(YEARS); 

// 类型定义，用于分片
 const TYPES = [
    { name: 'movies', tags: ['type:movie'], exclude: [] },
    { name: 'tvseries', tags: ['type:tv'], exclude: ['type:animation'] }, 
    { name: 'anime', tags: ['type:animation'], exclude: [] },
 ];
 const REGIONS_WITHOUT_ALL = REGIONS.filter(r => r !== 'all'); 

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
         console.log(`  Cache miss/expired for ${path.basename(localPath)}. Redownloading.`);
    } catch (e) {
        if (e.code !== 'ENOENT') console.warn(`  Error checking cache for ${localPath}:`, e.message);
        /* no cache or error stating*/
    }
    console.log(`  Downloading from: ${url}`);
     try {
        const response = await fetch(url, { headers: { 'User-Agent': 'IMDb-Builder/1.0' } });
        if (!response.ok) throw new Error(`Failed to download ${url}: ${response.statusText}`);
        const gunzip = zlib.createGunzip();
         // 使用临时文件防止下载中断导致缓存文件损坏
        const tempDownloadPath = `${localPath}.download`;
        const destination = createWriteStream(tempDownloadPath);
        await pipeline(response.body, gunzip, destination);
        await fs.rename(tempDownloadPath, localPath); 
        console.log(`  Download and unzip complete for ${path.basename(localPath)}.`);
     } catch (error) {
        console.error(`  Error downloading or unzipping ${url}:`, error);
        await fs.unlink(`${localPath}.download`).catch(() => {});
        await fs.unlink(localPath).catch(() => {}); 
        throw error; 
     }
}


async function processTsvByLine(filePath, processor) { 
     const fileStream = createReadStream(filePath);
    const rl = readline.createInterface({ input: fileStream, crlfDelay: Infinity });
    let isFirstLine = true;
    let lineCount = 0;
    try {
        for await (const line of rl) {
            lineCount++;
            if (isFirstLine) { isFirstLine = false; continue; }
            if (line && line.trim() && line.includes('\t')) processor(line);
        }
    } catch(err) {
         console.error(`❌ Error reading/processing file ${filePath} at line ~${lineCount}:`, err.message || err);
         throw err;
    } finally {
       // 确保资源释放
       rl.close();
       fileStream.destroy();
    }
}

async function processInParallel(items, concurrency, task) {
    const queue = [...items]; let processedCount = 0; const totalCount = items.length;
    const results = [];
    const errors = [];

    const worker = async () => {
        while (queue.length > 0) {
            const item = queue.shift();
            if (item) {
                 try {
                   const result = await task(item);
                   // 确保只收集非 null/undefined 的结果
                   if (result != null) results.push(result); 
                } catch (error) {
                    errors.push({item, error}); // 收集错误
                     // console.warn 移到外面统一报告
                }
                 // 进度条逻辑移到 try/catch 之外，确保即使出错也计数
                 processedCount++;
                 // 优化进度条刷新频率
                 const reportInterval = Math.max(100, Math.floor(totalCount / 200));
                 if (processedCount % reportInterval === 0 || processedCount === totalCount) {
                      const progress = ((processedCount / totalCount) * 100).toFixed(1);
                      // 增加错误计数显示
                      const errorMsg = errors.length > 0 ? ` (${errors.length} errors)` : '';
                      process.stdout.write(`  Progress: ${processedCount} / ${totalCount} (${progress}%)${errorMsg} \r`);
                 }
            }
        }
    };
     // 防止 concurrency 过大或 items 为空
    const workerCount = Math.min(concurrency, Math.max(1, items.length));
    const workers = Array(workerCount).fill(null).map(() => worker());
    await Promise.all(workers);
     // 确保光标换行
    process.stdout.write(`  Progress: ${processedCount} / ${totalCount} (100.0%)${errors.length > 0 ? ` (${errors.length} errors)` : ''} \n`);
     if(errors.length > 0) {
        console.warn(`  Parallel processing finished with ${errors.length} errors.`);
        // 打印部分错误详情
        errors.slice(0, 5).forEach(({item, error}) => {
           console.warn(`    - Error on ${JSON.stringify(item).substring(0,50)}...: ${error.message}`);
        });
         if(errors.length > 5) console.warn(`    ... and ${errors.length - 5} more errors.`);
     }
     return results; // 返回处理结果
}

// 性能优化: 修改后的写入函数，只写，不处理数据
 const writeShard = async (relativeFilePath, data) => { // data is already minified
    const fullPath = path.join(FINAL_OUTPUT_DIR, relativeFilePath);
    const dir = path.dirname(fullPath);
     try {
       await fs.mkdir(dir, { recursive: true }); // Ensure dir exists
       await fs.writeFile(fullPath, JSON.stringify(data));
     } catch (err) {
        console.error(`❌ Error writing shard file: ${fullPath}`, err.message);
        // 可选：重新抛出或记录
     }
};


// --- 构建与分片流程 ---
async function buildAndShard() {
     // --- PHASE 1 & 2: Indexing, Caching, Filtering ---
    console.log('\nPHASE 1 & 2: Building indexes, Caching & Filtering...');
    await fs.mkdir(DATASET_DIR, { recursive: true });
    const ratingsIndex = new Map();
    const ratingsPath = path.join(DATASET_DIR, DATASETS.ratings.local);
    await downloadAndUnzipWithCache(DATASETS.ratings.url, ratingsPath, MAX_DOWNLOAD_AGE_MS);
     console.log('  Processing ratings...');
    await processTsvByLine(ratingsPath, (line) => {
        const [tconst, averageRating, numVotes] = line.split('\t');
        const votes = parseInt(numVotes, 10) || 0; const rating = parseFloat(averageRating) || 0;
        if (votes >= MIN_VOTES && rating >= MIN_RATING) ratingsIndex.set(tconst, { rating, votes });
    });
    console.log(`  Ratings index built with ${ratingsIndex.size} items.`);

    const idPool = new Set();
    const basicsPath = path.join(DATASET_DIR, DATASETS.basics.local);
    await downloadAndUnzipWithCache(DATASETS.basics.url, basicsPath, MAX_DOWNLOAD_AGE_MS);
     console.log('  Processing basics...');
    await processTsvByLine(basicsPath, (line) => {
        const [tconst, titleType, , , isAdult, startYear] = line.split('\t');
        const year = parseInt(startYear, 10);
        if (!tconst.startsWith('tt') || isAdult === '1' || !ALLOWED_TITLE_TYPES.has(titleType) || isNaN(year) || year < MIN_YEAR || !ratingsIndex.has(tconst)) return;
        idPool.add(tconst);
    });
     ratingsIndex.clear(); 
     console.log(`  Memory usage after indexing (approx MB): ${(process.memoryUsage().heapUsed / 1024 / 1024).toFixed(2)}`);
    console.log(`  Filtered ID pool contains ${idPool.size} items to enrich.`);

    // --- PHASE 3: Enriching  ---
    console.log(`\nPHASE 3: Enriching ${idPool.size} items via TMDB API...`);
     const enrichmentTask = async (id) => {
         // try/catch 移至 processInParallel worker 内部
         const info = await findByImdbId(id);
         if (!info || !info.id || !info.media_type) return null; // 返回 null 表示跳过
         const details = await getTmdbDetails(info.id, info.media_type);
          if (!details) return null;
         const analyzedItem = analyzeAndTagItem(details);
          return analyzedItem; // 返回分析后的对象
    };
    
     // processInParallel 内部已过滤 null
     const enrichedDatabase = await processInParallel(Array.from(idPool), MAX_CONCURRENT_ENRICHMENTS, enrichmentTask);
     idPool.clear(); // 性能优化: 尽早释放内存
      console.log(`  Memory usage after enrichment (approx MB): ${(process.memoryUsage().heapUsed / 1024 / 1024).toFixed(2)}`);
     console.log(`  Enrichment complete. ${enrichedDatabase.length} items loaded into memory for sharding.`);

    // --- PHASE 4: Sharding from Memory  ---
     console.log(`\nPHASE 4: Calculating stats and Sharding ${enrichedDatabase.length} items...`);
     if (enrichedDatabase.length === 0) {
        console.warn("  No items enriched, skipping sharding.");
        // 确保目录存在并写入空的 manifest
        await fs.rm(FINAL_OUTPUT_DIR, { recursive: true, force: true });
        await fs.mkdir(FINAL_OUTPUT_DIR, { recursive: true });
        await fs.writeFile(path.join(FINAL_OUTPUT_DIR, 'manifest.json'), JSON.stringify({
            buildTimestamp: new Date().toISOString(),
            regions: REGIONS,
            tags: GENRES_AND_THEMES,
            years: YEARS,
         }));
        return;
     }
    
     // --- 计算分数和预处理 ---
    const validForStats = enrichedDatabase.filter(i => i && i.vote_count > 100);
     if (validForStats.length === 0) {
         console.warn("  Not enough items with >100 votes to calculate stats, using defaults.");
     }
    const totalRating = validForStats.reduce((sum, item) => sum + (item.vote_average || 0), 0);
    const GLOBAL_AVERAGE_RATING = validForStats.length > 0 ? totalRating / validForStats.length : 6.8;
    const sortedVotes = validForStats.map(i => i.vote_count || 0).sort((a,b) => a - b);
     // 防止数组为空时访问索引出错
    const MINIMUM_VOTES_THRESHOLD = (sortedVotes.length > 0) ? (sortedVotes[Math.floor(sortedVotes.length * 0.75)] || 500) : 500;
    console.log(`  Global Stats: AvgRating=${GLOBAL_AVERAGE_RATING.toFixed(2)}, MinVotes=${MINIMUM_VOTES_THRESHOLD}`);

     // 初始化 Shards Map 和 单次遍历
     const shards = new Map(); // Map<string, any[]> path -> minifiedItems
     const addToShard = (relativeFilePath, minifiedItem) => {
        if (!shards.has(relativeFilePath)) {
            shards.set(relativeFilePath, []);
         }
         shards.get(relativeFilePath).push(minifiedItem);
     };

     console.log('  Processing items & assigning to shards (Single Pass)...');
     // --- 单次遍历开始 ---
    enrichedDatabase.forEach(item => {
         if (!item || !item.semantic_tags) return; // 防御性编程

         // 1. 计算分数
        const pop = item.popularity || 0;
        const year = item.release_year || 1970;
        const R = item.vote_average || 0;
        const v = item.vote_count || 0;
        const yearDiff = Math.max(0, CURRENT_YEAR - year);
         const bayesianRating = (v + MINIMUM_VOTES_THRESHOLD) > 0 
            ? (v / (v + MINIMUM_VOTES_THRESHOLD)) * R + (MINIMUM_VOTES_THRESHOLD / (v + MINIMUM_VOTES_THRESHOLD)) * GLOBAL_AVERAGE_RATING
            : GLOBAL_AVERAGE_RATING;
         // 确保 hotness_score 和 default_order 是有效数字
        const hotness_score = isNaN(bayesianRating) ? 0 : Math.log10(pop + 1) * (1 / Math.sqrt(yearDiff + 2)) * bayesianRating;
        const default_order = pop;
         
        // 使用 Set 进行 O(1) 标签查找
        const itemTagSet = new Set(item.semantic_tags);
        const isTV = itemTagSet.has('type:tv'); 
        const mediaType = isTV ? 'tv' : 'movie';

         // 2. 最小化数据 (在循环内执行)
        const minifiedItem = {
             id: item.id,
             p: item.poster_path,
             b: item.backdrop_path,
             t: item.title,
             r: parseFloat((item.vote_average || 0).toFixed(1)), // 保持一位小数
             y: item.release_year,
             hs: parseFloat((hotness_score || 0).toFixed(3)),
             d: parseFloat((default_order || 0).toFixed(3)),
             mt: mediaType,
             o: item.overview 
        };

         // 3. 分配到 Shards 
         // 3.1. 近期热门
        if (item.release_year >= RECENT_YEAR_THRESHOLD) {
            addToShard('recent_hot.json', minifiedItem);
        }
        // 3.2. 按类型/主题
        for (const tag of GENRES_AND_THEMES) {
             if (itemTagSet.has(tag)) { // O(1)
                 addToShard(path.join('by_tag', tag.replace(':', '_') + '.json'), minifiedItem);
             }
        }
        // 3.3. 按年份
         if (item.release_year && VALID_YEARS_SET.has(item.release_year)) { // O(1)
              addToShard(path.join('by_year', `${item.release_year}.json`), minifiedItem);
         }
        // 3.4. 按电影/剧集/动画 + 地区
        for (const type of TYPES) {
             const matchesType = type.tags.every(t => itemTagSet.has(t)) && !type.exclude.some(e => itemTagSet.has(e));
             if (matchesType) {
                 addToShard(path.join(type.name, 'all.json'), minifiedItem); 
                 for (const region of REGIONS_WITHOUT_ALL) {
                     if (itemTagSet.has(region)) { // O(1)
                         const filename = region.replace(':', '_') + '.json';
                         addToShard(path.join(type.name, filename), minifiedItem);
                     }
                 }
             }
         }
    });
     // --- 单次遍历结束 ---
    enrichedDatabase.length = 0; 
    console.log(`  Assigned items to ${shards.size} shards.`);
    console.log(`  Memory usage after sharding assignment (approx MB): ${(process.memoryUsage().heapUsed / 1024 / 1024).toFixed(2)}`);


    // --- PHASE 5: Writing Shards ---
     console.log(`\nPHASE 5: Writing ${shards.size} shards concurrently to ${FINAL_OUTPUT_DIR}...`);
     await fs.rm(FINAL_OUTPUT_DIR, { recursive: true, force: true });
     // mkdir in writeShard to handle deep paths correctly

     const writePromises = [];
     for (const [relativeFilePath, data] of shards.entries()) {
         writePromises.push(writeLimit(() => writeShard(relativeFilePath, data)));
     }
      writePromises.push(writeLimit(async () => {
          await fs.mkdir(FINAL_OUTPUT_DIR, { recursive: true }); // Ensure base dir exists for manifest
          await fs.writeFile(path.join(FINAL_OUTPUT_DIR, 'manifest.json'), JSON.stringify({
            buildTimestamp: new Date().toISOString(),
            regions: REGIONS,
            tags: GENRES_AND_THEMES,
            years: YEARS,
          }))
       }
      ));

     await Promise.all(writePromises); // 等待所有写入完成
     shards.clear(); // 释放内存
      console.log(`  Memory usage after writing (approx MB): ${(process.memoryUsage().heapUsed / 1024 / 1024).toFixed(2)}`);
    console.log(`  ✅ Sharding complete. Files written to ${FINAL_OUTPUT_DIR}`);
}


// --- 主函数入口 ---
async function main() {
    console.log('Starting IMDb Sharded Build Process (Optimized)...');
     console.log(`Initial memory usage (approx MB): ${(process.memoryUsage().heapUsed / 1024 / 1024).toFixed(2)}`);
    const startTime = Date.now();
    try {
        await buildAndShard(); 
        const duration = (Date.now() - startTime) / 1000;
        console.log(`\n✅ Build process successful! Took ${duration.toFixed(2)} seconds.`);
    } catch (error) {
        console.error('\n❌ FATAL ERROR during build process:', error.stack || error);
        process.exit(1);
    } finally {
         // 清理 TEMP_DIR
        await fs.rm(TEMP_DIR, { recursive: true, force: true }).catch(err => console.warn("Could not clean TEMP_DIR:", err.message));
        console.log(`Final memory usage (approx MB): ${(process.memoryUsage().heapUsed / 1024 / 1024).toFixed(2)}`);
    }
}

main();
