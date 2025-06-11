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

const GENRES_AND_THEMES = [
    'genre:romance',       // genre:爱情
    'genre:adventure',     // genre:冒险
    'genre:mystery',       // genre:悬疑
    'genre:thriller',      // genre:惊悚
    'genre:horror',        // genre:恐怖
    'genre:sci-fi',        // genre:科幻 
    'genre:fantasy',       // genre:奇幻
    'genre:action',        // genre:动作
    'genre:comedy',        // genre:喜剧
    'genre:drama',         // genre:剧情
    'genre:history',       // genre:历史
    'genre:war',           // genre:战争
    'genre:crime',         // genre:犯罪
    'theme:whodunit', 'theme:spy', 'theme:courtroom', 'theme:slice-of-life', 'theme:wuxia', 'theme:superhero', 'theme:cyberpunk', 'theme:space-opera', 'theme:time-travel', 'theme:post-apocalyptic', 'theme:mecha', 'theme:zombie', 'theme:monster', 'theme:ghost', 'theme:magic', 'theme:gangster', 'theme:film-noir', 'theme:serial-killer', 'theme:xianxia', 'theme:kaiju', 'theme:isekai'
];
// +++ }}
const YEARS = Array.from({length: CURRENT_YEAR - 1990 + 1}, (_, i) => 1990 + i).reverse();


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
     // Add timeout to fetch
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 120000); // 2 mins timeout
    try {
       const response = await fetch(url, { 
            headers: { 'User-Agent': 'IMDb-Builder/1.0' },
            signal: controller.signal
        });
        if (!response.ok) throw new Error(`Failed to download ${url}: ${response.statusText}`);
        const gunzip = zlib.createGunzip();
        const destination = createWriteStream(localPath);
        await pipeline(response.body, gunzip, destination);
        console.log(`  Download and unzip complete for ${path.basename(localPath)}.`);
    } catch(error) {
         console.error(`  Error downloading/unzipping ${url}:`, error.message);
         throw error; // re-throw to stop process
    } finally {
        clearTimeout(timeout);
    }
}

async function processTsvByLine(filePath, processor) {
    // Check file exists before processing
     try {
        await fs.access(filePath, fsConstants.R_OK);
     } catch(e) {
         console.error(`Cannot access file for reading: ${filePath}`, e);
         throw new Error(`File access error: ${filePath}`);
     }
     
    const fileStream = createReadStream(filePath);
     fileStream.on('error', (err) => console.error(`Error reading stream from ${filePath}:`, err));
    const rl = readline.createInterface({ input: fileStream, crlfDelay: Infinity });
    let isFirstLine = true;
    let lineCount = 0;
    try {
       for await (const line of rl) {
            lineCount++;
           if (isFirstLine) { isFirstLine = false; continue; }
           if (line && line.includes('\t')) {
              try {
                 processor(line);
              } catch(procError) {
                 console.warn(`Error processing line ${lineCount} in ${path.basename(filePath)}: ${procError.message} -> ${line.substring(0, 50)}...`);
                 // Continue processing other lines
              }
           }
       }
     } catch(readError) {
        console.error(`Error during line-by-line reading of ${filePath}:`, readError);
        throw readError; // Stop if reading fails fundamentally
     }
}

// Simple retry helper for API calls
async function retry(fn, retries = 3, delay = 1000, name="operation") {
    for (let i = 0; i < retries; i++) {
        try {
            return await fn();
        } catch (error) {
             console.warn(`  Retry ${i+1}/${retries} for ${name} failed: ${error.message}`);
            if (i < retries - 1) {
                await new Promise(resolve => setTimeout(resolve, delay * (i + 1))); // backoff
            } else {
                throw error; // re-throw last error
            }
        }
    }
}


async function processInParallel(items, concurrency, task) {
    const queue = [...items]; let processedCount = 0; const totalCount = items.length;
     if (totalCount === 0) return;
     
    const worker = async (workerId) => {
        while (queue.length > 0) {
            const item = queue.shift();
            if (item) {
                 try {
                   await task(item);
                 } catch (taskError) {
                    // Log error but allow other tasks/workers to continue
                    console.error(`\n  Worker ${workerId}: Error processing item ${item}:`, taskError.message);
                 }
                processedCount++;
                if (processedCount % Math.max(100, Math.floor(totalCount/20)) === 0 || processedCount === totalCount) {
                   const percent = ((processedCount / totalCount) * 100).toFixed(1);
                    process.stdout.write(`  Progress: ${processedCount} / ${totalCount} (${percent}%) \r`);
                }
            }
        }
    };
    console.log(`  Starting ${Math.min(concurrency, totalCount)} workers for ${totalCount} items...`);
    const workers = Array(Math.min(concurrency, totalCount)).fill(null).map((_, idx) => worker(idx));
    await Promise.all(workers);
    process.stdout.write('\n'); // Ensure newline after progress bar
}

// --- 主要构建流程 ---

async function buildDataLake() {
    console.log('\nPHASE 1: Building indexes & Caching...');
    const ratingsIndex = new Map();
    const ratingsPath = path.join(DATASET_DIR, DATASETS.ratings.local);
    await downloadAndUnzipWithCache(DATASETS.ratings.url, ratingsPath, MAX_DOWNLOAD_AGE_MS);
    await processTsvByLine(ratingsPath, (line) => {
        const [tconst, averageRating, numVotes] = line.split('\t');
        // Check validity before parsing
        if (!tconst || !averageRating || !numVotes) return;
        const votes = parseInt(numVotes, 10); 
        const rating = parseFloat(averageRating);
         // Check NaN
        if (isNaN(votes) || isNaN(rating)) return;
        if (votes >= MIN_VOTES && rating >= MIN_RATING) ratingsIndex.set(tconst, { rating, votes });
    });
    console.log(`  Ratings index built with ${ratingsIndex.size} items meeting criteria (>=${MIN_VOTES} votes, >=${MIN_RATING} rating).`);

    console.log(`\nPHASE 2: Streaming basics & filtering...`);
    const idPool = new Set();
    const basicsPath = path.join(DATASET_DIR, DATASETS.basics.local);
    await downloadAndUnzipWithCache(DATASETS.basics.url, basicsPath, MAX_DOWNLOAD_AGE_MS);
    await processTsvByLine(basicsPath, (line) => {
        const [tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes, genres] = line.split('\t');
        if (!tconst || !titleType || !startYear || startYear === '\\N') return;
        const year = parseInt(startYear, 10);
        if (!tconst.startsWith('tt') || 
            isAdult === '1' || 
            !ALLOWED_TITLE_TYPES.has(titleType) || 
            isNaN(year) || 
            year < MIN_YEAR || 
            !ratingsIndex.has(tconst)) {
           return; // Skip item if any condition fails
        }
        idPool.add(tconst);
    });
    console.log(`  Filtered ID pool contains ${idPool.size} items to enrich (>=${MIN_YEAR}, allowed types, has rating).`);
     if (idPool.size === 0) {
        console.warn("  WARNING: No items passed filtering. Data lake will be empty. Check MIN_VOTES, MIN_RATING, MIN_YEAR.");
        return; // No need to proceed to enrichment
     }

    console.log(`\nPHASE 3: Enriching items via TMDB API...`);
    await fs.rm(DATA_LAKE_FILE, { force: true }); // Only remove the specific file
    await fs.mkdir(TEMP_DIR, { recursive: true }); // Ensure directory exists
    
    const writeStream = createWriteStream(DATA_LAKE_FILE, { flags: 'a' });
     writeStream.on('error', (err) => {
         console.error("  ERROR writing to data lake file:", err);
     });

     let enrichedCount = 0;
    const enrichmentTask = async (id) => {
         try {
             // Add retry logic for API calls
            const info = await retry(() => findByImdbId(id), 3, 500, `findByImdbId(${id})`);
            if (!info || !info.id || !info.media_type) return;
            
            const details = await retry(() => getTmdbDetails(info.id, info.media_type), 3, 500, `getTmdbDetails(${info.id}, ${info.media_type})`);
            if (details) {
                const analyzedItem = analyzeAndTagItem(details); 
                if (analyzedItem && Array.isArray(analyzedItem.semantic_tags)) { // +++ }}
                     // Use locking or just rely on atomic appends for JSONL
                    writeStream.write(JSON.stringify(analyzedItem) + '\n');
                    enrichedCount++;
                } else if (analyzedItem) {
                }
            }
        } catch (error) {
            console.warn(`\n  Skipping ID ${id} due to enrichment error: ${error.message}`);
        }
    };
    
    await processInParallel(Array.from(idPool), MAX_CONCURRENT_ENRICHMENTS, enrichmentTask);
    await new Promise(resolve => writeStream.end(resolve));
    console.log(`  Data Lake written to ${DATA_LAKE_FILE} with ${enrichedCount} enriched items.`);
}

async function shardDatabase() {
     try {
       const stats = await fs.stat(DATA_LAKE_FILE);
        if(stats.size === 0) {
           console.warn("\nPHASE 4 Skipped: Data lake file is empty. No data to shard.");
           return;
        }
     } catch(e) {
        console.warn("\nPHASE 4 Skipped: Data lake file not found or inaccessible. No data to shard.");
        return;
     }

    console.log('\nPHASE 4: Sharding database from data lake...');
    await fs.rm(FINAL_OUTPUT_DIR, { recursive: true, force: true });
    await fs.mkdir(FINAL_OUTPUT_DIR, { recursive: true });

    const database = [];
     const uniqueTags = new Set(); // {{ +++ FIX (Mechanism 2) : Collect unique tags +++ }}
    const rl = readline.createInterface({ input: createReadStream(DATA_LAKE_FILE), crlfDelay: Infinity });
     let lineNum = 0;
    for await (const line of rl) { 
       lineNum++;
       if (line.trim()) {
          try {
            const item = JSON.parse(line);
            database.push(item);
            getTags(item).forEach(tag => uniqueTags.add(tag)); 
             
          } catch(parseError) {
             console.warn(`  Skipping corrupt JSON line ${lineNum} in data lake: ${parseError.message} -> ${line.substring(0, 80)}...`);
          }
       }
    }
    console.log(`  Loaded ${database.length} valid items from data lake.`);
     if (database.length === 0) return;

    console.log(`\n--- 诊断信息: 数据湖分析 (PHASE 4) ---`);
    console.log(`  总计有效条目: ${database.length}`);
    console.log(`  发现的唯一标签 (${uniqueTags.size}):`, Array.from(uniqueTags).sort()); 
    console.log(`  标签样本 (前3条):`, database.slice(0, 3).map(item => ({id: item.id, tags: getTags(item)})) );
     console.log(`  >> 请检查上方 "发现的唯一标签" 列表，确保 GENRES_AND_THEMES 和 types 定义中的字符串与其完全一致！ <<`);
    console.log(`----------------------------------------\n`);
    


    // --- 计算分数和预处理 ---
    const validForStats = database.filter(i => i.vote_count > 100);
    const GLOBAL_AVERAGE_RATING = validForStats.length > 0 
       ? validForStats.reduce((sum, item) => sum + (item.vote_average || 0), 0) / validForStats.length
       : 6.8; // Default fallback
       
    const sortedVotes = validForStats.map(i => i.vote_count).sort((a,b) => a - b);
    const MINIMUM_VOTES_THRESHOLD = (sortedVotes.length > 0)
       ? sortedVotes[Math.floor(sortedVotes.length * 0.75)] 
       : 500; // Default fallback

    console.log(`  Global Stats: AvgRating=${GLOBAL_AVERAGE_RATING.toFixed(2)}, MinVotes75thPercentile=${MINIMUM_VOTES_THRESHOLD}`);

    database.forEach(item => {
        const pop = item.popularity || 0;
        const year = item.release_year || 1970;
        const R = item.vote_average || 0;
        const v = item.vote_count || 0;
        const yearDiff = Math.max(0, CURRENT_YEAR - year);
        const bayesianRating = (v + MINIMUM_VOTES_THRESHOLD > 0)
           ? (v / (v + MINIMUM_VOTES_THRESHOLD)) * R + (MINIMUM_VOTES_THRESHOLD / (v + MINIMUM_VOTES_THRESHOLD)) * GLOBAL_AVERAGE_RATING
           : GLOBAL_AVERAGE_RATING;
           
        item.hotness_score = Math.log10(pop + 1) * (1 / Math.sqrt(yearDiff + 2)) * bayesianRating;
        item.default_order = pop;
        
        const tags = getTags(item); // Safe access
        if (tags.includes('type:animation')) {
            item.mediaType = 'anime';
        } else if (tags.includes('type:tv')) {
             item.mediaType = 'tv';
        } else { // Default or type:movie
             item.mediaType = 'movie'; 
        }
        
    });


    const writeShard = async (filePath, data) => {
        const relativePath = path.relative(process.cwd(), path.join(FINAL_OUTPUT_DIR, filePath)); // Cleaner log path
        console.log(`  - 正在写入 ${data.length} 条目到: ${relativePath}`);
        if (data.length === 0) {
             console.warn(`    ! 警告: 分片 '${relativePath}' 未匹配到数据! 请检查标签定义与诊断日志。`);

        }
        

        const fullPath = path.join(FINAL_OUTPUT_DIR, filePath);
        const dir = path.dirname(fullPath);
        await fs.mkdir(dir, { recursive: true });
                
        const minifiedData = data.map(item => ({
             id: item.id,
             p: item.poster_path,
             b: item.backdrop_path,
             t: item.title,
             r: item.vote_average ? parseFloat(item.vote_average.toFixed(2)) : 0, // Format rating
             y: item.release_year,
             hs: Number.isFinite(item.hotness_score) ? parseFloat(item.hotness_score.toFixed(3)) : 0, 
             d: Number.isFinite(item.default_order) ? parseFloat(item.default_order.toFixed(3)) : 0,
             mt: item.mediaType, // Uses the corrected mediaType
             o: item.overview || '' // Ensure overview exists
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
        await writeShard(path.join('by_tag', filename), database.filter(i => hasTag(i, tag)));
         
    }
    
    // 3. 按年份
    for (const year of YEARS) {
        await writeShard(path.join('by_year', `${year}.json`), database.filter(i => i.release_year === year));
    }
    
    // 4. 按电影/剧集/动画 + 地区
    const types = [
        { name: 'movies', tags: ['type:movie'], exclude: [] },
        { name: 'tvseries', tags: ['type:tv'], exclude: ['type:animation'] }, // 日剧/日漫分离 (Exclude anime from general tv)
        { name: 'anime', tags: ['type:animation'], exclude: [] }, // Anime category relies on this tag
    ];
    for (const type of types) {
        let baseData = database.filter(i => {
            const itemTags = getTags(i);
            return type.tags.every(t => itemTags.includes(t)) && !type.exclude.some(e => itemTags.includes(e));
        });
        for (const region of REGIONS) {
            let data = (region === 'all') ? baseData : baseData.filter(i => hasTag(i, region));
            const filename = region.replace(':', '_') + '.json';
            await writeShard(path.join(type.name, filename), data);
        }
    }
    
    await fs.writeFile(path.join(FINAL_OUTPUT_DIR, 'manifest.json'), JSON.stringify({
        buildTimestamp: new Date().toISOString(),
        regions: REGIONS,
        tags: GENRES_AND_THEMES, // Reflects the updated English tags
        years: YEARS,
         itemCount: database.length,
         uniqueTagsFound: Array.from(uniqueTags).sort(),
    }, null, 2)); // pretty print manifest
    console.log(`  ✅ Sharding complete. Files written to ${FINAL_OUTPUT_DIR}`);
}

async function main() {
    console.log('Starting IMDb Sharded Build Process...');
     await fs.mkdir(DATASET_DIR, {recursive: true});
     await fs.mkdir(TEMP_DIR, {recursive: true});
     await fs.mkdir(FINAL_OUTPUT_DIR, {recursive: true});

    const startTime = Date.now();
    try {
        await buildDataLake();
        await shardDatabase();
        const duration = (Date.now() - startTime) / 1000;
        console.log(`\n✅ Build process successful! Took ${duration.toFixed(2)} seconds.`);
    } catch (error) {
        console.error('\n❌ FATAL ERROR during build process:', error.stack || error); // Log stack trace
        process.exit(1);
    } finally {
        console.log("\nCleanup: Keeping TEMP_DIR for inspection. Manually delete if needed.");

    }
}

main();
