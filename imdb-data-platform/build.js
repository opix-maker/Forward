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
const MAX_CONCURRENT_ENRICHMENTS = 80; // 并发数, 降低一点避免触发rate limit
const MAX_DOWNLOAD_AGE_MS = 20 * 60 * 60 * 1000; // 缓存有效期 (20小时), workflow 12小时跑一次，确保一天至少更新一次
const MIN_VOTES = 2500;         // 最低票数 
const MIN_YEAR = 1990;          // 最早年份
const MIN_RATING = 6.0;         // 最低评分
const ALLOWED_TITLE_TYPES = new Set(['movie', 'tvSeries', 'tvMiniSeries', 'tvMovie']); // 只允许的类型

// --- PATHS ---
const DATASET_DIR = './datasets';
const TEMP_DIR = './temp';
const FINAL_OUTPUT_DIR = './dist'; 
const DATA_LAKE_FILE = path.join(TEMP_DIR, 'datalake.jsonl'); 
const FINAL_DATABASE_FILE = path.join(FINAL_OUTPUT_DIR, 'database.json');


const DATASETS = {
    basics: { url: 'https://datasets.imdbws.com/title.basics.tsv.gz', local: 'title.basics.tsv' },
   // akas: { url: 'https://datasets.imdbws.com/title.akas.tsv.gz', local: 'title.akas.tsv' }, // 优化: 不再使用 akas
    ratings: { url: 'https://datasets.imdbws.com/title.ratings.tsv.gz', local: 'title.ratings.tsv' },
};

 // 优化: 带缓存的下载
async function downloadAndUnzipWithCache(url, localPath, maxAgeMs) {
     const dir = path.dirname(localPath);
     await fs.mkdir(dir, { recursive: true });

    try {
        const stats = await fs.stat(localPath);
        const age = Date.now() - stats.mtimeMs;
        if (age < maxAgeMs) {
            console.log(`  Cache hit: Using local file ${path.basename(localPath)} (age: ${(age / 1000 / 60).toFixed(0)} mins)`);
            return; // Use cached file
        } else {
             console.log(`  Cache expired for ${path.basename(localPath)}. Redownloading.`);
        }
    } catch (error) {
        if (error.code === 'ENOENT') {
           console.log(`  No cache found for ${path.basename(localPath)}. Downloading.`);
        } else {
           console.warn(` Error checking cache for ${localPath}:`, error);
           // Proceed to download despite error
        }
    }
    
    // --- Original Download Logic ---
    console.log(`  Downloading from official URL: ${url}`);
    const response = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0 IMDb Discovery Builder' } });
     if (!response.ok) {
         // Ensure temporary file is removed if download fails partially
         await fs.unlink(localPath).catch(() => {}); 
         const errorBody = await response.text().catch(()=>'');
         throw new Error(`Failed to download ${url} - Status: ${response.status} ${response.statusText}. Body: ${errorBody}`);
    }
    const gunzip = zlib.createGunzip();
    const destination = createWriteStream(localPath);
     console.log(`  Streaming and unzipping to ${path.basename(localPath)}...`);
    await pipeline(response.body, gunzip, destination);
    console.log(`  Download and unzip complete for ${path.basename(localPath)}.`);
}


async function processTsvByLine(filePath, processor) {
     // Check if file exists before trying to read
     try {
       await fs.access(filePath, fsConstants.R_OK);
     } catch {
        console.error(`Cannot access file for reading: ${filePath}. Skipping processing.`);
        return; 
     }

    const fileStream = createReadStream(filePath);
    const rl = readline.createInterface({ input: fileStream, crlfDelay: Infinity });
    let isFirstLine = true;
     let lineCount = 0;
    for await (const line of rl) {
       lineCount++;
        if (isFirstLine) { isFirstLine = false; continue; }
       // Basic check for malformed lines
       if (line && line.includes('\t')) {
           try {
              processor(line, lineCount);
           } catch(e) {
               console.warn(`Error processing line ${lineCount} in ${path.basename(filePath)}: ${e.message} -> ${line.substring(0, 50)}...`);
           }
       }
    }
}

 // 优化: 改进进度日志
async function processInParallel(items, concurrency, task) {
    const queue = [...items];
    let processedCount = 0;
     let successCount = 0;
     let errorCount = 0;
    const totalCount = items.length;
     if (totalCount === 0) return { successCount, errorCount };

     const logProgress = () => {
          const percentage = ((processedCount / totalCount) * 100).toFixed(1);
          // Use process.stdout.write to update the same line (in supporting terminals)
           process.stdout.write(`  Progress: ${processedCount} / ${totalCount} (${percentage}%) | Success: ${successCount} | Errors: ${errorCount}  \r`);
     };

    const worker = async () => {
        while (queue.length > 0) {
            const item = queue.shift();
            if (item) {
                try {
                   const success = await task(item);
                   if(success) successCount++;
                   else errorCount++; // task returns false or null on known skip/error
                } catch(error) {
                    // Catch unexpected errors within task execution
                    console.error(`\n  Worker error processing item ${item}:`, error);
                     errorCount++;
                }
                 processedCount++;
                // Log every 50 or on the last item
                if (processedCount % 50 === 0 || processedCount === totalCount) {
                     logProgress();
                }
            }
        }
    };

    const workers = Array(Math.min(concurrency, totalCount)).fill(null).map(() => worker());
    await Promise.all(workers);
     process.stdout.write('\n'); // New line after progress bar finishes
     return { successCount, errorCount };
}


async function buildAndEnrichToDisk() {
    console.log('\nPHASE 1: Building lightweight indexes & Caching...');
    const ratingsIndex = new Map();
    // const akasIndex = new Map(); // 优化: 移除

    const ratingsPath = path.join(DATASET_DIR, DATASETS.ratings.local);
    await downloadAndUnzipWithCache(DATASETS.ratings.url, ratingsPath, MAX_DOWNLOAD_AGE_MS);
    await processTsvByLine(ratingsPath, (line) => {
        const [tconst, averageRating, numVotes] = line.split('\t');
         // 优化: 提前过滤低分低票，减少Map大小
        const votes = parseInt(numVotes, 10) || 0;
        const rating = parseFloat(averageRating) || 0;
         if (votes >= MIN_VOTES && rating >= MIN_RATING) {
            ratingsIndex.set(tconst, { rating, votes });
         }
    });
     console.log(`  Ratings index built with ${ratingsIndex.size} items meeting vote/rating criteria.`);

    /* 额移除 akas 处理
    const akasPath = path.join(DATASET_DIR, DATASETS.akas.local);
    await downloadAndUnzipWithCache(DATASETS.akas.url, akasPath, MAX_DOWNLOAD_AGE_MS);
     await processTsvByLine(akasPath, (line) => {
        // ...
    });
     console.log(`  Akas index built with ${akasIndex.size} items.`);
    */

    console.log(`\nPHASE 2: Streaming basics and filtering (Votes>=${MIN_VOTES}, Year>${MIN_YEAR}, Rating>=${MIN_RATING}, Types=${[...ALLOWED_TITLE_TYPES].join(',')})...`);
    const idPool = new Set();
    const basicsPath = path.join(DATASET_DIR, DATASETS.basics.local);
    await downloadAndUnzipWithCache(DATASETS.basics.url, basicsPath, MAX_DOWNLOAD_AGE_MS);
    await processTsvByLine(basicsPath, (line) => {
        // tconst	titleType	primaryTitle	originalTitle	isAdult	startYear	endYear	runtimeMinutes	genres
        const [tconst, titleType, , , isAdult, startYear] = line.split('\t');
        
         const year = parseInt(startYear, 10);
         // 优化: 应用更严格的过滤条件
        if (
            !tconst.startsWith('tt') ||               // Must be title
            isAdult === '1' ||                        // No adult
            !ALLOWED_TITLE_TYPES.has(titleType) ||    // Must be allowed type
            isNaN(year) || year <= MIN_YEAR ||        // Must be recent enough
            !ratingsIndex.has(tconst)                 // Must exist in pre-filtered ratings index
            ) 
         {
             return; // Skip
         }
        // rating and votes already checked when building ratingsIndex
        idPool.add(tconst);
    });
     // 打印过滤后的数量，验证效果
    console.log(`  ✅ Filtered ID pool contains ${idPool.size} items to enrich. (Estimate: ${ (idPool.size * 2 * 0.5 / 60).toFixed(1) } mins API time @ 0.5s/call)`);
     if(idPool.size === 0) {
         console.warn("  ⚠️ ID Pool is empty, skipping enrichment phase.");
         return;
     }


    console.log(`\nPHASE 3: Enriching items via TMDB API (Concurrency: ${MAX_CONCURRENT_ENRICHMENTS})...`);
    await fs.rm(TEMP_DIR, { recursive: true, force: true });
    await fs.mkdir(TEMP_DIR, { recursive: true });
    const writeStream = createWriteStream(DATA_LAKE_FILE, { flags: 'a' });
     let writeError = null;
      writeStream.on('error', (err) => {
        console.error("  ❌ Error writing to data lake file:", err);
         writeError = err;
      });


    const enrichmentTask = async (id) => {
         if(writeError) return false; // Stop processing if write stream failed
        try {
             // Find first, then get details - allows correct media_type
            const info = await findByImdbId(id);
            if (!info || !info.id || !info.media_type) return false; // TMDB ID not found or no type

            const details = await getTmdbDetails(info.id, info.media_type);
            if (!details)  return false; // Details not found
               
             const analyzedItem = analyzeAndTagItem(details);
             if (analyzedItem) {
                  // Ensure no newline chars inside JSON string itself
                  writeStream.write(JSON.stringify(analyzedItem).replace(/[\r\n]/g, ' ') + '\n');
                  return true; // Success
             }
             return false; // Analyze failed

        } catch (error) {
            // Errors from fetchFromTmdb (API errors after retries, network errors)
            // console.warn(`\n  Skipping ID ${id} due to enrichment error: ${error.message}`); // Reduce log noise
            return false; // Signal error/skip for this item
        }
    };

    const { successCount, errorCount } = await processInParallel(Array.from(idPool), MAX_CONCURRENT_ENRICHMENTS, enrichmentTask);

    await new Promise(resolve => writeStream.end(resolve));
     if(writeError) throw writeError;
    console.log(`  ✅ Data Lake written to ${DATA_LAKE_FILE}. Enriched: ${successCount}, Skipped/Errors: ${errorCount}.`);
}

async function assembleFinalDatabase() {
     // Check if data lake file exists and is not empty
     try {
         const stats = await fs.stat(DATA_LAKE_FILE);
         if(stats.size === 0) {
            console.log('\nPHASE 4: Data lake is empty. Skipping database assembly.');
            return;
         }
     } catch(e) {
          console.log('\nPHASE 4: Data lake file not found. Skipping database assembly.');
          return;
     }

    console.log('\nPHASE 4: Assembling final database from data lake...');
     // 路径修复: 确保最终目录存在
    await fs.rm(FINAL_OUTPUT_DIR, { recursive: true, force: true }); 
    await fs.mkdir(FINAL_OUTPUT_DIR, { recursive: true });

    const writeStream = createWriteStream(FINAL_DATABASE_FILE);
     let writeError = null;
      writeStream.on('error', (err) => {
         console.error("  ❌ Error writing to final database file:", err);
          writeError = err;
      });

    writeStream.write(`{"buildTimestamp":"${new Date().toISOString()}","database":[`);

    const rl = readline.createInterface({ input: createReadStream(DATA_LAKE_FILE), crlfDelay: Infinity });
    let firstLine = true;
     let itemCount = 0;
    for await (const line of rl) {
        if (!line.trim() || writeError) continue; 
        if (!firstLine) {
            writeStream.write(',');
        }
        writeStream.write(line);
        firstLine = false;
         itemCount++;
    }

    writeStream.write(']}');
    await new Promise(resolve => writeStream.end(resolve));
      if(writeError) throw writeError;
    console.log(`  ✅ Final database (${itemCount} items) written to ${FINAL_DATABASE_FILE}`);
}

async function cleanup() {
    console.log('\nPHASE 5: Cleaning up...');
     // 只清理临时文件和数据集缓存，不清理最终输出目录 FINAL_OUTPUT_DIR
    await fs.rm(TEMP_DIR, { recursive: true, force: true });
    // 可选: 如果想每次强制重新下载，也清理 DATASET_DIR
    // await fs.rm(DATASET_DIR, { recursive: true, force: true }); 
     console.log('  Cleanup complete.');
}

async function main() {
    console.log('Starting IMDb Discovery Engine build process v8.1 (Optimized Velocity)...');
     console.log(`  Target: ${FINAL_DATABASE_FILE}`);
    const startTime = Date.now();
    try {
        await buildAndEnrichToDisk();
        await assembleFinalDatabase();
        const duration = (Date.now() - startTime) / 1000;
        console.log(`\n✅ Build process successful! Took ${duration.toFixed(2)} seconds.`);
    } catch (error) {
        console.error('\n❌ FATAL ERROR during build process:', error.stack || error);
        process.exit(1);
    } finally {
       await cleanup();
    }
}

main();
