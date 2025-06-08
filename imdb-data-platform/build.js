import fs from 'fs/promises';
import path from 'path';
import fetch from 'node-fetch';
import { pipeline } from 'stream/promises';
import { createWriteStream, createReadStream } from 'fs';
import zlib from 'zlib';
import readline from 'readline';
// Mock imports if you don't run it with the actual files, 
// but assuming they exist for the fix context
// import { findByImdbId, getTmdbDetails } from './src/utils/tmdb_api.js';
// import { analyzeAndTagItem } from './src/core/analyzer.js';
 // --- MOCKS for standalone testing ---
 const findByImdbId = async (id) => ({ id: `tmdb_${id}`, media_type: 'movie'});
 const getTmdbDetails = async (id, type) => ({ id, type, title: `Title ${id}`});
 const analyzeAndTagItem = (details) => ({...details, semantic_tags: ['test', Math.random()>0.8?'cyberpunk':'other',  Math.random()>0.7?'zombie':'other'] });
 // --- END MOCKS ---


const DATASET_DIR = './datasets';
const OUTPUT_DIR = './dist';
const MAX_CONCURRENT_ENRICHMENTS = 100; 
// FIX 3a: Define how many candidates to fetch/enrich for keyword lists before applying the final limit
const KEYWORD_CANDIDATE_POOL_SIZE = 1500; 

const DATASETS = {
    basics: { url: 'https://datasets.imdbws.com/title.basics.tsv.gz', local: 'title.basics.tsv' },
    akas: { url: 'https://datasets.imdbws.com/title.akas.tsv.gz', local: 'title.akas.tsv' },
    ratings: { url: 'https://datasets.imdbws.com/title.ratings.tsv.gz', local: 'title.ratings.tsv' },
};

// --- 数据切片矩阵：定义最终输出的JSON文件 ---
const BUILD_MATRIX = {
    // 官方榜单
    top_movies_250: { name: 'IMDb Top 250 电影', filters: { types: ['movie'], minVotes: 25000 }, sortBy: 'rating', limit: 250 },
    top_tv_250: { name: 'IMDb Top 250 剧集', filters: { types: ['tvseries', 'tvminiseries'], minVotes: 10000 }, sortBy: 'rating', limit: 250 },
    // 亚洲精选
    top_jp_anime: { name: '高分日系动画', filters: { types: ['movie', 'tvseries'], regions: ['JP'], genres: ['Animation'], minVotes: 1000 }, sortBy: 'rating',  limit: 100 }, // Added minVotes here too just in case
    top_kr_tv: { name: '高分韩剧', filters: { types: ['tvseries'], regions: ['KR'], minVotes: 1000 }, sortBy: 'rating', limit: 100 },
    top_cn_movie: { name: '高分国产电影', filters: { types: ['movie'], regions: ['CN', 'HK', 'TW'], minVotes: 1000 }, sortBy: 'rating', limit: 100 },
    top_cn_tv: { name: '高分国产剧', filters: { types: ['tvseries'], regions: ['CN', 'HK', 'TW'], minVotes: 500 }, sortBy: 'rating',  limit: 100 },
    // 主题探索
    theme_cyberpunk: { name: '赛博朋克精选', filters: { types: ['movie', 'tvseries', 'tvminiseries'], genres: ['Sci-Fi'], keywords: ['cyberpunk', 'dystopia'], minVotes: 500 }, sortBy: 'rating', limit: 50 }, // Added types/minVotes
    theme_zombie: { name: '僵尸末日', filters: {  types: ['movie', 'tvseries', 'tvminiseries'], genres: ['Horror'], keywords: ['zombie'], minVotes: 500 }, sortBy: 'rating', limit: 50 },// Added types/minVotes
     theme_wuxia: { name: '武侠世界', filters: { types: ['movie', 'tvseries'], genres: ['Action', 'Adventure'], regions: ['CN', 'HK', 'TW'], keywords: ['wuxia', 'martial-arts'], minVotes: 500}, sortBy: 'rating', limit: 50 },// Added types/minVotes
    // 近期热门 (这个无法从数据集直接获得，需要保留网页抓取作为补充)
    // weekly_trending: { name: '本周热门', crawl: true, path: '/chart/moviemeter/', limit: 100 },
     // Example with no filters, just to test the fix
    // no_filter_example: { name: 'No filter', sortBy: 'votes', limit: 10 },
};


async function downloadAndUnzip(url, localPath) {
     // FIX 5: Use path.resolve or ensure localPath is correctly formed
    const resolvedPath = path.resolve(localPath);
    const dir = path.dirname(resolvedPath);
     try {
       await fs.access(resolvedPath);
        console.log(`  File already exists, skipping download: ${path.basename(resolvedPath)}`);
       return; // Skip if file exists
     } catch (error) {
        // File does not exist, proceed with download
     }

    await fs.mkdir(dir, { recursive: true });
    console.log(`  Downloading from official URL: ${url}`);
     // Add timeout to fetch
    const response = await fetch(url, { 
        headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36' },
       // signal: AbortSignal.timeout(300000) // 5 minutes timeout 
     });
    if (!response.ok) {
        const errorBody = await response.text();
         // Clean up potentially partial file on error
         await fs.unlink(resolvedPath).catch(()=> {});
        throw new Error(`Failed to download ${url} - Status: ${response.status} ${response.statusText}. Body: ${errorBody}`);
    }
     // Check content type? IMDB sometimes returns HTML error pages with 200 OK
     if (!response.headers.get('content-type')?.includes('application/x-gzip') && !response.headers.get('content-type')?.includes('application/gzip')) {
         console.warn(`  Warning: Expected gzip but got ${response.headers.get('content-type')} for ${url}`);
         // Depending on strictness, you might want to throw an error here
     }


    const gunzip = zlib.createGunzip();
    const destination = createWriteStream(resolvedPath);
     console.log(`  Starting pipeline for ${path.basename(resolvedPath)}...`);
    await pipeline(response.body, gunzip, destination);
    console.log(`  Download and unzip complete for ${path.basename(resolvedPath)}.`);
}

async function processTsvByLine(filePath, processor) {
     const resolvedPath = path.resolve(filePath); // FIX 5
    const fileStream = createReadStream(resolvedPath);
     // Handle stream errors
     fileStream.on('error', (err) => {
       console.error(`Error reading file stream ${filePath}:`, err);
        throw err;
     });
    const rl = readline.createInterface({ input: fileStream, crlfDelay: Infinity });
    let isFirstLine = true;
    let count = 0;
     try {
      for await (const line of rl) {
          if (isFirstLine) { isFirstLine = false; continue; }
           // Skip empty lines
           if (!line.trim()) continue; 
          processor(line, count++);
       }
      } catch(err) {
        console.error(`Error processing line in ${filePath}:`, err);
        // Decide if you want to rethrow or just log
         throw err; 
      } finally {
         rl.close();
         fileStream.destroy();
      }
}

async function buildLocalDatabase() {
    console.log('\nPHASE 1: Building local IMDb database from datasets...');
    const db = new Map();

    // FIX 5: Use path.join consistently
    const ratingsPath = path.join(DATASET_DIR, DATASETS.ratings.local);
    await downloadAndUnzip(DATASETS.ratings.url, ratingsPath);
     console.log("  Processing Ratings...");
    await processTsvByLine(ratingsPath, (line) => {
        const [tconst, averageRating, numVotes] = line.split('\t');
         if (!tconst || tconst === '\\N') return;
        db.set(tconst, { rating: parseFloat(averageRating) || 0, votes: parseInt(numVotes, 10) || 0 });
    });
     console.log(`  Ratings processed. DB size: ${db.size}`);


    const akasPath = path.join(DATASET_DIR, DATASETS.akas.local);
     await downloadAndUnzip(DATASETS.akas.url, akasPath);
     console.log("  Processing AKAs (Regions)...");
     let akasCount = 0;
    await processTsvByLine(akasPath, (line, count) => {
       // if (count % 5000000 === 0) console.log(`    AKAs processed: ${count}`); // Progress indicator for huge file
        const [titleId, , , region] = line.split('\t');
        if (!region || region === '\\N' || !db.has(titleId)) return;
        const entry = db.get(titleId); // Get entry once
         if (!entry) return; // Should not happen due to !db.has, but safe
        if (!entry.regions) entry.regions = new Set();
        entry.regions.add(region);
         akasCount++;
    });
     console.log(`  AKAs regions added: ${akasCount}`);


    const basicsPath = path.join(DATASET_DIR, DATASETS.basics.local);
    await downloadAndUnzip(DATASETS.basics.url, basicsPath);
     console.log("  Processing Basics...");
      let basicsCount = 0;
     await processTsvByLine(basicsPath, (line, count) => {
        // if (count % 1000000 === 0) console.log(`    Basics processed: ${count}`);
        const [tconst, titleType, , , isAdult, , , runtimeMinutes, genres] = line.split('\t'); // Added runtime
         // only keep items that already have a rating AND are not adult
        if (!db.has(tconst) || isAdult === '1') {
             // Important: If it has a rating but IS adult, remove it entirely
            if (db.has(tconst)) db.delete(tconst); 
            return;
        };
        const entry = db.get(tconst);
        entry.type = titleType ? titleType.toLowerCase() : 'unknown';
         // FIX 6: Handle \\N in genres
        entry.genres = (genres && genres !== '\\N') ? genres.split(',').filter(g => g && g !== '\\N') : [];
         entry.runtime = parseInt(runtimeMinutes, 10) || null; // added runtime
         basicsCount++;
    });
     console.log(`  Basics merged: ${basicsCount}. Final clean DB size: ${db.size}`);

    
    console.log(`  Local database built with ${db.size} entries.`);
     // FIX 2: Correctly map from Map [key, value] pairs to Array of {id, ...value} objects
    // return Array.from(db.values()).map(item => ({ id: item.id, ...item })); // OLD WRONG CODE
     return Array.from(db, ([id, value]) => ({ id, ...value })); // NEW CORRECT CODE
}

// Keep original behaviour: task modifies external state (enrichedDataLake)
// The return value of processInParallel isn't actually used in main()
async function processInParallel(items, concurrency, task) {
    const queue = [...items];
    // const results = []; // Not used if task has side-effects
    let processedCount = 0;
    const totalCount = items.length;
     if (totalCount === 0) return; // Handle empty input

    const worker = async (workerId) => {
       // console.log(`  Worker ${workerId} started.`);
        while (queue.length > 0) {
            const item = queue.shift(); // Get item from the start
            if (item === undefined) break; // Double check if queue became empty
            try {
                 await task(item);
               // const result = await task(item);
               // if (result) results.push(result); // Only if task returns data and we need results array
            } catch (error) {
                 // Use item.id or just item depending on what is passed
                console.warn(`  Task for item ${String(item).substring(0, 20)} failed: ${error.message}`);
            }
             // Synchonisation point or atomic op needed for perfect count, but good enough for logging
            processedCount++; 
            if (processedCount % Math.ceil(totalCount / 20) === 0 || processedCount === totalCount) { // Log ~20 times
                const percent = ((processedCount / totalCount) * 100).toFixed(1);
                console.log(`  Enrichment Progress: ${processedCount} / ${totalCount} (${percent}%)`);
            }
        }
       // console.log(`  Worker ${workerId} finished.`);
    };

    const workers = [];
    // Ensure concurrency isn't more than items
    const activeWorkers = Math.min(concurrency, totalCount);
     console.log(`  Starting ${activeWorkers} workers...`);
    for (let i = 0; i < activeWorkers; i++) {
        workers.push(worker(i));
    }
    await Promise.all(workers);
    // return results; // Not used
}

async function main() {
    console.log('Starting IMDb Discovery Engine build process v9.0 (Apex)...');
    const startTime = Date.now();
    try {
        await fs.mkdir(DATASET_DIR, { recursive: true });
         // FIX 5: Use path.join
        await fs.rm(path.join(OUTPUT_DIR), { recursive: true, force: true });
        await fs.mkdir(path.join(OUTPUT_DIR), { recursive: true });
        
        const localDB = await buildLocalDatabase();

        console.log('\nPHASE 2: Generating ID lists from local database...');
        const listBuckets = {};
        const allImdbIdsToEnrich = new Set();

        for (const [key, config] of Object.entries(BUILD_MATRIX)) {
            console.log(`  Processing matrix: ${key}`);
            let results = [...localDB]; // Work on a copy
            
            // FIX 1: Add fallback `|| {}` to prevent crash if config.filters is undefined
            const { types, minVotes = 0, regions, genres, keywords } = config.filters || {}; // Get keywords here

            // Apply filters (excluding keywords)
            if (types) results = results.filter(item => item.type && types.includes(item.type));
            // Ensure item.votes exists and is a number
            if (minVotes > 0) results = results.filter(item => typeof item.votes === 'number' && item.votes >= minVotes); 
             // FIX 4: Check item.regions exists before spreading
            if (regions) results = results.filter(item => item.regions && [...item.regions].some(r => regions.includes(r)));
             // FIX 4: Check item.genres exists before calling .some
            if (genres) results = results.filter(item => item.genres && item.genres.length > 0 && genres.some(g => item.genres.includes(g)));
            
             // Ensure sort key exists and is a number
            if(config.sortBy) {
               results.sort((a, b) => (b[config.sortBy] || 0) - (a[config.sortBy] || 0));
             }

            const finalLimit = config.limit || 100;
             // FIX 3b: Determine the limit for PHASE 2. 
             // If keywords exist, fetch a larger pool to enrich, otherwise use the final limit.
             const preEnrichmentLimit = keywords 
                  ? Math.max(finalLimit, KEYWORD_CANDIDATE_POOL_SIZE) // Get more candidates if keywords are involved
                  : finalLimit; // Get exactly the limit if no keywords

             // FIX 2 check: Ensure item.id is used here (relies on buildLocalDatabase fix)
            const idList = results.slice(0, preEnrichmentLimit).map(item => item.id); 
            listBuckets[key] = idList;
            idList.forEach(id => allImdbIdsToEnrich.add(id));
             console.log(`    -> Selected ${idList.length} candidates (pre-enrichment).`);
        }
        console.log(`  Phase 2 complete. Found ${allImdbIdsToEnrich.size} unique IDs to enrich.`);

        console.log(`\nPHASE 3: Enriching ${allImdbIdsToEnrich.size} unique items from final lists...`);
        const enrichedDataLake = new Map();
        // Task populates enrichedDataLake via closure
        const enrichmentTask = async (id) => {
             if (!id) return; // Skip if ID is null/undefined
            try {
               const info = await findByImdbId(id);
                if (!info) { 
                   // console.log(`    Could not find TMDB info for ${id}`);
                    return;
                }
               const details = await getTmdbDetails(info.id, info.media_type);
                if (details) {
                   // NOTE: analyzeAndTagItem must return object with `semantic_tags` array for keywords to work
                    const analyzedItem = analyzeAndTagItem(details); 
                    if (analyzedItem) {
                         // Optional: merge original imdb data if needed for sorting later
                         // const original = localDB.find(item => item.id === id);
                         // enrichedDataLake.set(id, {...analyzedItem, imdb_rating: original?.rating, imdb_votes: original?.votes });
                        enrichedDataLake.set(id, analyzedItem);
                    }
                }
             } catch (e) {
                 // Errors during TMDB fetch or analysis
                 console.warn(`    Enrichment failed for ID ${id}: ${e.message}`);
             }
        };
         // Pass the array of unique IDs
        await processInParallel(Array.from(allImdbIdsToEnrich), MAX_CONCURRENT_ENRICHMENTS, enrichmentTask); 
        console.log(`  Enrichment complete. Data lake contains ${enrichedDataLake.size} items.`);

        console.log('\nPHASE 4: Writing final data marts...');
        // await fs.rm(OUTPUT_DIR, { recursive: true, force: true }); // Moved to start
        // await fs.mkdir(OUTPUT_DIR, { recursive: true }); // Moved to start

        for (const [key, config] of Object.entries(BUILD_MATRIX)) {
             // Get the (potentially larger) list of IDs from Phase 2, map them to enriched data
             // filter(Boolean) removes IDs that failed enrichment. Order is preserved.
            let dataToWrite = listBuckets[key]
                .map(id => enrichedDataLake.get(id))
                .filter(Boolean); // item must exist (enrichment succeeded)

            // FIX 1 & 3c: Safely get keywords and final limit
             const { keywords } = config.filters || {};
             const finalLimit = config.limit || 100;
            
            // 关键词筛选需要在增强后进行
            if (keywords && keywords.length > 0) {
                 // FIX 4: check item.semantic_tags exists
                 dataToWrite = dataToWrite.filter(item => 
                     item.semantic_tags && 
                     Array.isArray(item.semantic_tags) &&
                     keywords.some(kw => item.semantic_tags.some(tag => tag.includes(kw)))
                  );
               // console.log(`    Keyword filter for ${key}: found ${dataToWrite.length} matches in pool.`);
            } 
             
            // FIX 3d: >>> Apply the FINAL limit HERE <<<<
            // If no keywords: dataToWrite already has length <= finalLimit from Phase 2, slice does no harm.
            // If keywords: dataToWrite had length <= KEYWORD_CANDIDATE_POOL_SIZE, was filtered by keywords, 
            // and is NOW cut down to the actual desired finalLimit.
            dataToWrite = dataToWrite.slice(0, finalLimit);

             // FIX 5: Use path.join
             const outputPath = path.join(OUTPUT_DIR, `${key}.json`);
            await fs.writeFile(outputPath, JSON.stringify(dataToWrite, null, 2)); // Prettier JSON
            console.log(`  Generated list: ${config.name} -> ${key}.json (${dataToWrite.length} items)`);
        }

         // FIX 5: Use path.join
        const index = { buildTimestamp: new Date().toISOString(), lists: Object.entries(BUILD_MATRIX).map(([id, { name }]) => ({ id, name, file: `${id}.json` })) };
        await fs.writeFile(path.join(OUTPUT_DIR, 'index.json'), JSON.stringify(index, null, 2)); // Prettier JSON
        console.log(`\nSuccessfully wrote index file.`);

        const duration = (Date.now() - startTime) / 1000;
        console.log(`\n✅ Build process successful! Took ${duration.toFixed(2)} seconds.`);

    } catch (error) {
        console.error('\n❌ FATAL ERROR during build process:', error.stack || error); // Log stack trace
        process.exit(1);
    }
}

main();
