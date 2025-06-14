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
const ITEMS_PER_PAGE = 30; 
const SORT_CONFIG = {
    // key: 客户端使用的短名称, value: data lake 中的完整属性名
    'hs': 'hotness_score', // 热度
    'r': 'vote_average',   // 评分
    'd': 'default_order',  // 默认 (popularity)
};

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
// 注意: 'all' 必须包含在内
const REGIONS = ['all', 'region:chinese', 'region:us-eu', 'region:east-asia', 'country:cn', 'country:hk', 'country:tw', 'country:us', 'country:gb', 'country:jp', 'country:kr', 'country:fr', 'country:de', 'country:ca', 'country:au'];
const GENRES_AND_THEMES = ['genre:爱情', 'genre:冒险', 'genre:悬疑', 'genre:惊悚', 'genre:恐怖', 'genre:科幻', 'genre:奇幻', 'genre:动作', 'genre:喜剧', 'genre:剧情', 'genre:历史', 'genre:战争', 'genre:犯罪', 'theme:whodunit', 'theme:spy', 'theme:courtroom', 'theme:slice-of-life', 'theme:wuxia', 'theme:superhero', 'theme:cyberpunk', 'theme:space-opera', 'theme:time-travel', 'theme:post-apocalyptic', 'theme:mecha', 'theme:zombie', 'theme:monster', 'theme:ghost', 'theme:magic', 'theme:gangster', 'theme:film-noir', 'theme:serial-killer', 'theme:xianxia', 'theme:kaiju', 'theme:isekai'];
const YEARS = Array.from({length: CURRENT_YEAR - 1990 + 1}, (_, i) => 1990 + i).reverse();

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
   } catch (e) { /* no cache */ }
   console.log(`  Downloading from: ${url}`);
   // 使用 node-fetch V2+ 兼容的写法
   const response = await fetch(url, { headers: { 'User-Agent': 'IMDb-Builder/1.0' } });
   if (!response.ok) throw new Error(`Failed to download ${url}: ${response.statusText}`);
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
       if (line && line.includes('\t')) processor(line);
   }
}

async function processInParallel(items, concurrency, task) {
   const queue = [...items]; let processedCount = 0; const totalCount = items.length;
   const worker = async () => {
       while (queue.length > 0) {
           const item = queue.shift();
           if (item) {
               await task(item);
               processedCount++;
               if (processedCount % 100 === 0 || processedCount === totalCount) {
                   process.stdout.write(`  Progress: ${processedCount} / ${totalCount} \r`);
               }
           }
       }
   };
   const workers = Array(concurrency).fill(null).map(() => worker());
   await Promise.all(workers);
   process.stdout.write('\n');
}

// --- 主要构建流程 ---

// --- Phase 1-3: 构建数据湖 ---
async function buildDataLake() {
    console.log('\nPHASE 1: Building indexes & Caching...');
    const ratingsIndex = new Map();
    const ratingsPath = path.join(DATASET_DIR, DATASETS.ratings.local);
    await downloadAndUnzipWithCache(DATASETS.ratings.url, ratingsPath, MAX_DOWNLOAD_AGE_MS);
    await processTsvByLine(ratingsPath, (line) => {
        const [tconst, averageRating, numVotes] = line.split('\t');
        const votes = parseInt(numVotes, 10) || 0; const rating = parseFloat(averageRating) || 0;
        if (votes >= MIN_VOTES && rating >= MIN_RATING) ratingsIndex.set(tconst, { rating, votes });
    });
    console.log(`  Ratings index built with ${ratingsIndex.size} items.`);

    console.log(`\nPHASE 2: Streaming basics & filtering...`);
    const idPool = new Set();
    const basicsPath = path.join(DATASET_DIR, DATASETS.basics.local);
    await downloadAndUnzipWithCache(DATASETS.basics.url, basicsPath, MAX_DOWNLOAD_AGE_MS);
    await processTsvByLine(basicsPath, (line) => {
        const [tconst, titleType, , , isAdult, startYear] = line.split('\t');
        const year = parseInt(startYear, 10);
        if (!tconst.startsWith('tt') || isAdult === '1' || !ALLOWED_TITLE_TYPES.has(titleType) || isNaN(year) || year < MIN_YEAR || !ratingsIndex.has(tconst)) return;
        idPool.add(tconst);
    });
    console.log(`  Filtered ID pool contains ${idPool.size} items to enrich.`);

    console.log(`\nPHASE 3: Enriching items via TMDB API...`);
    // 确保 TEMP_DIR 存在且为空
    await fs.rm(TEMP_DIR, { recursive: true, force: true }).catch(() => {}); 
    await fs.mkdir(TEMP_DIR, { recursive: true });

    const writeStream = createWriteStream(DATA_LAKE_FILE, { flags: 'a' });
    const enrichmentTask = async (id) => {
         try {
            const info = await findByImdbId(id);
            if (!info || !info.id || !info.media_type) return;
            const details = await getTmdbDetails(info.id, info.media_type);
            if (details) {
                const analyzedItem = analyzeAndTagItem(details); 
                if (analyzedItem) {
                    writeStream.write(JSON.stringify(analyzedItem) + '\n');
                }
            }
        } catch (error) {
            console.warn(`  Skipping ID ${id} due to enrichment error: ${error.message}`);
        }
    };
    await processInParallel(Array.from(idPool), MAX_CONCURRENT_ENRICHMENTS, enrichmentTask);
    await new Promise(resolve => writeStream.end(resolve));
    console.log(`  Data Lake written to ${DATA_LAKE_FILE}`);
}


// --- Phase 4: 分片、排序和分页 ---

// 精简条目 (用于最终输出)，包含所有必要字段
function minifyItem(item) {
    return {
        id: item.id,
        p: item.poster_path,       // 海报
        b: item.backdrop_path,     // 背景图
        t: item.title,             // 标题
        r: item.vote_average,      // 评分 (原始评分)
        y: item.release_year,      // 年份
        rd: item.release_date,     // <<< 完整日期
        hs: parseFloat(item.hotness_score.toFixed(3)), // 热度分
        d: parseFloat(item.default_order.toFixed(3)),  // 默认分 (popularity)
        mt: item.mediaType,        // 媒体类型: movie, tv, anime
        o: item.overview           // 简介
    };
}

// 排序、分页并写入分片
async function processAndWriteSortedPaginatedShards(basePath, data) {
    if (data.length === 0) {
        // console.log(`  Skipping empty shard: ${basePath}`);
        return;
    }
    
    const metadata = { total_items: data.length, items_per_page: ITEMS_PER_PAGE, pages: {} };

    // 遍历每种排序方式
    for (const [sortPrefix, internalKey] of Object.entries(SORT_CONFIG)) {
        // 1. 排序 (降序)
        const sortedData = [...data].sort((a, b) => (b[internalKey] || 0) - (a[internalKey] || 0));

        // 2. 分页
        const numPages = Math.ceil(sortedData.length / ITEMS_PER_PAGE);
        metadata.pages[sortPrefix] = numPages;

        // 3. 写入每个分页文件
        for (let page = 1; page <= numPages; page++) {
            const start = (page - 1) * ITEMS_PER_PAGE;
            const end = start + ITEMS_PER_PAGE;
            const pageData = sortedData.slice(start, end);
            
            // 精简数据
            const minifiedPageData = pageData.map(minifyItem);
            
            // 构造路径: dist/base/path/by_hs/page_1.json
            const finalPath = path.join(FINAL_OUTPUT_DIR, basePath, `by_${sortPrefix}`, `page_${page}.json`);
            const dir = path.dirname(finalPath);
            
            await fs.mkdir(dir, { recursive: true });
            await fs.writeFile(finalPath, JSON.stringify(minifiedPageData));
        }
    }

    // 4. 写入元数据文件: dist/base/path/meta.json
    const metaPath = path.join(FINAL_OUTPUT_DIR, basePath, 'meta.json');
    const metaDir = path.dirname(metaPath);
    await fs.mkdir(metaDir, { recursive: true });
    await fs.writeFile(metaPath, JSON.stringify(metadata));
    // console.log(`  ✅ Generated shard: ${basePath} (${data.length} items, ${Object.keys(metadata.pages).length} sorts)`);
}


async function shardDatabase() {
    console.log('\nPHASE 4: Sharding, Sorting, and Paginating database...');
    // 确保 FINAL_OUTPUT_DIR 存在且为空
    await fs.rm(FINAL_OUTPUT_DIR, { recursive: true, force: true }).catch(() => {});
    await fs.mkdir(FINAL_OUTPUT_DIR, { recursive: true });

    // --- 加载数据湖 ---
    const database = [];
    const rl = readline.createInterface({ input: createReadStream(DATA_LAKE_FILE), crlfDelay: Infinity });
    for await (const line of rl) { if (line.trim()) database.push(JSON.parse(line)); }
    console.log(`  Loaded ${database.length} items from data lake.`);

    // --- 计算分数和预处理 ---
    const validForStats = database.filter(i => i.vote_count > 100);
    const totalRating = validForStats.reduce((sum, item) => sum + (item.vote_average || 0), 0);
    const GLOBAL_AVERAGE_RATING = validForStats.length > 0 ? totalRating / validForStats.length : 6.8;
    const sortedVotes = validForStats.map(i => i.vote_count).sort((a,b) => a - b);
    const MINIMUM_VOTES_THRESHOLD = sortedVotes[Math.floor(sortedVotes.length * 0.75)] || 500;
    console.log(`  Global Stats: AvgRating=${GLOBAL_AVERAGE_RATING.toFixed(2)}, MinVotes=${MINIMUM_VOTES_THRESHOLD}`);

    database.forEach(item => {
        const pop = item.popularity || 0;
        const year = item.release_year || 1970;
        const R = item.vote_average || 0;
        const v = item.vote_count || 0;
        const yearDiff = Math.max(0, CURRENT_YEAR - year);
        // 计算贝叶斯平均分
        const bayesianRating = (v / (v + MINIMUM_VOTES_THRESHOLD)) * R + (MINIMUM_VOTES_THRESHOLD / (v + MINIMUM_VOTES_THRESHOLD)) * GLOBAL_AVERAGE_RATING;
        // 计算热度分
        item.hotness_score = Math.log10(pop + 1) * (1 / Math.sqrt(yearDiff + 2)) * bayesianRating;
        // 设置默认排序分
        item.default_order = pop; // 使用 popularity 作为默认排序
    });


    // --- 生成分片 (使用预排序和预分页) ---
    console.log('  Generating sorted and paginated shards...');
    let shardCount = 0;

    const contentTypes = ['all', 'movie', 'tv', 'anime'];
    const definedRegions = REGIONS; // 使用 build.js 中定义的 region 列表

    // 辅助函数
    const filterData = (baseData, type, region) => {
        let filtered = baseData;
        
        // 1. 按类型过滤
        if (type !== 'all') {
            filtered = filtered.filter(i => i.mediaType === type);
        }

        // 2. 按地区过滤
        if (region !== 'all') {
            filtered = filtered.filter(i => i.semantic_tags.includes(region));
        }
        return filtered;
    };

    // 1. 近期热门 (结合 Type 和 Region)
    console.log('  Processing: Recent Hot...');
    const recentHotBase = database.filter(i => i.release_year >= RECENT_YEAR_THRESHOLD);
    for (const type of contentTypes) {
        for (const region of definedRegions) {
            const data = filterData(recentHotBase, type, region);
            const pathName = `hot/${type}/${region.replace(':', '_')}`;
            await processAndWriteSortedPaginatedShards(pathName, data);
            shardCount++;
        }
    }

    // 2. 按分类/主题 (结合 Type 和 Region)
    console.log('  Processing: Categories/Themes...');
    // 添加“全部分类”
    const allCategories = ['all', ...GENRES_AND_THEMES]; 
    for (const tag of allCategories) {
        const tagBaseData = (tag === 'all') ? database : database.filter(i => i.semantic_tags.includes(tag));
        for (const type of contentTypes) {
            for (const region of definedRegions) {
                const data = filterData(tagBaseData, type, region);
                const pathName = `tag/${tag.replace(':', '_')}/${type}/${region.replace(':', '_')}`;
                await processAndWriteSortedPaginatedShards(pathName, data);
                shardCount++;
            }
        }
    }

    // 3. 按年份 (结合 Type 和 Region)
    console.log('  Processing: Years...');
    // 添加“全部年份”
    const allYears = ['all', ...YEARS]; 
    for (const year of allYears) {
        const yearBaseData = (year === 'all') ? database : database.filter(i => i.release_year === year);
        for (const type of contentTypes) {
             for (const region of definedRegions) {
                const data = filterData(yearBaseData, type, region);
                const pathName = `year/${year}/${type}/${region.replace(':', '_')}`;
                await processAndWriteSortedPaginatedShards(pathName, data);
                shardCount++;
             }
        }
    }

    // 4. 独立的电影/剧集/动画列表 (按 Region)
    // 这些是之前客户端直接调用的，现在也统一为预分页
    console.log('  Processing: Direct Types (Movies, TV, Anime)...');
    const directTypes = [
        { name: 'movies', mediaType: 'movie' },
        { name: 'tvseries', mediaType: 'tv' },
        { name: 'anime', mediaType: 'anime' },
    ];
    for (const type of directTypes) {
        let baseData = database.filter(i => i.mediaType === type.mediaType);
        for (const region of definedRegions) {
            let data = (region === 'all') ? baseData : baseData.filter(i => i.semantic_tags.includes(region));
            const pathName = `${type.name}/${region.replace(':', '_')}`;
            await processAndWriteSortedPaginatedShards(pathName, data);
            shardCount++;
        }
    }

    // --- 生成 Manifest ---
    await fs.writeFile(path.join(FINAL_OUTPUT_DIR, 'manifest.json'), JSON.stringify({
        buildTimestamp: new Date().toISOString(),
        regions: REGIONS,
        tags: GENRES_AND_THEMES,
        years: YEARS,
        itemsPerPage: ITEMS_PER_PAGE,
        sortOptions: Object.keys(SORT_CONFIG),
        contentTypes: contentTypes
    }));
    console.log(`  ✅ Sharding, Sorting, and Paginating complete. Generated ${shardCount} base shards. Files written to ${FINAL_OUTPUT_DIR}`);
}

// --- 主函数 ---
async function main() {
    console.log('Starting IMDb Sharded Build Process...');
    const startTime = Date.now();
    try {
        await buildDataLake();
        await shardDatabase();
        const duration = (Date.now() - startTime) / 1000;
        console.log(`\n✅ Build process successful! Took ${duration.toFixed(2)} seconds.`);
    } catch (error) {
        console.error('\n❌ FATAL ERROR during build process:', error);
        process.exit(1);
    } finally {
        // 清理临时文件 (开发时可以注释掉以便检查 datalake.jsonl)
        // await fs.rm(TEMP_DIR, { recursive: true, force: true }).catch(() => {}); 
        console.log('Build finished.');
    }
}

main();
