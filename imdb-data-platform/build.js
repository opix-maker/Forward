import fs from 'fs/promises';
import path from 'path';
import fetch from 'node-fetch';
import { pipeline } from 'stream/promises';
import { createWriteStream, createReadStream } from 'fs';
import zlib from 'zlib';
import readline from 'readline';
import { findByImdbId, getTmdbDetails } from './src/utils/tmdb_api.js'; 
import { analyzeAndTagItem } from './src/core/analyzer.js';

// --- 占位函数  ---
 const findByImdbId = async (id) => { console.warn("MOCK findByImdbId:", id); return id.startsWith('tt') ? {id: parseInt(id.substring(2)), media_type:'movie'}: null; }; 
 const getTmdbDetails = async (id, type) => { console.warn("MOCK getTmdbDetails:", id, type); const isTv = Math.random() > 0.7; const isAnime = Math.random() > 0.8; const tags = ['country:us']; if(isTv) tags.push('type:tv'); else tags.push('type:movie'); if(isAnime) tags.push('type:animation'); return {id, title:`Mock Title ${id}`, poster_path: '/mock.jpg', backdrop_path: '/back.jpg', overview:'Mock overview', release_year: 2020 + (id%3), vote_average: 6 + (id%40)/10, vote_count: 500 + (id%5000), popularity: 10 + (id%100), semantic_tags: tags}; }; 
 const analyzeAndTagItem = (details) => { console.warn("MOCK analyzeAndTagItem"); return details; }; 



// --- 配置 CONFIGURATION ---
const MAX_CONCURRENT_ENRICHMENTS = 60; // 并发数，根据API限制和机器性能调整
const MAX_DOWNLOAD_AGE_MS = 24 * 60 * 60 * 1000; // 缓存有效期 24 小时
const MIN_VOTES = 1500; // CCE 优化: 初始过滤票数阈值
const MIN_YEAR = 1990; 
const MIN_RATING = 6.0; 
const ALLOWED_TITLE_TYPES = new Set(['movie', 'tvSeries', 'tvMiniSeries', 'tvMovie']);
const CURRENT_YEAR = new Date().getFullYear();
const RECENT_YEAR_THRESHOLD = CURRENT_YEAR - 1; // “近期” 定义为: 去年和今年

// --- 路径 PATHS ---
const DATASET_DIR = './datasets';
const TEMP_DIR = './temp';
const FINAL_OUTPUT_DIR = './dist'; // 最终输出目录
const DATA_LAKE_FILE = path.join(TEMP_DIR, 'datalake.jsonl');

const DATASETS = {
    basics: { url: 'https://datasets.imdbws.com/title.basics.tsv.gz', local: 'title.basics.tsv' },
    ratings: { url: 'https://datasets.imdbws.com/title.ratings.tsv.gz', local: 'title.ratings.tsv' },
};

// --- 分片配置 (保持不变，确保兼容性 K2) ---
const REGIONS = ['all', 'region:chinese', 'region:us-eu', 'region:east-asia', 'country:cn', 'country:hk', 'country:tw', 'country:us', 'country:gb', 'country:jp', 'country:kr', 'country:fr', 'country:de', 'country:ca', 'country:au'];
const GENRES_AND_THEMES = ['genre:爱情', 'genre:冒险', 'genre:悬疑', 'genre:惊悚', 'genre:恐怖', 'genre:科幻', 'genre:奇幻', 'genre:动作', 'genre:喜剧', 'genre:剧情', 'genre:历史', 'genre:战争', 'genre:犯罪', 'theme:whodunit', 'theme:spy', 'theme:courtroom', 'theme:slice-of-life', 'theme:wuxia', 'theme:superhero', 'theme:cyberpunk', 'theme:space-opera', 'theme:time-travel', 'theme:post-apocalyptic', 'theme:mecha', 'theme:zombie', 'theme:monster', 'theme:ghost', 'theme:magic', 'theme:gangster', 'theme:film-noir', 'theme:serial-killer', 'theme:xianxia', 'theme:kaiju', 'theme:isekai'];
const YEARS = Array.from({length: CURRENT_YEAR - 1990 + 1}, (_, i) => 1990 + i).reverse();

// --- 辅助函数 HELPERS ---

/** 下载并解压，带缓存和错误处理 */
async function downloadAndUnzipWithCache(url, localPath, maxAgeMs) {
     const dir = path.dirname(localPath);
     await fs.mkdir(dir, { recursive: true });
    try {
        const stats = await fs.stat(localPath);
        if (Date.now() - stats.mtimeMs < maxAgeMs) {
            console.log(`  ✅ 缓存命中: ${path.basename(localPath)}`);
            return;
        }
         console.log(`  🕛 缓存过期: ${path.basename(localPath)}`);
    } catch (e) {
         if (e.code !== 'ENOENT') console.warn(`  缓存检查错误: ${e.message}`);
     }
    console.log(`  ⏳ 正在下载: ${url}`);
    const response = await fetch(url, { headers: { 'User-Agent': 'IMDb-Builder/1.1-CCE' } });
    if (!response.ok) throw new Error(`HTTP error! status: ${response.status}, URL: ${url}`);
    
    const tempPath = `${localPath}.tmp`; // 使用临时文件，下载完成后再重命名
     try {
        const gunzip = zlib.createGunzip();
        const destination = createWriteStream(tempPath);
         response.body.on('error', (err) => { throw new Error(`下载流错误: ${err}`); });
         gunzip.on('error', (err) => { throw new Error(`解压错误: ${err}`); });
         destination.on('error', (err) => {throw new Error(`写入流错误: ${err}`); });

        await pipeline(response.body, gunzip, destination);
        await fs.rename(tempPath, localPath); // 原子操作重命名
        console.log(`  ✅ 下载并解压完成: ${path.basename(localPath)}`);
     } catch(error) {
         console.error(`  ❌ 流处理失败 ${url}: ${error}`);
         await fs.unlink(tempPath).catch(() => {}); // 清理临时文件
         throw error; 
     }
}

/** 按行处理 TSV 文件 */
async function processTsvByLine(filePath, processor) {
    try {
         const fileStream = createReadStream(filePath);
          fileStream.on('error', (err) => { console.error(`读取流错误 ${filePath}:`, err); throw err;});
         const rl = readline.createInterface({ input: fileStream, crlfDelay: Infinity });
         let isFirstLine = true;
         let count = 0;
         for await (const line of rl) {
            if (isFirstLine) { isFirstLine = false; continue; }
            if (line && line.includes('\t')) {
                 processor(line);
                 count++;
            }
         }
          return count;
     } catch (err) {
         console.error(`处理 TSV 文件错误 ${filePath}:`, err);
         throw err; 
     }
}

/** 并发处理任务，带进度和错误计数 */
async function processInParallel(items, concurrency, task) {
     const queue = [...items];
     let processedCount = 0;
     let errorCount = 0;
     const totalCount = items.length;
      if (totalCount === 0) return;

    const worker = async (workerId) => {
        while (queue.length > 0) {
            const item = queue.shift(); 
            if (!item) continue;
            try {
                 await task(item);
            } catch(error) {
                 errorCount++;
                 // 记录错误但允许其他 worker 继续
                 console.warn(`  [Worker ${workerId}] 错误: ${item}: ${error.message}` );
            } finally {
                processedCount++;
                if (processedCount % 250 === 0 || processedCount === totalCount) {
                   const percent = ((processedCount / totalCount) * 100).toFixed(1);
                   process.stdout.write(`  进度: ${processedCount}/${totalCount} (${percent}%), 错误数: ${errorCount} \r`);
                }
            }
        }
    };
    const activeWorkers = Math.min(concurrency, totalCount, MAX_CONCURRENT_ENRICHMENTS);
     console.log(`  启动 ${activeWorkers} 个 worker...`);
    const workers = Array(activeWorkers).fill(null).map((_, i) => worker(i + 1));
    await Promise.all(workers);
    process.stdout.write('\n'); // 进度条后换行
     if(errorCount > 0) console.warn(`  ❗ 并发处理完成，共 ${errorCount} 个错误。`);
}

// CCE 优化 (Design Point 1): 数据完整性校验
function isValidItem(item) {
     if (!item) return false;
     // 必须包含 ID, 标题和海报路径
     const hasId = item.id !== null && item.id !== undefined;
     const hasTitle = typeof item.title === 'string' && item.title.trim().length > 0;
     // 确保海报路径存在且不为空白
     const hasPoster = typeof item.poster_path === 'string' && item.poster_path.trim().length > 0;
     return hasId && hasTitle && hasPoster;
}

// CCE 优化 (Design Point 2): 获取中位数
const getMedian = (sortedArray) => {
    if (!sortedArray || sortedArray.length === 0) return 0;
    const mid = Math.floor(sortedArray.length / 2);
    if (sortedArray.length % 2 !== 0) {
      return sortedArray[mid]; // 奇数
    } 
    return (sortedArray[mid - 1] + sortedArray[mid]) / 2; // 偶数取平均
};


// --- 主要构建流程 ---

/** 阶段 1-3: 构建数据湖 */
async function buildDataLake() {
    console.log('\nPHASE 1: 构建索引 & 缓存...');
    const ratingsIndex = new Map();
    const ratingsPath = path.join(DATASET_DIR, DATASETS.ratings.local);
    await downloadAndUnzipWithCache(DATASETS.ratings.url, ratingsPath, MAX_DOWNLOAD_AGE_MS);
    const ratingsCount = await processTsvByLine(ratingsPath, (line) => {
        const [tconst, averageRating, numVotes] = line.split('\t');
        const votes = parseInt(numVotes, 10) || 0; 
        const rating = parseFloat(averageRating) || 0.0;
        if (votes >= MIN_VOTES && rating >= MIN_RATING) {
            ratingsIndex.set(tconst, { rating, votes });
        }
    });
    console.log(`  处理 ${ratingsCount} 条评分, 索引包含 ${ratingsIndex.size} 个合格条目。`);

    console.log(`\nPHASE 2: 流式处理基础数据 & 过滤... (年份 >= ${MIN_YEAR}, 票数 >= ${MIN_VOTES}, 评分 >= ${MIN_RATING})`);
    const idPool = new Set();
    const basicsPath = path.join(DATASET_DIR, DATASETS.basics.local);
    await downloadAndUnzipWithCache(DATASETS.basics.url, basicsPath, MAX_DOWNLOAD_AGE_MS);
     let skippedAdult = 0, skippedType = 0, skippedYear = 0, skippedRating = 0;
    const basicsCount = await processTsvByLine(basicsPath, (line) => {
        const [tconst, titleType, , , isAdult, startYear] = line.split('\t');
         if (!tconst || !tconst.startsWith('tt')) return;
         if (isAdult === '1') { skippedAdult++; return; }
         if (!ALLOWED_TITLE_TYPES.has(titleType)) { skippedType++; return; }
        const year = parseInt(startYear, 10);
        if (isNaN(year) || year < MIN_YEAR) { skippedYear++; return; }
         if (!ratingsIndex.has(tconst)) { skippedRating++; return; } // 必须存在于合格评分索引中
        idPool.add(tconst);
    });
     console.log(`  处理 ${basicsCount} 条基础数据。`);
     console.log(`  跳过: 成人内容(${skippedAdult}), 类型(${skippedType}), 年份(${skippedYear}), 评分/票数(${skippedRating})`);
    console.log(`  ✅ 过滤后的 ID 池: ${idPool.size} 个条目待富集。`);

    console.log(`\nPHASE 3: 通过 TMDB API 富集数据 & 构建数据湖...`);
    await fs.rm(TEMP_DIR, { recursive: true, force: true }); 
    await fs.mkdir(TEMP_DIR, { recursive: true });
    const writeStream = createWriteStream(DATA_LAKE_FILE, { flags: 'w' }); // 覆盖写入
     let validItemsCount = 0;

    const enrichmentTask = async (id) => {
         try {
            const info = await findByImdbId(id); // IMDb ID -> TMDB ID & type
            if (!info || !info.id || !info.media_type) return;

            const details = await getTmdbDetails(info.id, info.media_type); // 获取 TMDB 详情
            if (!details) return; 

            const analyzedItem = analyzeAndTagItem(details); // 分析并打标签 (semantic_tags)
            
             // CCE 优化 (Design Point 1): 写入前校验数据完整性
            if (isValidItem(analyzedItem)) {
                 writeStream.write(JSON.stringify(analyzedItem) + '\n');
                 validItemsCount++;
            } 
             // else { console.log(`  跳过无效条目 ${id}`); }

        } catch (error) {
             // 重新抛出，由 processInParallel 捕获
            throw new Error(`富集 ID ${id} 错误: ${error.message}`);
        }
    };
    
    await processInParallel(Array.from(idPool), MAX_CONCURRENT_ENRICHMENTS, enrichmentTask);
    await new Promise(resolve => writeStream.end(resolve)); // 确保流完全写入并关闭
    console.log(`  ✅ 数据湖构建完成: ${validItemsCount} 个有效条目写入 ${DATA_LAKE_FILE}`);
}


/** 阶段 4: 评分与分片 */
async function shardDatabase() {
    console.log('\nPHASE 4: 计算评分并分片...');
    await fs.rm(FINAL_OUTPUT_DIR, { recursive: true, force: true });
    await fs.mkdir(FINAL_OUTPUT_DIR, { recursive: true });

    const database = [];
     try {
         const fileStream = createReadStream(DATA_LAKE_FILE);
          fileStream.on('error', (err) => { throw err; });
         const rl = readline.createInterface({ input: fileStream, crlfDelay: Infinity });
         for await (const line of rl) { 
             if (line.trim()) {
                 try {
                      database.push(JSON.parse(line)); 
                 } catch(parseErr) {
                      console.warn(`  跳过错误的 JSON 行: ${parseErr.message}`);
                 }
             }
          }
      } catch (err) {
          console.error(`  ❌ 读取数据湖文件错误 ${DATA_LAKE_FILE}:`, err);
          throw err; 
      }
     if (database.length === 0) {
          console.warn("  ❗ 数据湖为空，跳过分片。");
          return;
     }
    console.log(`  从数据湖加载 ${database.length} 个条目。`);

    // --- 计算分数和预处理 ---
    console.log('  正在计算分数...');
    const validForStats = database.filter(i => i.vote_count > 200); // 使用有一定票数的条目计算统计值
     if(validForStats.length === 0) {
          console.warn("  ❗ 没有足够的带票数条目来计算统计数据。");
           return;
      }
    const totalRating = validForStats.reduce((sum, item) => sum + (item.vote_average || 0), 0);
    const GLOBAL_AVERAGE_RATING = totalRating / validForStats.length; // C (全局平均分)

    const sortedVotes = validForStats.map(i => i.vote_count).sort((a,b) => a - b);
     // CCE 优化 (Design Point 2): 使用中位数 (50th percentile) 作为贝叶斯阈值 m
    const MINIMUM_VOTES_THRESHOLD = getMedian(sortedVotes) || 500; // m (置信阈值)
    
    console.log(`  📊 全局统计: 平均分(C)=${GLOBAL_AVERAGE_RATING.toFixed(2)}, 中位票数(m)=${MINIMUM_VOTES_THRESHOLD.toFixed(0)}`);

    database.forEach(item => {
        const pop = item.popularity || 0;
        const year = item.release_year || 1970;
        const R = item.vote_average || 0; // 条目评分
        const v = item.vote_count || 0;   // 条目票数
        const C = GLOBAL_AVERAGE_RATING;
        const m = MINIMUM_VOTES_THRESHOLD;
         
        const yearDiff = Math.max(0, CURRENT_YEAR - year);
        const bayesianRating = (v + m > 0) 
             ? (v / (v + m)) * R + (m / (v + m)) * C
             : C; // 票数过低则默认全局平均分
        
        // 热度分 (hs): log(pop) * time_decay * quality_score
         item.hotness_score = Math.log1p(pop) * (1 / Math.sqrt(yearDiff + 1.5)) * bayesianRating; 

         // CCE 优化 (Design Point 2): 默认排序 (d) 改为贝叶斯评分 (稳定质量分)
        item.default_order = bayesianRating;
        
        // CCE 优化 (Design Point 3): 类型逻辑预处理
        const tags = item.semantic_tags || [];
        const isAnimation = tags.includes('type:animation');
        const isTV = tags.includes('type:tv');

         // 字段 'mt': 保持与 widget 兼容 ('movie' 或 'tv')
        item.mediaType = isTV ? 'tv' : 'movie'; 
         
         // 内部字段 '_shardType': 定义互斥分类，用于分片。优先级: 动画 > 剧集 > 电影
         if (isAnimation) {
             item._shardType = 'anime';
         } else if (isTV) {
              item._shardType = 'tvseries';
         } else {
              item._shardType = 'movies'; 
         }
    });
     console.log('  ✅ 分数计算和类型分配完成。');

    // --- 写入分片辅助函数 ---
    const writeShard = async (filePath, data) => {
        if (!Array.isArray(data) || data.length === 0) return; // 不创建空文件
        
        const fullPath = path.join(FINAL_OUTPUT_DIR, filePath);
        const dir = path.dirname(fullPath);
        await fs.mkdir(dir, { recursive: true });
        
         // CCE: 保持与 Widget 严格兼容的结构 (K1, Ψ2)
         // 注意: 不包含 _shardType，但必须包含 mt (即 item.mediaType)
        const minifiedData = data.map(item => ({
             id: item.id,                       // TMDB ID
             p: item.poster_path,               // 海报
             b: item.backdrop_path,             // 背景图
             t: item.title,                     // 标题
             r: item.vote_average,              // 原始评分
             y: item.release_year,              // 年份
             hs: parseFloat(item.hotness_score.toFixed(4)), // 热度分
             d: parseFloat(item.default_order.toFixed(3)),  // 默认排序 (现为贝叶斯评分)
             mt: item.mediaType,                // 媒体类型 ('movie' or 'tv') - Widget 必需
             o: item.overview || ''             // 简介
        }));
        
        await fs.writeFile(fullPath, JSON.stringify(minifiedData));
    };

    // --- 生成分片 ---
    console.log('  ⏳ 正在生成分片...');
    // 1. 近期热门
    await writeShard('recent_hot.json', database.filter(i => i.release_year >= RECENT_YEAR_THRESHOLD));
    
    // 2. 按类型/主题 (Tag)
     await fs.mkdir(path.join(FINAL_OUTPUT_DIR, 'by_tag'), { recursive: true });
    for (const tag of GENRES_AND_THEMES) {
        const data = database.filter(i => i.semantic_tags && i.semantic_tags.includes(tag));
         if(data.length > 0) {
            const filename = tag.replace(':', '_') + '.json';
            await writeShard(path.join('by_tag', filename), data);
         }
    }
    
    // 3. 按年份
     await fs.mkdir(path.join(FINAL_OUTPUT_DIR, 'by_year'), { recursive: true });
    for (const year of YEARS) {
         const data = database.filter(i => i.release_year === year);
         if(data.length > 0) {
            await writeShard(path.join('by_year', `${year}.json`), data);
         }
    }
    
     // 4. 按电影/剧集/动画 + 地区 
     // CCE 优化 (Design Point 3): 使用 _shardType 创建互斥集合
    const types = [
        { name: 'movies',   filter: (i) => i._shardType === 'movies' },
        { name: 'tvseries', filter: (i) => i._shardType === 'tvseries'},
        { name: 'anime',    filter: (i) => i._shardType === 'anime' },
    ];

    for (const type of types) {
        let baseData = database.filter(type.filter); // 获取该类型的互斥基础数据集
         if(baseData.length === 0) continue;
         await fs.mkdir(path.join(FINAL_OUTPUT_DIR, type.name), { recursive: true });
        for (const region of REGIONS) {
            let regionData = (region === 'all') 
                ? baseData 
                : baseData.filter(i => i.semantic_tags && i.semantic_tags.includes(region));
             if(regionData.length > 0) {
               const filename = region.replace(':', '_') + '.json';
               await writeShard(path.join(type.name, filename), regionData);
             }
        }
    }
    
    // Manifest 文件
    await fs.writeFile(path.join(FINAL_OUTPUT_DIR, 'manifest.json'), JSON.stringify({
        buildTimestamp: new Date().toISOString(),
        itemCount: database.length,
        stats: { C: GLOBAL_AVERAGE_RATING, m: MINIMUM_VOTES_THRESHOLD },
        config: { MIN_VOTES, MIN_YEAR, MIN_RATING, RECENT_YEAR_THRESHOLD },
        regions: REGIONS,
        tags: GENRES_AND_THEMES,
        years: YEARS,
    }, null, 2)); 

    console.log(`  ✅ 分片完成。文件已写入 ${FINAL_OUTPUT_DIR}`);
}

/** 主函数 */
async function main() {
    console.log('🚀 启动 IMDb 分片构建流程 (CCE 优化版)...');
    const startTime = Date.now();
     let tempDirCleaned = false;
    try {
        await buildDataLake();
        await shardDatabase();
        const duration = (Date.now() - startTime) / 1000;
        console.log(`\n🎉 构建成功! 耗时 ${duration.toFixed(2)} 秒。`);
         console.log('  🧹 清理临时目录...');
         await fs.rm(TEMP_DIR, { recursive: true, force: true });
          tempDirCleaned = true;

    } catch (error) {
        console.error('\n❌ 构建过程发生致命错误:', error.stack || error);
         console.log('  ❗ 临时目录已保留，用于调试:', TEMP_DIR);
        process.exit(1);
    } finally {
         // 确保清理
         if (!tempDirCleaned) {
           // 可选: await fs.rm(TEMP_DIR, { recursive: true, force: true }).catch(e => console.error("清理失败:", e));
         }
    }
}

// 执行主函数
main();
