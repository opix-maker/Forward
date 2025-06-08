import fs from 'fs/promises';
import path from 'path';
import fetch from 'node-fetch';
import { pipeline } from 'stream/promises';
import { createWriteStream, createReadStream } from 'fs';
import zlib from 'zlib';
import readline from 'readline';
import { findByImdbId, getTmdbDetails } from './src/utils/tmdb_api.js';
import { analyzeAndTagItem } from './src/core/analyzer.js';

const DATASET_DIR = './datasets';
const OUTPUT_FILE = './dist/database.json';
const MAX_ITEMS_PER_CRAWL = 500;
const MAX_CONCURRENT_ENRICHMENTS = 20;

const DATASETS = {
    basics: { url: 'https://datasets.imdbws.com/title.basics.tsv.gz', local: 'title.basics.tsv' },
    ratings: { url: 'https://datasets.imdbws.com/title.ratings.tsv.gz', local: 'title.ratings.tsv' },
};

// --- 抓取任务矩阵：为数据湖提供丰富、多样的源头 ---
const CRAWL_MATRIX = [
    // 核心榜单
    { path: '/chart/moviemeter/' }, { path: '/chart/top/' }, { path: '/chart/tvmeter/' }, { path: '/chart/toptv/' },
    // 亚洲地区专项抓取 (确保数据多样性)
    { params: { countries: 'jp', title_type: 'feature,tv_series', sort: 'user_rating,desc' } },
    { params: { countries: 'kr', title_type: 'feature,tv_series', sort: 'user_rating,desc' } },
    { params: { countries: 'cn,hk,tw', title_type: 'feature,tv_series', sort: 'user_rating,desc' } },
    { params: { countries: 'in', title_type: 'feature', sort: 'user_rating,desc' } },
    // 核心类型专项抓取
    { params: { genres: 'sci-fi', sort: 'user_rating,desc' } }, { params: { genres: 'horror', sort: 'user_rating,desc' } },
    { params: { genres: 'animation', sort: 'user_rating,desc' } }, { params: { genres: 'comedy', sort: 'user_rating,desc' } },
    { params: { genres: 'action', sort: 'user_rating,desc' } }, { params: { genres: 'documentary', sort: 'user_rating,desc' } },
];

async function downloadAndUnzip(url, localPath) {
    const dir = path.dirname(localPath);
    await fs.mkdir(dir, { recursive: true });
    console.log(`  Downloading from official URL: ${url}`);
    const response = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36' } });
    if (!response.ok) {
        const errorBody = await response.text();
        throw new Error(`Failed to download ${url} - Status: ${response.status} ${response.statusText}. Body: ${errorBody}`);
    }
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
        processor(line);
    }
}

async function buildDataLake() {
    console.log('\nPHASE 1: Building Data Lake...');
    const ratingsIndex = new Map();
    const basicsData = new Map();

    // 1. 构建轻量级评分索引
    const ratingsPath = path.join(DATASET_DIR, DATASETS.ratings.local);
    try {
        await fs.access(ratingsPath);
    } catch {
        await downloadAndUnzip(DATASETS.ratings.url, ratingsPath);
    }
    await processTsvByLine(ratingsPath, (line) => {
        const [tconst, averageRating, numVotes] = line.split('\t');
        ratingsIndex.set(tconst, { rating: parseFloat(averageRating) || 0, votes: parseInt(numVotes, 10) || 0 });
    });
    console.log(`  Ratings index built with ${ratingsIndex.size} entries.`);

    // 2. 流式处理basics文件，并与评分数据合并
    const basicsPath = path.join(DATASET_DIR, DATASETS.basics.local);
    try {
        await fs.access(basicsPath);
    } catch {
        await downloadAndUnzip(DATASETS.basics.url, basicsPath);
    }
    await processTsvByLine(basicsPath, (line) => {
        const [tconst, titleType, primaryTitle, , isAdult, startYear, , genres] = line.split('\t');
        if (!tconst.startsWith('tt') || isAdult === '1') return;
        const ratingInfo = ratingsIndex.get(tconst) || { rating: 0, votes: 0 };
        basicsData.set(tconst, { id: tconst, type: titleType.toLowerCase(), title: primaryTitle, year: parseInt(startYear, 10) || null, genres: genres ? genres.split(',') : [], ...ratingInfo });
    });
    console.log(`  Basics data processed with ${basicsData.size} entries.`);
    
    // 3. 抓取IMDb页面，获取ID列表
    const finalIdPool = new Set();
    console.log('\nPHASE 2: Crawling IMDb pages for ID pool...');
    for (const task of CRAWL_MATRIX) {
        const url = task.path ? `${IMDB_BASE_URL}${task.path}` : `https://www.imdb.com/search/title/?${new URLSearchParams(task.params)}`;
        console.log(`  Crawling: ${url}`);
        const html = await fetch(url).then(res => res.text());
        const $ = cheerio.load(html);
        $('li.ipc-metadata-list-summary-item a.ipc-title-link-wrapper').each((_, el) => {
            const imdbId = $(el).attr('href')?.match(/tt\d+/)?.[0];
            if (imdbId) finalIdPool.add(imdbId);
        });
    }
    console.log(`  Crawl complete. Final ID pool size: ${finalIdPool.size}`);
    
    // 4. 丰富TMDB数据并进行语义分析
    console.log('\nPHASE 3: Enriching with TMDB data...');
    const dataLake = [];
    const imdbIdArray = Array.from(finalIdPool);
    for (let i = 0; i < imdbIdArray.length; i += MAX_CONCURRENT_ENRICHMENTS) {
        const batch = imdbIdArray.slice(i, i + MAX_CONCURRENT_ENRICHMENTS);
        const promises = batch.map(id => findByImdbId(id).then(info => info ? getTmdbDetails(info.id, info.media_type) : null).then(details => analyzeAndTagItem(details)));
        const results = await Promise.allSettled(promises);
        results.forEach(r => {
            if (r.status === 'fulfilled' && r.value) {
                // 将IMDb数据库中的评分和投票数补充到最终结果中
                const imdbData = basicsData.get(r.value.imdb_id);
                if (imdbData) {
                    r.value.imdb_rating = imdbData.rating;
                    r.value.imdb_votes = imdbData.votes;
                }
                dataLake.push(r.value);
            }
        });
        console.log(`  Enriched batch ${i / MAX_CONCURRENT_ENRICHMENTS + 1} / ${Math.ceil(imdbIdArray.length / MAX_CONCURRENT_ENRICHMENTS)}`);
    }
    
    console.log(`Data Lake built with ${dataLake.length} fully analyzed items.`);
    return dataLake;
}

function deriveListsFromDataLake(dataLake) {
    console.log('\nPHASE 4: Deriving lists from Data Lake...');
    const derivedLists = {};
    const now = new Date();
    const sixWeeksAgo = new Date(now.getTime() - 6 * 7 * 24 * 60 * 60 * 1000);
    const fourWeeksLater = new Date(now.getTime() + 4 * 7 * 24 * 60 * 60 * 1000);
    const sixMonthsAgo = new Date(now.getTime() - 180 * 24 * 60 * 60 * 1000);

    // 院线热映
    derivedLists.inTheaters = dataLake.filter(item => {
        const releaseDate = new Date(item.release_date);
        return item.semantic_tags.includes('type:movie') && releaseDate >= sixWeeksAgo && releaseDate <= fourWeeksLater;
    }).sort((a, b) => b.popularity - a.popularity).slice(0, 50);

    // 本周热门
    derivedLists.weeklyTrending = [...dataLake].sort((a, b) => b.popularity - a.popularity).slice(0, 50);

    // 年度口碑榜
    const currentYear = now.getFullYear();
    derivedLists[`annualBest_${currentYear}`] = dataLake.filter(item => item.release_year === currentYear && item.vote_count > 1000).sort((a, b) => b.vote_average - a.vote_average).slice(0, 50);
    derivedLists[`annualBest_${currentYear - 1}`] = dataLake.filter(item => item.release_year === (currentYear - 1) && item.vote_count > 1000).sort((a, b) => b.vote_average - a.vote_average).slice(0, 50);

    // 近期流行
    derivedLists.recentlyPopular = dataLake.filter(item => new Date(item.release_date) >= sixMonthsAgo).sort((a, b) => b.popularity - a.popularity).slice(0, 100);

    // 主题探索
    derivedLists.theme_mindBenders = dataLake.filter(item => item.semantic_tags.includes('style:plot-twist')).sort((a, b) => b.vote_average - a.vote_average).slice(0, 50);
    derivedLists.theme_hiddenGems = dataLake.filter(item => item.vote_average > 8.0 && item.vote_count > 5000 && item.vote_count < 25000).sort((a, b) => b.vote_average - a.vote_average).slice(0, 50);

    // 系列宇宙
    const collections = new Map();
    dataLake.forEach(item => {
        if (item.belongs_to_collection) {
            const id = item.belongs_to_collection.id;
            if (!collections.has(id)) collections.set(id, { name: item.belongs_to_collection.name, items: [] });
            collections.get(id).items.push(item);
        }
    });
    derivedLists.seriesUniverse = Array.from(collections.values()).filter(c => c.items.length > 1).map(c => {
        c.items.sort((a, b) => new Date(a.release_date) - new Date(b.release_date));
        return c;
    });

    console.log('Finished deriving lists.');
    return derivedLists;
}

async function main() {
    console.log('Starting IMDb Discovery Engine build process v5.1 (Final)...');
    const startTime = Date.now();
    try {
        await fs.mkdir(DATASET_DIR, { recursive: true });
        const dataLake = await buildDataLake();
        const derivedLists = deriveListsFromDataLake(dataLake);

        console.log('\nPHASE 5: Writing final database file...');
        const output = {
            buildTimestamp: new Date().toISOString(),
            ...derivedLists,
            fullDatabase: dataLake,
        };
        await fs.rm(OUTPUT_DIR, { recursive: true, force: true });
        await fs.mkdir(OUTPUT_DIR, { recursive: true });
        await fs.writeFile(OUTPUT_FILE, JSON.stringify(output));

        const duration = (Date.now() - startTime) / 1000;
        console.log(`\n✅ Build process successful! Took ${duration.toFixed(2)} seconds.`);
    } catch (error) {
        console.error('\n❌ FATAL ERROR during build process:', error);
        process.exit(1);
    }
}

main();
