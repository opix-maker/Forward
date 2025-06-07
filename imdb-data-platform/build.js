import fs from 'fs/promises';
import { fetchImdbIdList } from './src/builders/imdb_builder.js';
import { findByImdbId, getTmdbDetails } from './src/utils/tmdb_api.js';
import { analyzeAndTagItem } from './src/core/analyzer.js';
import { sleep, writeJsonFile, ensureDir } from './src/utils/helpers.js';

const MAX_CONCURRENT_ENRICHMENTS = 15;
const OUTPUT_DIR = './dist';

// --- 任务定义区 (保持不变) ---
const BUILD_MATRIX = {
    official_popular_movies: { name: 'IMDb 热门电影', crawlParams: { title_type: 'feature', sort: 'moviemeter,asc' } },
    official_top_movies: { name: 'IMDb Top 250 电影', crawlParams: { title_type: 'feature', sort: 'user_rating,desc', num_votes: '25000,' } },
    official_popular_tv: { name: 'IMDb 热门剧集', crawlParams: { title_type: 'tv_series', sort: 'moviemeter,asc' } },
    official_top_tv: { name: 'IMDb Top 250 剧集', crawlParams: { title_type: 'tv_series', sort: 'user_rating,desc', num_votes: '10000,' } },
    jp_anime_top: { name: '日本高分动画', tags: ['lang:ja', 'genre:动画'], sortBy: 'vote_average', sourcePool: true },
    jp_anime_popular: { name: '日本热门动画', tags: ['lang:ja', 'genre:动画'], sortBy: 'vote_count', sourcePool: true },
    kr_tv_top: { name: '韩国高分剧集', tags: ['lang:ko', 'type:tv'], sortBy: 'vote_average', sourcePool: true },
    kr_tv_popular: { name: '韩国热门剧集', tags: ['lang:ko', 'type:tv'], sortBy: 'vote_count', sourcePool: true },
    us_tv_top: { name: '美国高分剧集', tags: ['lang:en', 'country:us', 'type:tv'], sortBy: 'vote_average', sourcePool: true },
    cn_tv_top: { name: '国产高分剧集', tags: ['lang:chinese', 'country:cn', 'type:tv'], sortBy: 'vote_average', sourcePool: true },
    cn_anime_top: { name: '国产高分动画', tags: ['lang:chinese', 'country:cn', 'genre:动画'], sortBy: 'vote_average', sourcePool: true },
    theme_cyberpunk: { name: '赛博朋克精选', tags: ['theme:cyberpunk'], sortBy: 'vote_average', sourcePool: true },
    theme_zombie: { name: '僵尸末日', tags: ['theme:zombie'], sortBy: 'vote_average', sourcePool: true },
    theme_wuxia: { name: '武侠世界', tags: ['theme:wuxia', 'lang:chinese'], sortBy: 'vote_average', sourcePool: true },
    theme_adult: { name: '成人内容精选', tags: ['theme:adult'], sortBy: 'vote_average', sourcePool: true },
    series_collection: { name: '系列电影宇宙', type: 'series', sourcePool: true }
};

// --- 构建流程 ---

async function enrichListWithTmdb(imdbIdList) {
    const enrichedItems = [];
    for (let i = 0; i < imdbIdList.length; i += MAX_CONCURRENT_ENRICHMENTS) {
        const batch = imdbIdList.slice(i, i + MAX_CONCURRENT_ENRICHMENTS);
        const promises = batch.map(async (imdbId) => {
            if (!imdbId) return null;
            const tmdbInfo = await findByImdbId(imdbId);
            if (!tmdbInfo) return null;
            const details = await getTmdbDetails(tmdbInfo.id, tmdbInfo.media_type);
            if (!details) return null;
            return analyzeAndTagItem(details);
        });
        const settledResults = await Promise.allSettled(promises);
        settledResults.forEach(result => {
            if (result.status === 'fulfilled' && result.value) enrichedItems.push(result.value);
        });
        await sleep(500);
    }
    return enrichedItems;
}

async function main() {
    console.log('Starting intelligent build process v4 (Cache Busting)...');
    const startTime = Date.now();
    const buildId = startTime; // 使用时间戳作为本次构建的唯一ID
    const dataPool = new Map();

    try {
        // 清理旧的构建产物
        await fs.rm(OUTPUT_DIR, { recursive: true, force: true });
        await ensureDir(OUTPUT_DIR);
        console.log('Cleaned up old dist directory.');

        // --- 阶段一 & 二: 构建数据池 ---
        console.log('\nPHASE 1 & 2: Building a common data pool...');
        // ... (这部分逻辑保持不变)
        const poolCrawlTasks = [
            { title_type: 'feature,tv_series,tv_miniseries', sort: 'moviemeter,asc', count: 250 },
            { title_type: 'feature,tv_series,tv_miniseries', sort: 'user_rating,desc', num_votes: '10000,', count: 250 },
        ];
        const uniqueImdbIdsForPool = new Set();
        for (const params of poolCrawlTasks) {
            const ids = await fetchImdbIdList(params);
            ids.forEach(id => uniqueImdbIdsForPool.add(id));
        }
        const poolItems = await enrichListWithTmdb(Array.from(uniqueImdbIdsForPool));
        poolItems.forEach(item => dataPool.set(item.id, item));
        console.log(`  Data pool built with ${dataPool.size} unique items.`);

        // --- 阶段三: 生成带唯一ID的榜单文件 ---
        console.log('\nPHASE 3: Generating final lists with build ID...');
        const seriesCollection = new Map();
        const index = {
            buildId: buildId, // **核心变更**
            lists: BUILD_MATRIX,
            series: [],
            buildTimestamp: new Date(buildId).toISOString()
        };

        for (const [key, config] of Object.entries(BUILD_MATRIX)) {
            let final_list = [];

            if (config.sourcePool) {
                let listData = Array.from(dataPool.values());
                if (config.type === 'series') {
                    listData.forEach(item => {
                        if (item.belongs_to_collection) {
                            const seriesId = item.belongs_to_collection.id;
                            if (!seriesCollection.has(seriesId)) {
                                seriesCollection.set(seriesId, { name: item.belongs_to_collection.name, items: [] });
                            }
                            seriesCollection.get(seriesId).items.push(item);
                        }
                    });
                    continue;
                }
                if (config.tags) {
                    listData = listData.filter(item => config.tags.every(tag => item.semantic_tags.includes(tag)));
                }
                if (config.sortBy) {
                    listData.sort((a, b) => (b[config.sortBy] || 0) - (a[config.sortBy] || 0));
                }
                final_list = listData.slice(0, 50);
            } else if (config.crawlParams) {
                const imdbIdList = await fetchImdbIdList(config.crawlParams);
                final_list = await enrichListWithTmdb(imdbIdList);
            }
            
            // **核心变更**: 使用带buildId的文件名
            const fileName = `${key}.${buildId}.json`;
            await writeJsonFile(`${OUTPUT_DIR}/${fileName}`, final_list);
            console.log(`  Generated list: ${config.name} -> ${fileName} (${final_list.length} items)`);
        }
        
        // 处理系列电影
        for (const [seriesId, seriesData] of seriesCollection.entries()) {
            if (seriesData.items.length < 2) continue;
            seriesData.items.sort((a, b) => new Date(a.release_date) - new Date(b.release_date));
            const seriesKey = `series_${seriesId}`;
            const fileName = `${seriesKey}.${buildId}.json`;
            await writeJsonFile(`${OUTPUT_DIR}/${fileName}`, seriesData.items);
            index.series.push({ id: seriesKey, name: seriesData.name });
        }

        // 最后写入索引文件
        await writeJsonFile(`${OUTPUT_DIR}/index.json`, index);
        console.log(`\nSuccessfully wrote index file with buildId: ${buildId}`);

        const duration = (Date.now() - startTime) / 1000;
        console.log(`\n✅ Build process successful! Took ${duration.toFixed(2)} seconds.`);

    } catch (error) {
        console.error('\n❌ FATAL ERROR during build process:', error);
        process.exit(1);
    }
}

main();
