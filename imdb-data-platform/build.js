import fs from 'fs/promises';
import { fetchImdbIdList } from './src/builders/imdb_builder.js';
import { findByImdbId, getTmdbDetails } from './src/utils/tmdb_api.js';
import { analyzeAndTagItem } from './src/core/analyzer.js';
import { sleep, writeJsonFile } from './src/utils/helpers.js';

const MAX_CONCURRENT_ENRICHMENTS = 15;
const OUTPUT_DIR = './dist';

// --- 任务定义区 (保持不变) ---
const BUILD_MATRIX = {
    official_popular_movies: { name: 'IMDb 热门电影', crawlParams: { title_type: 'feature', sort: 'moviemeter,asc' } },
    official_top_movies: { name: 'IMDb Top 250 电影', crawlParams: { title_type: 'feature', sort: 'user_rating,desc', num_votes: '25000,' } },
    official_popular_tv: { name: 'IMDb 热门剧集', crawlParams: { title_type: 'tv_series', sort: 'moviemeter,asc' } },
    official_top_tv: { name: 'IMDb Top 250 剧集', crawlParams: { title_type: 'tv_series', sort: 'user_rating,desc', num_votes: '10000,' } },
    
    jp_anime_top: { name: '日本高分动画', tags: ['lang:ja', 'genre:动画'], sortBy: 'vote_average' },
    jp_anime_popular: { name: '日本热门动画', tags: ['lang:ja', 'genre:动画'], sortBy: 'vote_count' },
    kr_tv_top: { name: '韩国高分剧集', tags: ['lang:ko', 'type:tv'], sortBy: 'vote_average' },
    kr_tv_popular: { name: '韩国热门剧集', tags: ['lang:ko', 'type:tv'], sortBy: 'vote_count' },
    us_tv_top: { name: '美国高分剧集', tags: ['lang:en', 'country:us', 'type:tv'], sortBy: 'vote_average' },
    us_tv_popular: { name: '美国热门剧集', tags: ['lang:en', 'country:us', 'type:tv'], sortBy: 'vote_count' },
    cn_tv_top: { name: '国产高分剧集', tags: ['lang:chinese', 'country:cn', 'type:tv'], sortBy: 'vote_average' },
    cn_anime_top: { name: '国产高分动画', tags: ['lang:chinese', 'country:cn', 'genre:动画'], sortBy: 'vote_average' },
    
    theme_cyberpunk: { name: '赛博朋克精选', tags: ['theme:cyberpunk'], sortBy: 'vote_average' },
    theme_zombie: { name: '僵尸末日', tags: ['theme:zombie'], sortBy: 'vote_average' },
    theme_wuxia: { name: '武侠世界', tags: ['theme:wuxia', 'lang:chinese'], sortBy: 'vote_average' },
    theme_film_noir: { name: '黑色电影', tags: ['theme:film-noir'], sortBy: 'vote_average' },
    theme_space_opera: { name: '太空歌剧', tags: ['theme:space-opera'], sortBy: 'vote_average' },
    theme_time_travel: { name: '时间旅行', tags: ['theme:time-travel'], sortBy: 'vote_average' },
    theme_adult: { name: '成人内容精选 (R/NC-17)', tags: ['theme:adult'], sortBy: 'vote_average' },

    series_collection: { name: '系列电影宇宙', type: 'series' }
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
    console.log('Starting intelligent build process v2.1 (Data Structure Fix)...');
    const startTime = Date.now();
    const allEnrichedItems = new Map();

    try {
        // --- 阶段一 & 二: 抓取、增强、打标 ---
        console.log('\nPHASE 1 & 2: Crawling, Enriching, and Analyzing...');
        const uniqueImdbIds = new Set();
        const crawlTasks = Object.values(BUILD_MATRIX).filter(config => config.crawlParams);
        for (const config of crawlTasks) {
            const ids = await fetchImdbIdList(config.crawlParams);
            ids.forEach(id => uniqueImdbIds.add(id));
        }
        console.log(`  Collected ${uniqueImdbIds.size} unique IMDb IDs to process.`);

        const imdbIdList = Array.from(uniqueImdbIds);
        const enrichedList = await enrichListWithTmdb(imdbIdList);
        enrichedList.forEach(item => allEnrichedItems.set(item.id, item));
        console.log(`  Total unique items analyzed: ${allEnrichedItems.size}`);

        // --- 阶段三: 生成最终榜单 ---
        console.log('\nPHASE 3: Generating final lists from analyzed data...');
        const analyzedData = Array.from(allEnrichedItems.values());
        const seriesCollection = new Map();

        for (const [key, config] of Object.entries(BUILD_MATRIX)) {
            if (config.type === 'series') {
                analyzedData.forEach(item => {
                    if (item.belongs_to_collection) {
                        const seriesId = item.belongs_to_collection.id;
                        if (!seriesCollection.has(seriesId)) {
                            seriesCollection.set(seriesId, { name: item.belongs_to_collection.name, items: [] });
                        }
                        seriesCollection.get(seriesId).items.push(item);
                    }
                });
                for (const [seriesId, seriesData] of seriesCollection.entries()) {
                    if (seriesData.items.length < 2) continue;
                    seriesData.items.sort((a, b) => new Date(a.release_date) - new Date(b.release_date));
                    // ===================================================================
                    //  核心修复：直接写入数组，而不是包装在对象里
                    // ===================================================================
                    await writeJsonFile(`${OUTPUT_DIR}/series_${seriesId}.json`, seriesData.items);
                }
                continue;
            }

            let listData = analyzedData;
            if (config.tags) {
                listData = analyzedData.filter(item => config.tags.every(tag => item.semantic_tags.includes(tag)));
            }
            
            if (config.sortBy) {
                listData.sort((a, b) => (b[config.sortBy] || 0) - (a[config.sortBy] || 0));
            }
            
            const final_list = listData.slice(0, 50);
            await writeJsonFile(`${OUTPUT_DIR}/${key}.json`, final_list);
            console.log(`  Generated list: ${config.name} -> ${key}.json (${final_list.length} items written)`);
        }

        const index = {
            lists: Object.fromEntries(Object.entries(BUILD_MATRIX).map(([key, val]) => [key, {name: val.name}])),
            series: Array.from(seriesCollection.entries()).filter(([, data]) => data.items.length >= 2).map(([id, data]) => ({ id: `series_${id}`, name: data.name })),
            buildTimestamp: new Date().toISOString()
        };
        await writeJsonFile(`${OUTPUT_DIR}/index.json`, index);
        console.log(`\nSuccessfully wrote index file.`);

        const duration = (Date.now() - startTime) / 1000;
        console.log(`\n✅ Build process successful! Took ${duration.toFixed(2)} seconds.`);

    } catch (error) {
        console.error('\n❌ FATAL ERROR during build process:', error);
        process.exit(1);
    }
}

main();
