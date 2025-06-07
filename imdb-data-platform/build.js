import fs from 'fs/promises';
import { fetchImdbIdList } from './src/builders/imdb_builder.js';
import { findByImdbId, getTmdbDetails } from './src/utils/tmdb_api.js';
import { analyzeAndTagItem } from './src/core/analyzer.js';
import { sleep, writeJsonFile } from './src/utils/helpers.js';

const MAX_CONCURRENT_ENRICHMENTS = 15;
const OUTPUT_DIR = './dist';

// --- 任务定义区 ---
// 定义基础的、广撒网的抓取任务
const CRAWL_TASKS = [
    { title_type: 'feature,tv_series,tv_miniseries', sort: 'moviemeter,asc', count: 250 }, // 热门
    { title_type: 'feature,tv_series,tv_miniseries', sort: 'user_rating,desc', num_votes: '25000,', count: 250 }, // 高分
    { title_type: 'feature,tv_series', release_date: `${new Date().getFullYear() - 1}-01-01,`, sort: 'user_rating,desc', num_votes: '1000,' }, // 近期高分
];

// 定义最终要生成的榜单 (基于语义标签)
const FINAL_LISTS_CONFIG = {
    // 专属频道
    anime_jp_top: { name: '日本高分动画', tags: ['lang:ja', 'genre:animation'], sortBy: 'vote_average' },
    anime_jp_popular: { name: '日本热门动画', tags: ['lang:ja', 'genre:animation'], sortBy: 'vote_count' },
    tv_kr_top: { name: '韩国高分剧集', tags: ['lang:ko', 'type:tv'], sortBy: 'vote_average' },
    tv_kr_popular: { name: '韩国热门剧集', tags: ['lang:ko', 'type:tv'], sortBy: 'vote_count' },
    tv_us_top: { name: '美国高分剧集', tags: ['lang:en', 'country:us', 'type:tv'], sortBy: 'vote_average' },
    tv_cn_top: { name: '国产高分剧集', tags: ['lang:zh', 'country:cn', 'type:tv'], sortBy: 'vote_average' },
    // 主题探索
    theme_cyberpunk: { name: '赛博朋克精选', tags: ['theme:cyberpunk'], sortBy: 'vote_average' },
    theme_zombie: { name: '僵尸题材精选', tags: ['theme:zombie'], sortBy: 'vote_average' },
    theme_wuxia: { name: '武侠世界', tags: ['theme:wuxia'], sortBy: 'vote_average' },
    theme_adult: { name: '成人内容精选', tags: ['theme:adult'], sortBy: 'vote_average' },
    // 系列电影 (特殊处理)
    series_collection: { name: '系列电影宇宙', type: 'series' }
};

// --- 构建流程 ---

async function main() {
    console.log('Starting intelligent build process...');
    const startTime = Date.now();
    const allEnrichedItems = new Map(); // 使用Map去重，key为tmdb_id

    try {
        // --- 阶段一: 广撒网抓取 & 阶段二: 增强与智能打标 ---
        console.log('\nPHASE 1 & 2: Crawling, Enriching, and Analyzing...');
        for (const params of CRAWL_TASKS) {
            const imdbIdList = await fetchImdbIdList(params);
            for (let i = 0; i < imdbIdList.length; i += MAX_CONCURRENT_ENRICHMENTS) {
                const batch = imdbIdList.slice(i, i + MAX_CONCURRENT_ENRICHMENTS);
                const promises = batch.map(async (imdbId) => {
                    const tmdbInfo = await findByImdbId(imdbId);
                    if (!tmdbInfo || allEnrichedItems.has(tmdbInfo.id)) return;
                    
                    const details = await getTmdbDetails(tmdbInfo.id, tmdbInfo.media_type);
                    if (!details) return;

                    const taggedItem = analyzeAndTagItem(details);
                    allEnrichedItems.set(taggedItem.id, taggedItem);
                });
                await Promise.allSettled(promises);
                await sleep(500);
            }
        }
        console.log(`  Total unique items analyzed: ${allEnrichedItems.size}`);

        // --- 阶段三: 根据配置生成最终榜单 ---
        console.log('\nPHASE 3: Generating final lists from analyzed data...');
        const analyzedData = Array.from(allEnrichedItems.values());
        const seriesCollection = new Map();

        for (const [key, config] of Object.entries(FINAL_LISTS_CONFIG)) {
            let listData = [];
            if (config.type === 'series') {
                // 特殊处理系列电影
                analyzedData.forEach(item => {
                    const seriesTag = item.semantic_tags.find(t => t.startsWith('series:'));
                    if (seriesTag) {
                        const seriesId = seriesTag.split(':')[1];
                        if (!seriesCollection.has(seriesId)) {
                            seriesCollection.set(seriesId, {
                                name: item.belongs_to_collection.name,
                                items: []
                            });
                        }
                        seriesCollection.get(seriesId).items.push(item);
                    }
                });
                // 为每个系列生成一个文件
                for (const [seriesId, seriesData] of seriesCollection.entries()) {
                    seriesData.items.sort((a, b) => new Date(a.release_date) - new Date(b.release_date)); // 按上映日期排序
                    const seriesKey = `series_${seriesId}`;
                    await writeJsonFile(`${OUTPUT_DIR}/${seriesKey}.json`, seriesData.items);
                    console.log(`  Generated series list: ${seriesData.name} -> ${seriesKey}.json`);
                }
                continue; // 跳过常规榜单生成
            }

            listData = analyzedData.filter(item => 
                config.tags.every(tag => item.semantic_tags.includes(tag))
            );

            listData.sort((a, b) => (b[config.sortBy] || 0) - (a[config.sortBy] || 0));
            
            await writeJsonFile(`${OUTPUT_DIR}/${key}.json`, listData.slice(0, 50));
            console.log(`  Generated list: ${config.name} -> ${key}.json (${listData.length} items found)`);
        }

        // 创建索引文件
        const index = {
            lists: FINAL_LISTS_CONFIG,
            series: Array.from(seriesCollection.values()).map((s, i) => ({ id: `series_${Array.from(seriesCollection.keys())[i]}`, name: s.name })),
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
