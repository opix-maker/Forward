import { STATIC_CHARTS_CONFIG, CURATED_LISTS_CONFIG, fetchImdbIdList } from './src/builders/imdb_builder.js';
import { findByImdbId, getTmdbDetails } from './src/utils/tmdb_api.js';
import { sleep, writeJsonFile } from './src/utils/helpers.js';

const MAX_CONCURRENT_ENRICHMENTS = 10;
const OUTPUT_DIR = './dist'; // 所有JSON文件将输出到这个目录

/**
 * 接收IMDb ID列表，并用TMDB的中文信息进行丰富
 * @param {Array<string>} imdbIdList - IMDb ID 列表
 * @returns {Array} - 包含完整TMDB中文信息的对象数组
 */
async function enrichListWithTmdb(imdbIdList) {
    const enrichedItems = [];
    for (let i = 0; i < imdbIdList.length; i += MAX_CONCURRENT_ENRICHMENTS) {
        const batch = imdbIdList.slice(i, i + MAX_CONCURRENT_ENRICHMENTS);
        const promises = batch.map(async (imdbId) => {
            if (!imdbId) return null;
            
            const tmdbSearchResult = await findByImdbId(imdbId);
            if (!tmdbSearchResult) return null;

            const mediaType = tmdbSearchResult.media_type || 'movie';
            const tmdbDetails = await getTmdbDetails(tmdbSearchResult.id, mediaType);
            if (!tmdbDetails) return null;

            // 提取中文标题和简介
            const chineseTranslation = tmdbDetails.translations?.translations?.find(t => t.iso_639_1 === 'zh');
            const title_zh = chineseTranslation?.data?.title || chineseTranslation?.data?.name || tmdbDetails.title || tmdbDetails.name;
            const overview_zh = chineseTranslation?.data?.overview || tmdbDetails.overview;

            return {
                imdb_id: imdbId,
                tmdb_id: tmdbDetails.id,
                title: title_zh,
                overview: overview_zh,
                poster_path: tmdbDetails.poster_path,
                backdrop_path: tmdbDetails.backdrop_path,
                release_date: tmdbDetails.release_date || tmdbDetails.first_air_date,
                genres: tmdbDetails.genres.map(g => g.name),
                vote_average: tmdbDetails.vote_average,
                vote_count: tmdbDetails.vote_count,
                media_type: mediaType,
            };
        });

        const settledResults = await Promise.allSettled(promises);
        settledResults.forEach(result => {
            if (result.status === 'fulfilled' && result.value) {
                enrichedItems.push(result.value);
            }
        });
        await sleep(500);
    }
    return enrichedItems;
}

async function main() {
    console.log('Starting main build process...');
    const startTime = Date.now();
    
    const allTasks = {
        ...STATIC_CHARTS_CONFIG,
        ...CURATED_LISTS_CONFIG
    };

    try {
        for (const [key, config] of Object.entries(allTasks)) {
            console.log(`\nProcessing task: ${config.name} (${key})`);
            
            // 1. 获取IMDb ID列表
            const imdbIdList = await fetchImdbIdList(config);
            if (!imdbIdList || imdbIdList.length === 0) {
                console.warn(`  No items found for ${config.name}. Skipping.`);
                continue;
            }
            console.log(`  Found ${imdbIdList.length} IMDb IDs.`);

            // 2. 丰富TMDB数据
            const enrichedData = await enrichListWithTmdb(imdbIdList);
            console.log(`  Enriched ${enrichedData.length} items with TMDB data.`);

            // 3. 写入独立的JSON文件
            const outputPath = `${OUTPUT_DIR}/${key}.json`;
            await writeJsonFile(outputPath, enrichedData);
            console.log(`  Successfully wrote data to ${outputPath}`);

            await sleep(1000); // 在任务之间礼貌性等待
        }

        // 4. 创建一个索引文件，方便客户端知道有哪些榜单
        const index = {
            staticCharts: Object.keys(STATIC_CHARTS_CONFIG).map(key => ({ id: key, name: STATIC_CHARTS_CONFIG[key].name })),
            curatedLists: Object.keys(CURATED_LISTS_CONFIG).map(key => ({ id: key, name: CURATED_LISTS_CONFIG[key].name })),
            buildTimestamp: new Date().toISOString()
        };
        await writeJsonFile(`${OUTPUT_DIR}/index.json`, index);
        console.log(`\nSuccessfully wrote index file to ${OUTPUT_DIR}/index.json`);

        const duration = (Date.now() - startTime) / 1000;
        console.log(`\n✅ Build process successful! Took ${duration.toFixed(2)} seconds.`);

    } catch (error) {
        console.error('\n❌ FATAL ERROR during build process:', error);
        process.exit(1);
    }
}

main();
