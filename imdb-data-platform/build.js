import fs from 'fs/promises';
import { buildImdbData } from './src/builders/imdb_builder.js';
import { findByImdbId, getTmdbDetails } from './src/utils/tmdb_api.js';
import { sleep } from './src/utils/helpers.js';

const MAX_CONCURRENT_ENRICHMENTS = 10;

/**
 * 将IMDb原始数据通过TMDB API进行增强
 * @param {object} rawImdbData
 * @returns {Promise<object>}
 */
async function enrichDataWithTmdb(rawImdbData) {
    console.log('Enriching raw IMDb data with TMDB details...');
    const enrichedData = {};

    for (const [chartKey, items] of Object.entries(rawImdbData)) {
        console.log(`- Enriching chart: ${chartKey} (${items.length} items)`);
        const enrichedItems = [];
        
        for (let i = 0; i < items.length; i += MAX_CONCURRENT_ENRICHMENTS) {
            const batch = items.slice(i, i + MAX_CONCURRENT_ENRICHMENTS);
            const promises = batch.map(async (item) => {
                const tmdbSearchResult = await findByImdbId(item.imdbId);
                if (!tmdbSearchResult) {
                    console.warn(`  - TMDB data not found for IMDb ID: ${item.imdbId} (${item.title})`);
                    return null; // 如果找不到，则跳过此项目
                }

                const mediaType = tmdbSearchResult.media_type || 'movie';
                const tmdbDetails = await getTmdbDetails(tmdbSearchResult.id, mediaType);
                if (!tmdbDetails) {
                    console.warn(`  - TMDB details fetch failed for TMDB ID: ${tmdbSearchResult.id}`);
                    return null;
                }
                
                // 组合最终数据
                return {
                    imdb_id: item.imdbId,
                    imdb_rating: item.rating,
                    imdb_rank: item.rank,
                    
                    tmdb_id: tmdbDetails.id,
                    title: tmdbDetails.title || tmdbDetails.name,
                    original_title: tmdbDetails.original_title || tmdbDetails.original_name,
                    overview: tmdbDetails.overview,
                    poster_path: tmdbDetails.poster_path ? `https://image.tmdb.org/t/p/w500${tmdbDetails.poster_path}` : null,
                    backdrop_path: tmdbDetails.backdrop_path ? `https://image.tmdb.org/t/p/w780${tmdbDetails.backdrop_path}` : null,
                    release_date: tmdbDetails.release_date || tmdbDetails.first_air_date,
                    genres: tmdbDetails.genres.map(g => g.name),
                    vote_average: tmdbDetails.vote_average,
                    vote_count: tmdbDetails.vote_count,
                    media_type: mediaType,
                    origin_country: tmdbDetails.origin_country || [],
                };
            });

            const settledResults = await Promise.allSettled(promises);
            settledResults.forEach(result => {
                if (result.status === 'fulfilled' && result.value) {
                    enrichedItems.push(result.value);
                }
            });
            
            console.log(`  ...processed batch ${Math.floor(i / MAX_CONCURRENT_ENRICHMENTS) + 1}`);
            await sleep(500); // 在批次之间短暂等待
        }
        enrichedData[chartKey] = enrichedItems;
    }
    
    console.log('Enrichment process finished.');
    return enrichedData;
}


/**
 * 主函数
 */
async function main() {
    console.log('Starting main build process...');
    const startTime = Date.now();

    try {
        // 1. 从IMDb抓取原始数据
        const rawImdbData = await buildImdbData();

        // 2. 通过TMDB增强数据
        const finalData = await enrichDataWithTmdb(rawImdbData);

        // 3. 准备最终输出文件
        const output = {
            buildTimestamp: new Date().toISOString(),
            imdb: finalData
        };

        // 4. 写入文件
        await fs.writeFile('imdb_precomputed_data.json', JSON.stringify(output, null, 2));

        const duration = (Date.now() - startTime) / 1000;
        console.log(`\n✅ Build process successful! Took ${duration.toFixed(2)} seconds.`);
        console.log('`imdb_precomputed_data.json` has been generated.');

    } catch (error) {
        console.error('\n❌ FATAL ERROR during build process:', error);
        process.exit(1);
    }
}

main();
